package redis

import (
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	maxReadFails   = 3
	maxBorrowFails = 3
)

type Config struct {
	Address        string
	Auth           string
	MaxIdleConns   int
	MaxOpenConns   int
	IdleTimeout    time.Duration
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type DB struct {
	pool           redis.Pool
	scheme         string
	addr           string
	auth           string
	connectTimeout time.Duration
	readTimeout    time.Duration
	writeTimeout   time.Duration
}

func (db *DB) Do(cmd string, args ...interface{}) (interface{}, error) {
	conn := db.pool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}
	reply, err := conn.Do(cmd, args...)
	conn.Close()
	return reply, err
}

func (db *DB) Read(cmd string, args ...interface{}) (interface{}, error) {
	fails := 0
	for {
		reply, err := db.Do(cmd, args...)
		if err != nil {
			fails++
			if fails >= maxReadFails {
				return nil, err
			}
			continue
		}
		return reply, nil
	}
}

func (db *DB) Borrow(f func(conn redis.Conn) error) error {
	fails := 0
	for {
		rc := db.pool.Get()
		if err := rc.Err(); err != nil {
			fails++
			if fails >= maxBorrowFails {
				return err
			}
			continue
		}
		// TODO test rc
		err := f(rc)
		rc.Close()
		return err
	}
}

func (db *DB) ScanStruct(dst interface{}, cmd string, args ...interface{}) error {
	v, err := redis.Values(db.Do(cmd, args...))
	if err != nil {
		return err
	}
	return redis.ScanStruct(v, dst)
}

func (db *DB) Int(cmd string, args ...interface{}) (int, error) {
	return redis.Int(db.Do(cmd, args...))
}

func (db *DB) String(cmd string, args ...interface{}) (string, error) {
	return redis.String(db.Do(cmd, args...))
}

func (db *DB) Strings(cmd string, args ...interface{}) ([]string, error) {
	return redis.Strings(db.Do(cmd, args...))
}

func (db *DB) dial() (redis.Conn, error) {
	c, err := redis.Dial(
		db.scheme,
		db.addr,
		redis.DialConnectTimeout(db.connectTimeout),
		redis.DialReadTimeout(db.readTimeout),
		redis.DialWriteTimeout(db.writeTimeout),
	)
	if err != nil {
		return nil, err
	}
	if db.auth != "" {
		_, err = c.Do("AUTH", db.auth)
		if err != nil {
			c.Close()
			return nil, err
		}
	}
	return c, nil
}

func Open(config *Config) *DB {
	scheme := "tcp"
	addr := config.Address
	s := strings.SplitN(addr, "://", 2)
	if len(s) == 2 {
		scheme = s[0]
		addr = s[1]
	}
	db := &DB{
		scheme: scheme,
		addr:   addr,
		auth:   config.Auth,
		pool: redis.Pool{
			MaxIdle:     config.MaxIdleConns,
			MaxActive:   config.MaxOpenConns,
			IdleTimeout: config.IdleTimeout,
			Wait:        true,
		},
	}
	// NOTE 暂时不配置 TestOnBorrow，观察线上空闲连接环境 reset 时长
	db.pool.Dial = db.dial
	return db
}
