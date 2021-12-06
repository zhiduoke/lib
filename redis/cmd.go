package redis

import (
	"github.com/gomodule/redigo/redis"
)

func muiltiDo(conn redis.Conn, f func(c redis.Conn) error) (interface{}, error) {
	_, err := conn.Do("MULTI")
	if err != nil {
		return nil, err
	}
	err = f(conn)
	if err != nil {
		conn.Do("DISCARD")
		return nil, err
	}
	return conn.Do("EXEC")
}

func (db *DB) Tx(f func(c redis.Conn) error) (interface{}, error) {
	conn := db.pool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}
	defer conn.Close()
	return muiltiDo(conn, f)
}

func (db *DB) WatchedTx(f func(c redis.Conn) error, watch ...interface{}) (interface{}, error) {
	conn := db.pool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}
	defer conn.Close()
	_, err := conn.Do("WATCH", watch...)
	if err != nil {
		return nil, err
	}
	return muiltiDo(conn, f)
}

func (db *DB) HGet(key, filed string) (string, error) {
	return redis.String(db.Do("HGET", key, filed))
}

func (db *DB) Get(key string) (string, error) {
	return redis.String(db.Do("GET", key))
}

func (db *DB) Exist(key string) (bool, error) {
	return redis.Bool(db.Do("EXISTS", key))
}

func (db *DB) Set(key string, value interface{}) error {
	_, err := db.Do("SET", key, value)
	return err
}

func (db *DB) Update(key string, value interface{}) (bool, error) {
	reply, err := db.Do("SET", key, value, "XX")
	if err != nil {
		return false, err
	}
	return reply != nil, nil
}

func (db *DB) SetEX(key string, value interface{}, expire int) (bool, error) {
	reply, err := db.Do("SET", key, value, "EX", expire)
	if err != nil {
		return false, err
	}
	return reply != nil, nil
}

func (db *DB) SetArgs(key string, value string, with string, arg int) (bool, error) {
	reply, err := db.Do("SET", key, value, with, arg)
	if err != nil {
		return false, err
	}
	return reply != nil, nil
}

func (db *DB) GetBytes(key string) ([]byte, error) {
	return redis.Bytes(db.Do("GET", key))
}

func (db *DB) Delete(key string) error {
	_, err := db.Do("DEL", key)
	return err
}

func (db *DB) Keys(key string) ([]string, error) {
	result, err := redis.Strings(db.Do("KEYS", key))
	return result, err
}

func (db *DB) Incr(key string) (int, error) {
	return redis.Int(db.Do("INCR", key))
}

// IncrAndExpire 使用事务执行 INCR 和 EXPIRE
func (db *DB) IncrAndExpire(key string, expire int64) error {
	_, err := db.Tx(func(conn redis.Conn) error {
		_, err := conn.Do("INCR", key)
		if err == nil {
			_, err = conn.Do("EXPIRE", key, expire)
		}
		return err
	})
	return err
}

func (db *DB) IncrEx(key string, expire int64) (int, error) {
	var res int
	err := db.Borrow(func(conn redis.Conn) error {
		var err error
		res, err = redis.Int(conn.Do("INCR", key))
		if err == nil {
			_, err = conn.Do("EXPIRE", key, expire)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return res, nil
}

func (db *DB) Decr(key string) (int, error) {
	return redis.Int(db.Do("DECR", key))
}

func (db *DB) DecrEx(key string, expire int64) (int, error) {
	var res int
	err := db.Borrow(func(conn redis.Conn) error {
		var err error
		res, err = redis.Int(conn.Do("DECR", key))
		if err == nil {
			_, err = conn.Do("EXPIRE", key, expire)
		}
		return err
	})
	if err != nil {
		return 0, err
	}
	return res, nil
}

func (db *DB) SetNX(key string, value string) (bool, error) {
	return redis.Bool(db.Do("SETNX", key, value))
}

func (db *DB) TTL(key string) (int, error) {
	return redis.Int(db.Do("TTL", key))
}

func (db *DB) HSetNX(key string, field string, value string) (bool, error) {
	return redis.Bool(db.Do("HSETNX", key, field, value))
}

func (db *DB) Expire(key string, time int) error {
	_, err := db.Do("EXPIRE", key, time)
	return err
}

func (db *DB) HIncrBy(key string, field string, value int) (int, error) {
	return redis.Int(db.Do("HINCRBY", key, field, value))
}

func (db *DB) HGetAll(key string) (map[string]string, error) {
	return redis.StringMap(redis.Values(db.Do("HGETALL", key)))
}

func (db *DB) Lock(key string, expire int) (bool, error) {
	added, err := db.Do("SET", key, 1, "NX", "EX", expire)
	if err != nil {
		return false, err
	}
	return added == nil, nil
}

func (db *DB) GetInt64(key string) (int64, error) {
	return redis.Int64(db.Do("GET", key))
}
