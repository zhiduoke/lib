package liteconsul

import (
	"strconv"
	"strings"
)

type KVEntry struct {
	CreateIndex uint64
	ModifyIndex uint64
	LockIndex   uint64
	Key         string
	Flags       uint64
	Value       []byte
	Session     string
}

func (c *Client) KVGet(key string, options *QueryOptions) (*KVEntry, *QueryMetadata, error) {
	var ents []KVEntry
	meta, err := c.query(&consulRequest{
		method: methodGET,
		path:   "/v1/kv/" + strings.TrimPrefix(key, "/"),
	}, options, &ents)
	if len(ents) > 0 {
		return &ents[0], meta, err
	}
	return nil, meta, err
}

func (c *Client) KVList(prefix string, options *QueryOptions) ([]KVEntry, *QueryMetadata, error) {
	var ents []KVEntry
	meta, err := c.query(&consulRequest{
		method: methodGET,
		path:   "/v1/kv/" + strings.TrimPrefix(prefix, "/"),
		params: []string{"recurse", "true"},
	}, options, &ents)
	return ents, meta, err
}

func (c *Client) KVDelete(key string, params ...string) (bool, error) {
	raw, err := c.invoke(&consulRequest{
		method: methodDELETE,
		path:   "/v1/kv/" + strings.TrimPrefix(key, "/"),
		params: params,
	})
	if err != nil {
		return false, err
	}
	// string(bytes) 不需要内存分配
	return string(raw) == "true", nil
}

func (c *Client) kvPutImpl(key string, value []byte, params ...string) (bool, error) {
	raw, err := c.invoke(&consulRequest{
		method: methodPUT,
		path:   "/v1/kv/" + strings.TrimPrefix(key, "/"),
		params: params,
		body:   value,
	})
	if err != nil {
		return false, err
	}
	// string(bytes) 不需要内存分配
	return string(raw) == "true", nil
}

func (c *Client) KVPut(key string, value []byte) (bool, error) {
	return c.kvPutImpl(key, value)
}

func (c *Client) KVCAS(key string, value []byte, cas uint64) (bool, error) {
	return c.kvPutImpl(key, value, "cas", strconv.FormatUint(cas, 10))
}
