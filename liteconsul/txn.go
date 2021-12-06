package liteconsul

import (
	"bytes"
	"encoding/json"
)

type TxnResponse struct {
	Results []map[string]json.RawMessage `json:"Results"`
	Errors  []struct {
		OpIndex int    `json:"OpIndex"`
		What    string `json:"What"`
	} `json:"Errors"`
}

type TxnKVOp struct {
	// see: https://www.consul.io/api-docs/txn#kv-operations
	Verb    string `json:"Verb"`
	Key     string `json:"Key"`
	Value   []byte `json:"Value,omitempty"`
	Flags   uint64 `json:"Flags,omitempty"`
	Index   uint64 `json:"Index,omitempty"`
	Session string `json:"Session,omitempty"`
}

type Operations struct {
	b    bytes.Buffer
	more bool
}

func (ops *Operations) addOp(op string, entity interface{}) {
	if ops.more {
		ops.b.WriteByte(',')
	} else {
		ops.more = true
	}
	ops.b.WriteString(`{"`)
	ops.b.WriteString(op)
	ops.b.WriteString(`":`)
	data, _ := json.Marshal(entity)
	ops.b.Write(data)
	ops.b.WriteByte('}')
}

func (ops *Operations) AddKVOp(kv *TxnKVOp) {
	ops.addOp("KV", kv)
}

func (ops *Operations) done() []byte {
	ops.b.WriteByte(']')
	return ops.b.Bytes()
}

func NewTxnOperations() *Operations {
	ops := Operations{}
	ops.b.WriteByte('[')
	return &ops
}

func (c *Client) TxnCommit(ops *Operations) (bool, *TxnResponse, error) {
	resp, err := c.send(&consulRequest{
		method: methodPUT,
		path:   "/v1/txn",
		body:   ops.done(),
	})
	if err != nil {
		return false, nil, err
	}
	if resp.StatusCode == 200 || resp.StatusCode == 409 {
		body := readBody(resp)
		var txnResp TxnResponse
		err = json.Unmarshal(body, &txnResp)
		if err != nil {
			return false, nil, err
		}
		return resp.StatusCode == 200, &txnResp, nil
	}
	return false, nil, errorFrom(resp)
}
