package liteconsul

import "testing"

func TestClient_TxnCommit(t *testing.T) {
	client := NewClient("http://localhost:8500", "")
	entry, _, err := client.KVGet("test/hello", nil)
	if err != nil && !IsNotFound(err) {
		t.Fatal(err)
	}
	t.Log(entry)
	lastIndex := uint64(0)
	if entry != nil {
		lastIndex = entry.ModifyIndex
	}
	ops := NewTxnOperations()
	ops.AddKVOp(&TxnKVOp{
		Verb:  "set",
		Key:   "test/world",
		Value: []byte("b"),
	})
	ops.AddKVOp(&TxnKVOp{
		Verb:  "cas",
		Key:   "test/hello",
		Value: []byte("a"),
		Index: lastIndex + 1,
	})
	committed, txnResp, err := client.TxnCommit(ops)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("committed", committed)
	t.Log("resp", txnResp.Errors)
}
