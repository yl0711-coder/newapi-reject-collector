package main

import (
	"encoding/json"
	"errors"
)

// 202 transfers durability to the task-external archive, NOT to Monitor's
// fact database. Standard HTTPS/Lightsail collectors never accept this receipt.
func validateArchiveReceipt(cfg Config, node, batchID, hash string, body []byte) error {
	var ack struct {
		Version int    `json:"version"`
		State   string `json:"state"`
		Node    string `json:"node"`
		Lane    string `json:"lane"`
		BatchID string `json:"batch_id"`
		Hash    string `json:"body_sha256"`
	}
	if cfg.ECSSocket == "" || len(body) > 4096 || json.Unmarshal(body, &ack) != nil || ack.Version != 1 || ack.State != "durably_archived" || ack.Node != node || ack.Lane != "reject" || ack.BatchID != batchID || ack.Hash != hash {
		return errors.New("external archive receipt mismatch; retain frozen reject batch")
	}
	return nil
}
