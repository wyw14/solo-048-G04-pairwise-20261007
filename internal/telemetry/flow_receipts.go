package telemetry

import (
	"fmt"
	"sync"
	"time"
)

type FlowReceipt struct {
	StationID    string    `json:"station_id"`
	ExchangerID  string    `json:"exchanger_id"`
	OperationID  string    `json:"operation_id"`
	LitersSecond float64   `json:"liters_second"`
	Stable       bool      `json:"stable"`
	At           time.Time `json:"at"`
}

func (r FlowReceipt) Matches(stationID, exchangerID, operationID string) bool {
	return r.StationID == stationID && r.ExchangerID == exchangerID && r.OperationID == operationID && r.Stable && r.LitersSecond > 0
}

// MatchesDrain reports a stable zero-flow receipt proving the exchanger has
// been fully drained for the given operation.
func (r FlowReceipt) MatchesDrain(stationID, exchangerID, operationID string) bool {
	return r.StationID == stationID && r.ExchangerID == exchangerID && r.OperationID == operationID && r.Stable && r.LitersSecond == 0
}

type FlowReceipts struct {
	mu       sync.RWMutex
	receipts []FlowReceipt
}

func NewFlowReceipts() *FlowReceipts {
	return &FlowReceipts{receipts: make([]FlowReceipt, 0)}
}

func (r *FlowReceipts) Record(receipt FlowReceipt) error {
	if receipt.StationID == "" || receipt.ExchangerID == "" || receipt.OperationID == "" {
		return fmt.Errorf("flow receipt identity is incomplete")
	}
	if receipt.At.IsZero() {
		receipt.At = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receipts = append(r.receipts, receipt)
	if len(r.receipts) > 256 {
		r.receipts = append([]FlowReceipt(nil), r.receipts[len(r.receipts)-256:]...)
	}
	return nil
}

func (r *FlowReceipts) Confirmed(stationID, exchangerID, operationID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for index := len(r.receipts) - 1; index >= 0; index-- {
		if r.receipts[index].Matches(stationID, exchangerID, operationID) {
			return true
		}
	}
	return false
}

// DrainConfirmed reports the latest drain evidence for an exchanger: the most
// recent receipt must be a stable zero-flow one. A later positive-flow receipt
// invalidates an earlier drain confirmation.
func (r *FlowReceipts) DrainConfirmed(stationID, exchangerID, operationID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for index := len(r.receipts) - 1; index >= 0; index-- {
		receipt := r.receipts[index]
		if receipt.StationID != stationID || receipt.ExchangerID != exchangerID || receipt.OperationID != operationID {
			continue
		}
		return receipt.MatchesDrain(stationID, exchangerID, operationID)
	}
	return false
}

func (r *FlowReceipts) ForStation(stationID string) []FlowReceipt {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]FlowReceipt, 0)
	for _, receipt := range r.receipts {
		if receipt.StationID == stationID {
			result = append(result, receipt)
		}
	}
	return result
}
