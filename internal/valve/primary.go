package valve

import (
	"fmt"
	"sync"
	"time"
)

type PrimaryAdmission struct {
	StationID   string    `json:"station_id"`
	ExchangerID string    `json:"exchanger_id"`
	OperationID string    `json:"operation_id"`
	Open        bool      `json:"open"`
	At          time.Time `json:"at"`
}

type PrimaryService struct {
	mu         sync.RWMutex
	admissions map[string]PrimaryAdmission
}

func NewPrimaryService() *PrimaryService {
	return &PrimaryService{admissions: make(map[string]PrimaryAdmission)}
}

func primaryKey(stationID, exchangerID string) string {
	return stationID + "/" + exchangerID
}

func (s *PrimaryService) Open(stationID, exchangerID, operationID string, flowConfirmed bool) (PrimaryAdmission, error) {
	if stationID == "" || exchangerID == "" || operationID == "" {
		return PrimaryAdmission{}, fmt.Errorf("primary admission identity is incomplete")
	}
	if !flowConfirmed {
		return PrimaryAdmission{}, fmt.Errorf("secondary flow is not confirmed")
	}
	admission := PrimaryAdmission{
		StationID: stationID, ExchangerID: exchangerID, OperationID: operationID,
		Open: true, At: time.Now().UTC(),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admissions[primaryKey(stationID, exchangerID)] = admission
	return admission, nil
}

func (s *PrimaryService) Close(stationID, exchangerID, operationID string) (PrimaryAdmission, error) {
	if stationID == "" || exchangerID == "" || operationID == "" {
		return PrimaryAdmission{}, fmt.Errorf("primary admission identity is incomplete")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	admission := PrimaryAdmission{
		StationID: stationID, ExchangerID: exchangerID, OperationID: operationID,
		Open: false, At: time.Now().UTC(),
	}
	s.admissions[primaryKey(stationID, exchangerID)] = admission
	return admission, nil
}

func (s *PrimaryService) Admission(stationID, exchangerID string) (PrimaryAdmission, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	admission, ok := s.admissions[primaryKey(stationID, exchangerID)]
	return admission, ok
}
