package switchover

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/exchanger"
	"heatpulse/internal/protection"
	"heatpulse/internal/station"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

// Service orchestrates primary/standby exchanger switchovers as a confirmed
// stage sequence: preheat -> flow -> switchover -> drain. Every transition is
// persisted so operations remain queryable after a restart.
type Service struct {
	stations   *station.Registry
	latches    *protection.LatchStore
	warmup     *exchanger.WarmupService
	receipts   *telemetry.FlowReceipts
	primary    *valve.PrimaryService
	audit      *audit.Recorder
	store      *operationStore
	mu         sync.Mutex
	operations map[string]Operation
}

func NewService(directory string, stations *station.Registry, latches *protection.LatchStore, warmup *exchanger.WarmupService, receipts *telemetry.FlowReceipts, primary *valve.PrimaryService, recorder *audit.Recorder) (*Service, error) {
	store := newOperationStore(directory)
	operations, err := store.load()
	if err != nil {
		return nil, fmt.Errorf("load switchover operations: %w", err)
	}
	service := &Service{
		stations: stations, latches: latches, warmup: warmup, receipts: receipts,
		primary: primary, audit: recorder, store: store, operations: operations,
	}
	service.reconcile()
	return service, nil
}

// reconcile re-applies the exchanger phases implied by each station's latest
// persisted operation, since the station registry restarts from seed values.
func (s *Service) reconcile() {
	latest := make(map[string]Operation)
	for _, operation := range s.operations {
		current, ok := latest[operation.StationID]
		if !ok || operation.StartedAt.After(current.StartedAt) {
			latest[operation.StationID] = operation
		}
	}
	for stationID, operation := range latest {
		if _, ok := s.stations.Get(stationID); !ok {
			continue
		}
		switch {
		case operation.Status == StatusCompleted:
			s.setPhase(stationID, operation.TargetExchanger, station.ExchangerCarrying)
			s.setPhase(stationID, operation.SourceExchanger, station.ExchangerStandby)
		case operation.Status == StatusRolledBack:
			s.setPhase(stationID, operation.SourceExchanger, station.ExchangerCarrying)
			s.setPhase(stationID, operation.TargetExchanger, station.ExchangerStandby)
		case operation.stageConfirmed(StageSwitchover):
			s.setPhase(stationID, operation.TargetExchanger, station.ExchangerCarrying)
			s.setPhase(stationID, operation.SourceExchanger, station.ExchangerDraining)
		default:
			s.setPhase(stationID, operation.SourceExchanger, station.ExchangerCarrying)
			s.setPhase(stationID, operation.TargetExchanger, station.ExchangerPriming)
		}
	}
}

func (s *Service) setPhase(stationID, exchangerID string, phase station.ExchangerPhase) {
	_, _ = s.stations.SetExchanger(stationID, exchangerID, phase)
}

// guard refuses to advance while the station is under maintenance or any
// protection latch is active.
func (s *Service) guard(stationID string) (station.Station, error) {
	item, ok := s.stations.Get(stationID)
	if !ok {
		return station.Station{}, fmt.Errorf("station %s not found", stationID)
	}
	if item.Maintenance {
		return item, fmt.Errorf("station %s is under maintenance", stationID)
	}
	latches := s.latches.Get(stationID)
	if latches.HighReturn || latches.FreezeRisk || latches.PressureTrip {
		return item, fmt.Errorf("station %s has active protection latches (high_return=%t freeze_risk=%t pressure_trip=%t)",
			stationID, latches.HighReturn, latches.FreezeRisk, latches.PressureTrip)
	}
	return item, nil
}

func (s *Service) Begin(stationID, sourceID, targetID, actor string) (Operation, error) {
	if sourceID == "" || targetID == "" || sourceID == targetID {
		return Operation{}, fmt.Errorf("source and target exchangers must differ and be non-empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item, err := s.guard(stationID)
	if err != nil {
		return Operation{}, err
	}
	sourcePhase, ok := item.Exchangers[sourceID]
	if !ok {
		return Operation{}, fmt.Errorf("exchanger %s not found on station %s", sourceID, stationID)
	}
	targetPhase, ok := item.Exchangers[targetID]
	if !ok {
		return Operation{}, fmt.Errorf("exchanger %s not found on station %s", targetID, stationID)
	}
	if sourcePhase != station.ExchangerCarrying {
		return Operation{}, fmt.Errorf("source exchanger %s is %s, not carrying", sourceID, sourcePhase)
	}
	if targetPhase != station.ExchangerStandby {
		return Operation{}, fmt.Errorf("target exchanger %s is %s, not standby", targetID, targetPhase)
	}
	for _, operation := range s.operations {
		if operation.StationID == stationID && operation.Status == StatusInProgress {
			return Operation{}, fmt.Errorf("station %s already has switchover %s in progress", stationID, operation.ID)
		}
	}
	if _, err := s.stations.SetExchanger(stationID, targetID, station.ExchangerPriming); err != nil {
		return Operation{}, err
	}
	operation := newOperation(uuid.NewString(), stationID, sourceID, targetID, time.Now().UTC())
	s.operations[operation.ID] = operation
	if err := s.store.save(s.operations); err != nil {
		delete(s.operations, operation.ID)
		return Operation{}, err
	}
	if actor == "" {
		actor = "operator"
	}
	_ = s.audit.Accepted("switchover.begin", operation.ID, actor, "switchover orchestration started",
		map[string]any{"station_id": stationID, "source": sourceID, "target": targetID})
	return operation, nil
}

// Confirm validates the evidence for the operation's current stage and
// advances the sequence. A rejected confirmation leaves the operation in
// place so it can be retried once conditions clear.
func (s *Service) Confirm(operationID string, stage Stage, actor string) (Operation, error) {
	if !validStage(stage) {
		return Operation{}, fmt.Errorf("unknown stage %s", stage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	operation, ok := s.operations[operationID]
	if !ok {
		return Operation{}, fmt.Errorf("switchover operation %s not found", operationID)
	}
	if operation.Status != StatusInProgress {
		return operation, fmt.Errorf("switchover operation %s is %s", operationID, operation.Status)
	}
	if actor == "" {
		actor = "operator"
	}
	if _, err := s.guard(operation.StationID); err != nil {
		_ = s.audit.Rejected("switchover.stage", operationID, actor, err.Error(), map[string]any{"stage": stage})
		return operation, err
	}
	detail, err := s.verifyStage(operation, stage)
	if err != nil {
		_ = s.audit.Rejected("switchover.stage", operationID, actor, err.Error(), map[string]any{"stage": stage})
		return operation, err
	}
	if err := s.applyStage(operation, stage); err != nil {
		return operation, err
	}
	if err := operation.confirm(stage, actor, detail, time.Now().UTC()); err != nil {
		return operation, err
	}
	s.operations[operationID] = operation
	if err := s.store.save(s.operations); err != nil {
		return operation, err
	}
	details := map[string]any{"station_id": operation.StationID, "stage": stage, "detail": detail}
	if operation.Status == StatusCompleted {
		_, _ = s.audit.Record(audit.Entry{Action: "switchover.complete", EntityID: operationID, Actor: actor,
			Outcome: audit.OutcomeCompleted, Message: "switchover orchestration completed", Details: details})
	} else {
		_ = s.audit.Accepted("switchover.stage", operationID, actor, "stage confirmed", details)
	}
	return operation, nil
}

// verifyStage checks the evidence required to confirm the given stage.
func (s *Service) verifyStage(operation Operation, stage Stage) (string, error) {
	if operation.Stage != stage {
		return "", fmt.Errorf("operation is awaiting stage %s, not %s", operation.Stage, stage)
	}
	switch stage {
	case StagePreheat:
		proof, ok := s.warmup.Latest(operation.StationID)
		if !ok || proof.ExchangerID != operation.TargetExchanger || !proof.Ready || proof.At.Before(operation.StartedAt) {
			return "", fmt.Errorf("target exchanger %s preheat is not confirmed", operation.TargetExchanger)
		}
		return fmt.Sprintf("outlet %.1f celsius", proof.Outlet), nil
	case StageFlow:
		if !s.receipts.Confirmed(operation.StationID, operation.TargetExchanger, operation.ID) {
			return "", fmt.Errorf("no stable flow receipt for target exchanger %s", operation.TargetExchanger)
		}
		receipt, _ := s.receipts.Latest(operation.StationID, operation.TargetExchanger, operation.ID)
		return fmt.Sprintf("%.2f liters/second stable", receipt.LitersSecond), nil
	case StageSwitchover:
		if !s.receipts.Confirmed(operation.StationID, operation.TargetExchanger, operation.ID) {
			return "", fmt.Errorf("cannot complete switchover without a stable flow receipt for target exchanger %s", operation.TargetExchanger)
		}
		return "flow receipt verified", nil
	case StageDrain:
		receipt, ok := s.receipts.Latest(operation.StationID, operation.SourceExchanger, operation.ID)
		if !ok {
			return "", fmt.Errorf("no drain receipt for source exchanger %s", operation.SourceExchanger)
		}
		return fmt.Sprintf("drain receipt %.2f liters/second", receipt.LitersSecond), nil
	}
	return "", fmt.Errorf("unknown stage %s", stage)
}

// applyStage performs the side effects of confirming a stage.
func (s *Service) applyStage(operation Operation, stage Stage) error {
	switch stage {
	case StageSwitchover:
		if _, err := s.primary.Open(operation.StationID, operation.TargetExchanger, operation.ID, true); err != nil {
			return err
		}
		return s.setPhases(operation.StationID, map[string]station.ExchangerPhase{
			operation.TargetExchanger: station.ExchangerCarrying,
			operation.SourceExchanger: station.ExchangerDraining,
		})
	case StageDrain:
		return s.setPhases(operation.StationID, map[string]station.ExchangerPhase{
			operation.SourceExchanger: station.ExchangerStandby,
		})
	}
	return nil
}

func (s *Service) setPhases(stationID string, phases map[string]station.ExchangerPhase) error {
	for exchangerID, phase := range phases {
		if _, err := s.stations.SetExchanger(stationID, exchangerID, phase); err != nil {
			return err
		}
	}
	return nil
}

// Rollback aborts an in-progress operation and compensates according to how
// far the sequence advanced: before the switchover stage only the target
// returns to standby; afterwards the primary admission is closed and the
// source exchanger resumes carrying.
func (s *Service) Rollback(operationID, actor, reason string) (Operation, error) {
	if reason == "" {
		return Operation{}, fmt.Errorf("rollback reason is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	operation, ok := s.operations[operationID]
	if !ok {
		return Operation{}, fmt.Errorf("switchover operation %s not found", operationID)
	}
	if operation.Status != StatusInProgress {
		return operation, fmt.Errorf("switchover operation %s is %s", operationID, operation.Status)
	}
	if actor == "" {
		actor = "operator"
	}
	compensated := operation.stageConfirmed(StageSwitchover)
	steps := []string{"target exchanger back to standby"}
	if compensated {
		if _, err := s.primary.Close(operation.StationID, operation.TargetExchanger, operation.ID); err == nil {
			steps = append(steps, "primary admission closed")
		}
		if err := s.setPhases(operation.StationID, map[string]station.ExchangerPhase{
			operation.SourceExchanger: station.ExchangerCarrying,
		}); err != nil {
			return operation, err
		}
		steps = append(steps, "source exchanger resumed carrying")
	}
	if err := s.setPhases(operation.StationID, map[string]station.ExchangerPhase{
		operation.TargetExchanger: station.ExchangerStandby,
	}); err != nil {
		return operation, err
	}
	now := time.Now().UTC()
	operation.Status = StatusRolledBack
	operation.Stage = ""
	operation.Error = reason
	operation.CompletedAt = now
	operation.UpdatedAt = now
	s.operations[operationID] = operation
	if err := s.store.save(s.operations); err != nil {
		return operation, err
	}
	_, _ = s.audit.Record(audit.Entry{Action: "switchover.rollback", EntityID: operationID, Actor: actor,
		Outcome: audit.OutcomeFailed, Message: reason, Details: map[string]any{
			"station_id": operation.StationID, "switchover_confirmed": compensated, "compensation": steps,
		}})
	return operation, nil
}

func (s *Service) Get(operationID string) (Operation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	operation, ok := s.operations[operationID]
	operation.Stages = append([]StageRecord(nil), operation.Stages...)
	return operation, ok
}

func (s *Service) List(stationID string) []Operation {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Operation, 0, len(s.operations))
	for _, operation := range s.operations {
		if stationID == "" || operation.StationID == stationID {
			operation.Stages = append([]StageRecord(nil), operation.Stages...)
			result = append(result, operation)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].StartedAt.Equal(result[j].StartedAt) {
			return result[i].ID < result[j].ID
		}
		return result[i].StartedAt.After(result[j].StartedAt)
	})
	return result
}
