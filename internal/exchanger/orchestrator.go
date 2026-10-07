package exchanger

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

type SwitchoverStage string

const (
	StageWarmup        SwitchoverStage = "warmup"
	StageStabilizeFlow SwitchoverStage = "stabilize_flow"
	StagePrimarySwitch SwitchoverStage = "primary_switch"
	StageDrainOld      SwitchoverStage = "drain_old"
)

// StageOrder is the fixed switchover choreography: preheat the standby unit,
// stabilise its secondary flow, cut the primary circuit over, then drain the
// old unit.
var StageOrder = []SwitchoverStage{StageWarmup, StageStabilizeFlow, StagePrimarySwitch, StageDrainOld}

type SwitchoverStatus string

const (
	StatusActive     SwitchoverStatus = "active"
	StatusCompleted  SwitchoverStatus = "completed"
	StatusFailed     SwitchoverStatus = "failed"
	StatusRolledBack SwitchoverStatus = "rolled_back"
)

type StageState struct {
	Stage       SwitchoverStage `json:"stage"`
	Confirmed   bool            `json:"confirmed"`
	EnteredAt   time.Time       `json:"entered_at"`
	ConfirmedAt time.Time       `json:"confirmed_at,omitempty"`
	Note        string          `json:"note,omitempty"`
}

type Switchover struct {
	ID              string                         `json:"id"`
	StationID       string                         `json:"station_id"`
	SourceExchanger string                         `json:"source_exchanger"`
	TargetExchanger string                         `json:"target_exchanger"`
	Status          SwitchoverStatus               `json:"status"`
	CurrentStage    SwitchoverStage                `json:"current_stage,omitempty"`
	Stages          map[SwitchoverStage]StageState `json:"stages"`
	FailureReason   string                         `json:"failure_reason,omitempty"`
	RolledBack      bool                           `json:"rolled_back"`
	Actor           string                         `json:"actor,omitempty"`
	StartedAt       time.Time                      `json:"started_at"`
	UpdatedAt       time.Time                      `json:"updated_at"`
	CompletedAt     time.Time                      `json:"completed_at,omitempty"`
}

func (s Switchover) Clone() Switchover {
	copy := s
	if s.Stages != nil {
		copy.Stages = make(map[SwitchoverStage]StageState, len(s.Stages))
		for stage, state := range s.Stages {
			copy.Stages[stage] = state
		}
	}
	return copy
}

// ProtectionGuard reports whether a station is currently held in a protection
// state that forbids a primary cutover (latched trips, active incidents).
type ProtectionGuard interface {
	Blocked(stationID string) (bool, string)
}

// executionError marks a stage failure that has already mutated plant state
// (a valve or exchanger operation failed mid-way) and therefore requires the
// compensating rollback. Plain validation errors — missing proofs or receipts
// — stay retryable and leave the operation parked at its stage.
type executionError struct{ cause error }

func (e *executionError) Error() string { return e.cause.Error() }
func (e *executionError) Unwrap() error { return e.cause }

func abort(cause error) error { return &executionError{cause: cause} }

// Orchestrator runs the staged primary/standby exchanger switchover. Every
// stage transition is gated by an explicit confirmation and persisted before
// it becomes visible, so operations remain queryable after a restart.
type Orchestrator struct {
	stations *station.Registry
	receipts *telemetry.FlowReceipts
	primary  *valve.PrimaryService
	warmup   *WarmupService
	guard    ProtectionGuard
	store    *SwitchoverStore
	audit    *audit.Recorder
	mu       sync.Mutex
	active   map[string]Switchover
}

func NewOrchestrator(stations *station.Registry, receipts *telemetry.FlowReceipts, primary *valve.PrimaryService,
	warmup *WarmupService, guard ProtectionGuard, state *SwitchoverStore, recorder *audit.Recorder) (*Orchestrator, error) {
	o := &Orchestrator{
		stations: stations, receipts: receipts, primary: primary, warmup: warmup,
		guard: guard, store: state, audit: recorder, active: make(map[string]Switchover),
	}
	saved, err := state.Load()
	if err != nil {
		return nil, err
	}
	for id, item := range saved {
		o.active[id] = item.Clone()
	}
	return o, nil
}

// Begin validates preconditions and parks the operation at the warmup stage.
func (o *Orchestrator) Begin(stationID, sourceID, targetID, actor string) (Switchover, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	item, ok := o.stations.Get(stationID)
	if !ok {
		return Switchover{}, fmt.Errorf("station %s not found", stationID)
	}
	if sourceID == "" || targetID == "" || sourceID == targetID {
		return Switchover{}, fmt.Errorf("source and target exchangers must differ")
	}
	if item.Exchangers[sourceID] != station.ExchangerCarrying {
		return Switchover{}, fmt.Errorf("source exchanger %s is not carrying load", sourceID)
	}
	if item.Exchangers[targetID] != station.ExchangerStandby {
		return Switchover{}, fmt.Errorf("target exchanger %s is not on standby", targetID)
	}
	if err := o.checkStationAvailable(item); err != nil {
		return Switchover{}, err
	}
	if _, err := o.findActive(stationID, ""); err != nil {
		return Switchover{}, err
	}

	// The standby unit starts priming; the old unit keeps carrying load until
	// the actual primary cutover.
	if _, err := o.stations.SetExchanger(stationID, targetID, station.ExchangerPriming); err != nil {
		return Switchover{}, err
	}
	now := time.Now().UTC()
	operation := Switchover{
		ID: uuid.NewString(), StationID: stationID, SourceExchanger: sourceID, TargetExchanger: targetID,
		Status: StatusActive, CurrentStage: StageWarmup, Actor: actor,
		Stages:    map[SwitchoverStage]StageState{StageWarmup: {Stage: StageWarmup, EnteredAt: now, Note: "awaiting warmup proof"}},
		StartedAt: now, UpdatedAt: now,
	}
	if err := o.persist(operation); err != nil {
		return Switchover{}, err
	}
	o.record("switchover.begin", stationID, audit.OutcomeAccepted, actor, "switchover entered warmup", operation)
	return operation.Clone(), nil
}

// Confirm verifies the evidence required for the current stage and advances
// the operation by one stage. Confirming the final stage completes the
// switchover; the completion gate re-checks flow evidence and the station's
// maintenance/protection state.
func (o *Orchestrator) Confirm(operationID, actor string) (Switchover, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	operation, err := o.requireActive(operationID)
	if err != nil {
		return Switchover{}, err
	}
	// A station that entered maintenance or a protection state mid-flight
	// blocks every further confirmation, not only the final one.
	if item, ok := o.stations.Get(operation.StationID); ok {
		if err := o.checkStationAvailable(item); err != nil {
			failed, failErr := o.failLocked(operation, err, actor)
			if failErr != nil {
				return failed, fmt.Errorf("%w (rollback also failed: %v)", err, failErr)
			}
			return failed, err
		}
	} else {
		return Switchover{}, fmt.Errorf("station %s not found", operation.StationID)
	}

	switch operation.CurrentStage {
	case StageWarmup:
		err = o.confirmWarmup(&operation)
	case StageStabilizeFlow:
		err = o.confirmStabilizedFlow(&operation)
	case StagePrimarySwitch:
		err = o.confirmCutover(&operation)
	case StageDrainOld:
		err = o.confirmDrainAndComplete(&operation)
	default:
		err = fmt.Errorf("unknown stage %q", operation.CurrentStage)
	}
	if err != nil {
		var fatal *executionError
		if !errors.As(err, &fatal) {
			// Retryable rejection (missing proof/receipt): park at the stage.
			return operation.Clone(), err
		}
		failed, failErr := o.failLocked(operation, err, actor)
		if failErr != nil {
			return failed, fmt.Errorf("%w (rollback also failed: %v)", err, failErr)
		}
		return failed, err
	}

	if operation.Status == StatusCompleted {
		o.record("switchover.complete", operation.StationID, audit.OutcomeCompleted, actor, "switchover completed", operation)
	} else {
		o.record("switchover.stage."+string(operation.CurrentStage), operation.StationID, audit.OutcomeAccepted,
			actor, "stage confirmed, awaiting next confirmation", operation)
	}
	return operation.Clone(), nil
}

// Rollback abandons an in-flight operation and applies compensation. Once the
// primary cutover has happened, a stable flow receipt for the source exchanger
// is mandatory so the primary side can be reopened onto a confirmed flow path.
func (o *Orchestrator) Rollback(operationID, reason, actor string) (Switchover, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	operation, err := o.requireMutable(operationID)
	if err != nil {
		return Switchover{}, err
	}
	if reason == "" {
		reason = "operator requested rollback"
	}
	if err := o.rollbackLocked(&operation, reason); err != nil {
		return operation, err
	}
	o.record("switchover.rollback", operation.StationID, audit.OutcomeRejected, actor,
		"switchover rolled back: "+reason, operation)
	return operation.Clone(), nil
}

// Fail marks the operation failed and attempts the compensating rollback.
func (o *Orchestrator) Fail(operationID, reason, actor string) (Switchover, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	operation, err := o.requireActive(operationID)
	if err != nil {
		return Switchover{}, err
	}
	cause := fmt.Errorf("operation failed: %s", reason)
	failed, failErr := o.failLocked(operation, cause, actor)
	if failErr != nil {
		return failed, fmt.Errorf("%w (rollback also failed: %v)", cause, failErr)
	}
	return failed, nil
}

func (o *Orchestrator) confirmWarmup(operation *Switchover) error {
	item, ok := o.stations.Get(operation.StationID)
	if !ok {
		return fmt.Errorf("station %s not found", operation.StationID)
	}
	proof, exists := o.warmup.Latest(operation.StationID)
	if !exists {
		return fmt.Errorf("warmup proof for %s has not been reported", operation.TargetExchanger)
	}
	if proof.ExchangerID != operation.TargetExchanger {
		return fmt.Errorf("warmup proof is for exchanger %s, expected %s", proof.ExchangerID, operation.TargetExchanger)
	}
	if proof.Revision != item.TargetRevision {
		return fmt.Errorf("warmup proof revision %d does not match target revision %d", proof.Revision, item.TargetRevision)
	}
	if !proof.Ready {
		return fmt.Errorf("target exchanger outlet %.1fC is below warmup threshold", proof.Outlet)
	}
	o.enterStage(operation, StageStabilizeFlow, "awaiting stable secondary flow receipt")
	return nil
}

func (o *Orchestrator) confirmStabilizedFlow(operation *Switchover) error {
	if !o.receipts.Confirmed(operation.StationID, operation.TargetExchanger, operation.ID) {
		return fmt.Errorf("stable target flow receipt is required before cutover")
	}
	o.enterStage(operation, StagePrimarySwitch, "ready to cut primary circuit over")
	return nil
}

func (o *Orchestrator) confirmCutover(operation *Switchover) error {
	// Re-verify immediately before moving the primary side.
	if !o.receipts.Confirmed(operation.StationID, operation.TargetExchanger, operation.ID) {
		return fmt.Errorf("target exchanger flow receipt is missing or no longer valid")
	}
	if _, err := o.primary.Open(operation.StationID, operation.TargetExchanger, operation.ID, true); err != nil {
		return abort(fmt.Errorf("open primary admission for target: %w", err))
	}
	if _, err := o.stations.SetExchanger(operation.StationID, operation.SourceExchanger, station.ExchangerDraining); err != nil {
		return abort(err)
	}
	if _, err := o.stations.SetExchanger(operation.StationID, operation.TargetExchanger, station.ExchangerCarrying); err != nil {
		return abort(err)
	}
	o.enterStage(operation, StageDrainOld, "awaiting drain confirmation for old exchanger")
	return nil
}

// confirmDrainAndComplete is the final gate: the old unit must show a stable
// zero-flow drain receipt, the target must still carry a confirmed flow, and
// the station must not have entered maintenance or a protection state.
func (o *Orchestrator) confirmDrainAndComplete(operation *Switchover) error {
	if !o.receipts.Confirmed(operation.StationID, operation.TargetExchanger, operation.ID) {
		return fmt.Errorf("cannot complete switchover without a confirmed target flow receipt")
	}
	if !o.receipts.DrainConfirmed(operation.StationID, operation.SourceExchanger, operation.ID) {
		return fmt.Errorf("old exchanger %s drain is not confirmed", operation.SourceExchanger)
	}
	if _, err := o.stations.SetExchanger(operation.StationID, operation.SourceExchanger, station.ExchangerStandby); err != nil {
		return abort(err)
	}
	now := time.Now().UTC()
	operation.Stages[StageDrainOld] = StageState{
		Stage: StageDrainOld, Confirmed: true,
		EnteredAt:   operation.Stages[StageDrainOld].EnteredAt,
		ConfirmedAt: now,
	}
	operation.Status = StatusCompleted
	operation.CurrentStage = ""
	operation.FailureReason = ""
	operation.RolledBack = false
	operation.CompletedAt = now
	operation.UpdatedAt = now
	return o.persist(*operation)
}

// rollbackLocked applies compensation appropriate to how far the choreography
// progressed.
func (o *Orchestrator) rollbackLocked(operation *Switchover, reason string) error {
	cutOver := false
	if admission, ok := o.primary.Admission(operation.StationID, operation.TargetExchanger); ok &&
		admission.Open && admission.OperationID == operation.ID {
		cutOver = true
	}
	if cutOver {
		// The primary side was moved: reopening the source requires its own
		// stable flow evidence, otherwise the plant would be left without a
		// confirmed flow path.
		if !o.receipts.Confirmed(operation.StationID, operation.SourceExchanger, operation.ID) {
			return fmt.Errorf("cannot reverse cutover: stable flow receipt for source exchanger %s is required", operation.SourceExchanger)
		}
		if _, err := o.primary.Open(operation.StationID, operation.SourceExchanger, operation.ID, true); err != nil {
			return fmt.Errorf("restore primary admission for source: %w", err)
		}
		if _, err := o.primary.Close(operation.StationID, operation.TargetExchanger, operation.ID); err != nil {
			return fmt.Errorf("close primary admission for target: %w", err)
		}
		if _, err := o.stations.SetExchanger(operation.StationID, operation.TargetExchanger, station.ExchangerStandby); err != nil {
			return err
		}
		if _, err := o.stations.SetExchanger(operation.StationID, operation.SourceExchanger, station.ExchangerCarrying); err != nil {
			return err
		}
	} else {
		if _, err := o.stations.SetExchanger(operation.StationID, operation.TargetExchanger, station.ExchangerStandby); err != nil {
			return err
		}
		if _, err := o.stations.SetExchanger(operation.StationID, operation.SourceExchanger, station.ExchangerCarrying); err != nil {
			return err
		}
	}
	now := time.Now().UTC()
	operation.Status = StatusRolledBack
	operation.CurrentStage = ""
	operation.FailureReason = reason
	operation.RolledBack = true
	operation.CompletedAt = now
	operation.UpdatedAt = now
	return o.persist(*operation)
}

// failLocked records the failure and attempts compensation. If compensation
// itself fails the operation stays StatusFailed so an operator can retry the
// rollback manually.
func (o *Orchestrator) failLocked(operation Switchover, cause error, actor string) (Switchover, error) {
	operation.Status = StatusFailed
	operation.FailureReason = cause.Error()
	operation.UpdatedAt = time.Now().UTC()
	if err := o.persist(operation); err != nil {
		return operation, err
	}
	o.record("switchover.failed", operation.StationID, audit.OutcomeFailed, actor, cause.Error(), operation)
	if err := o.rollbackLocked(&operation, cause.Error()); err != nil {
		operation.Status = StatusFailed
		operation.FailureReason = fmt.Sprintf("%s (rollback failed: %v)", cause.Error(), err)
		operation.UpdatedAt = time.Now().UTC()
		_ = o.persist(operation)
		return operation.Clone(), err
	}
	return operation.Clone(), nil
}

func (o *Orchestrator) checkStationAvailable(item station.Station) error {
	if item.Maintenance {
		return fmt.Errorf("station %s is in maintenance", item.ID)
	}
	if !item.Online {
		return fmt.Errorf("station %s is offline", item.ID)
	}
	if o.guard != nil {
		if blocked, reason := o.guard.Blocked(item.ID); blocked {
			return fmt.Errorf("station %s is in protection: %s", item.ID, reason)
		}
	}
	return nil
}

func (o *Orchestrator) findActive(stationID, excludeID string) (Switchover, error) {
	for _, item := range o.active {
		if item.StationID == stationID && item.Status == StatusActive && item.ID != excludeID {
			return item, fmt.Errorf("station %s already has active switchover %s", stationID, item.ID)
		}
	}
	return Switchover{}, nil
}

func (o *Orchestrator) requireActive(operationID string) (Switchover, error) {
	operation, ok := o.active[operationID]
	if !ok {
		return Switchover{}, fmt.Errorf("switchover %s not found", operationID)
	}
	if operation.Status != StatusActive {
		return operation, fmt.Errorf("switchover %s is %s", operationID, operation.Status)
	}
	return operation.Clone(), nil
}

// requireMutable accepts active operations and failed operations whose
// compensation did not complete, so an operator can retry the rollback after
// remediation (e.g. once the source flow receipt arrives).
func (o *Orchestrator) requireMutable(operationID string) (Switchover, error) {
	operation, ok := o.active[operationID]
	if !ok {
		return Switchover{}, fmt.Errorf("switchover %s not found", operationID)
	}
	if operation.Status != StatusActive && operation.Status != StatusFailed {
		return operation, fmt.Errorf("switchover %s is %s", operationID, operation.Status)
	}
	return operation.Clone(), nil
}

func (o *Orchestrator) enterStage(operation *Switchover, stage SwitchoverStage, note string) {
	if previous := operation.CurrentStage; previous != "" {
		state := operation.Stages[previous]
		state.Confirmed = true
		state.ConfirmedAt = time.Now().UTC()
		operation.Stages[previous] = state
	}
	now := time.Now().UTC()
	operation.CurrentStage = stage
	operation.UpdatedAt = now
	operation.Stages[stage] = StageState{Stage: stage, EnteredAt: now, Note: note}
	_ = o.persist(*operation)
}

func (o *Orchestrator) persist(operation Switchover) error {
	operation.UpdatedAt = time.Now().UTC()
	if err := o.store.Put(operation.Clone()); err != nil {
		return err
	}
	o.active[operation.ID] = operation.Clone()
	return nil
}

func (o *Orchestrator) record(action, stationID string, outcome audit.Outcome, actor, message string, operation Switchover) {
	if o.audit == nil {
		return
	}
	if actor == "" {
		actor = "system"
	}
	_, _ = o.audit.Record(audit.Entry{
		Action: action, EntityID: stationID, Actor: actor, Outcome: outcome, Message: message,
		Details: map[string]any{
			"switchover_id":    operation.ID,
			"source_exchanger": operation.SourceExchanger,
			"target_exchanger": operation.TargetExchanger,
			"current_stage":    string(operation.CurrentStage),
			"status":           string(operation.Status),
		},
	})
}

// Get returns a switchover by ID.
func (o *Orchestrator) Get(operationID string) (Switchover, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	item, ok := o.active[operationID]
	return item.Clone(), ok
}

// List returns all switchovers, newest first by start time.
func (o *Orchestrator) List() []Switchover {
	o.mu.Lock()
	defer o.mu.Unlock()
	items := make([]Switchover, 0, len(o.active))
	for _, item := range o.active {
		items = append(items, item.Clone())
	}
	sort.Slice(items, func(i, j int) bool { return items[i].StartedAt.After(items[j].StartedAt) })
	return items
}
