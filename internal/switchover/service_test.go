package switchover

import (
	"path/filepath"
	"strings"
	"testing"

	"heatpulse/internal/audit"
	"heatpulse/internal/exchanger"
	"heatpulse/internal/protection"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

type fixture struct {
	service  *Service
	stations *station.Registry
	warmup   *exchanger.WarmupService
	receipts *telemetry.FlowReceipts
	primary  *valve.PrimaryService
	latches  *protection.LatchStore
	dir      string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	dir := t.TempDir()
	stations := station.NewRegistry([]station.Station{
		{ID: "s1", Name: "测试站", ZoneID: "z1", Online: true, Exchangers: map[string]station.ExchangerPhase{
			"A": station.ExchangerCarrying, "B": station.ExchangerStandby,
		}},
	})
	latches := protection.NewLatchStore(store.NewStationSnapshotStore(dir))
	warmup := exchanger.NewWarmupService(stations)
	receipts := telemetry.NewFlowReceipts()
	primary := valve.NewPrimaryService()
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(dir, "events.jsonl")))
	service, err := NewService(dir, stations, latches, warmup, receipts, primary, recorder)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return fixture{service: service, stations: stations, warmup: warmup, receipts: receipts, primary: primary, latches: latches, dir: dir}
}

func (f fixture) phase(t *testing.T, exchangerID string) station.ExchangerPhase {
	t.Helper()
	item, ok := f.stations.Get("s1")
	if !ok {
		t.Fatalf("station s1 missing")
	}
	return item.Exchangers[exchangerID]
}

func (f fixture) proveWarmup(t *testing.T, exchangerID string) {
	t.Helper()
	proof, err := f.warmup.Proof("s1", exchangerID, 1, 45)
	if err != nil {
		t.Fatalf("warmup proof: %v", err)
	}
	if !proof.Ready {
		t.Fatalf("warmup proof not ready: %+v", proof)
	}
}

func (f fixture) recordFlow(t *testing.T, exchangerID, operationID string, liters float64, stable bool) {
	t.Helper()
	if err := f.receipts.Record(telemetry.FlowReceipt{
		StationID: "s1", ExchangerID: exchangerID, OperationID: operationID,
		LitersSecond: liters, Stable: stable,
	}); err != nil {
		t.Fatalf("record flow receipt: %v", err)
	}
}

func (f fixture) confirm(t *testing.T, operationID string, stage Stage) Operation {
	t.Helper()
	operation, err := f.service.Confirm(operationID, stage, "tester")
	if err != nil {
		t.Fatalf("confirm %s: %v", stage, err)
	}
	return operation
}

func TestHappyPath(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if operation.Status != StatusInProgress || operation.Stage != StagePreheat {
		t.Fatalf("unexpected initial state: %+v", operation)
	}
	if phase := f.phase(t, "B"); phase != station.ExchangerPriming {
		t.Fatalf("target should be priming, got %s", phase)
	}

	if _, err := f.service.Confirm(operation.ID, StagePreheat, "tester"); err == nil {
		t.Fatalf("preheat confirm should fail without warmup proof")
	}
	f.proveWarmup(t, "B")
	operation = f.confirm(t, operation.ID, StagePreheat)
	if operation.Stage != StageFlow {
		t.Fatalf("expected flow stage, got %s", operation.Stage)
	}

	if _, err := f.service.Confirm(operation.ID, StageFlow, "tester"); err == nil {
		t.Fatalf("flow confirm should fail without stable receipt")
	}
	f.recordFlow(t, "B", operation.ID, 5.2, true)
	operation = f.confirm(t, operation.ID, StageFlow)

	if _, err := f.service.Confirm(operation.ID, StageSwitchover, "tester"); err != nil {
		t.Fatalf("switchover confirm: %v", err)
	}
	if phase := f.phase(t, "B"); phase != station.ExchangerCarrying {
		t.Fatalf("target should be carrying, got %s", phase)
	}
	if phase := f.phase(t, "A"); phase != station.ExchangerDraining {
		t.Fatalf("source should be draining, got %s", phase)
	}
	admission, ok := f.primary.Admission("s1", "B")
	if !ok || !admission.Open {
		t.Fatalf("primary admission should be open: %+v", admission)
	}

	if _, err := f.service.Confirm(operation.ID, StageDrain, "tester"); err == nil {
		t.Fatalf("drain confirm should fail without source drain receipt")
	}
	f.recordFlow(t, "A", operation.ID, 0.4, false)
	operation = f.confirm(t, operation.ID, StageDrain)
	if operation.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s", operation.Status)
	}
	if phase := f.phase(t, "A"); phase != station.ExchangerStandby {
		t.Fatalf("source should be standby after drain, got %s", phase)
	}
	for _, record := range operation.Stages {
		if record.Status != StageConfirmed {
			t.Fatalf("stage %s not confirmed: %+v", record.Stage, record)
		}
	}
}

func TestConfirmRejectedUnderMaintenance(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	f.proveWarmup(t, "B")
	if _, err := f.stations.Update("s1", func(item *station.Station) error {
		item.Maintenance = true
		return nil
	}); err != nil {
		t.Fatalf("enter maintenance: %v", err)
	}
	if _, err := f.service.Confirm(operation.ID, StagePreheat, "tester"); err == nil || !strings.Contains(err.Error(), "maintenance") {
		t.Fatalf("confirm under maintenance should fail, got %v", err)
	}
	held, ok := f.service.Get(operation.ID)
	if !ok || held.Status != StatusInProgress || held.Stage != StagePreheat {
		t.Fatalf("operation should hold at preheat: %+v", held)
	}
	if _, err := f.stations.Update("s1", func(item *station.Station) error {
		item.Maintenance = false
		return nil
	}); err != nil {
		t.Fatalf("exit maintenance: %v", err)
	}
	f.confirm(t, operation.ID, StagePreheat)
}

func TestConfirmRejectedUnderProtectionLatch(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	f.proveWarmup(t, "B")
	if _, err := f.latches.Apply(protection.Latches{StationID: "s1", Revision: 1, HighReturn: true}); err != nil {
		t.Fatalf("apply latch: %v", err)
	}
	if _, err := f.service.Confirm(operation.ID, StagePreheat, "tester"); err == nil || !strings.Contains(err.Error(), "protection") {
		t.Fatalf("confirm under protection latch should fail, got %v", err)
	}
	if _, err := f.latches.Reset("s1", 2); err != nil {
		t.Fatalf("reset latches: %v", err)
	}
	f.confirm(t, operation.ID, StagePreheat)
}

func TestBeginRejectedUnderGuard(t *testing.T) {
	f := newFixture(t)
	if _, err := f.latches.Apply(protection.Latches{StationID: "s1", Revision: 1, PressureTrip: true}); err != nil {
		t.Fatalf("apply latch: %v", err)
	}
	if _, err := f.service.Begin("s1", "A", "B", "tester"); err == nil {
		t.Fatalf("begin under protection latch should fail")
	}
}

func TestBeginValidatesExchangers(t *testing.T) {
	f := newFixture(t)
	if _, err := f.service.Begin("s1", "B", "A", "tester"); err == nil {
		t.Fatalf("begin with non-carrying source should fail")
	}
	if _, err := f.service.Begin("s1", "A", "C", "tester"); err == nil {
		t.Fatalf("begin with unknown target should fail")
	}
	if _, err := f.service.Begin("s1", "A", "A", "tester"); err == nil {
		t.Fatalf("begin with identical exchangers should fail")
	}
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := f.service.Begin("s1", "A", "B", "tester"); err == nil {
		t.Fatalf("second begin while one is in progress should fail")
	}
	if _, err := f.service.Rollback(operation.ID, "tester", "cleanup"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

func TestRollbackBeforeSwitchover(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	f.proveWarmup(t, "B")
	f.confirm(t, operation.ID, StagePreheat)
	operation, err = f.service.Rollback(operation.ID, "tester", "preheat aborted")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if operation.Status != StatusRolledBack || operation.Error != "preheat aborted" {
		t.Fatalf("unexpected rollback state: %+v", operation)
	}
	if phase := f.phase(t, "A"); phase != station.ExchangerCarrying {
		t.Fatalf("source should keep carrying, got %s", phase)
	}
	if phase := f.phase(t, "B"); phase != station.ExchangerStandby {
		t.Fatalf("target should return to standby, got %s", phase)
	}
	if _, err := f.service.Confirm(operation.ID, StageFlow, "tester"); err == nil {
		t.Fatalf("confirm on rolled back operation should fail")
	}
	if _, err := f.service.Rollback(operation.ID, "tester", "again"); err == nil {
		t.Fatalf("second rollback should fail")
	}
}

func TestRollbackAfterSwitchover(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	f.proveWarmup(t, "B")
	f.confirm(t, operation.ID, StagePreheat)
	f.recordFlow(t, "B", operation.ID, 5.2, true)
	f.confirm(t, operation.ID, StageFlow)
	f.confirm(t, operation.ID, StageSwitchover)
	operation, err = f.service.Rollback(operation.ID, "tester", "drain pump fault")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if operation.Status != StatusRolledBack {
		t.Fatalf("expected rolled_back, got %s", operation.Status)
	}
	if phase := f.phase(t, "A"); phase != station.ExchangerCarrying {
		t.Fatalf("source should resume carrying, got %s", phase)
	}
	if phase := f.phase(t, "B"); phase != station.ExchangerStandby {
		t.Fatalf("target should return to standby, got %s", phase)
	}
	admission, ok := f.primary.Admission("s1", "B")
	if !ok || admission.Open {
		t.Fatalf("primary admission should be closed: %+v", admission)
	}
}

func TestOperationsSurviveRestart(t *testing.T) {
	f := newFixture(t)
	operation, err := f.service.Begin("s1", "A", "B", "tester")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	f.proveWarmup(t, "B")
	f.confirm(t, operation.ID, StagePreheat)

	// Simulate a restart: fresh registry (seed phases) and telemetry, same data directory.
	restarted := newFixtureFromDir(t, f.dir)
	restored, ok := restarted.service.Get(operation.ID)
	if !ok {
		t.Fatalf("operation should be queryable after restart")
	}
	if restored.Stage != StageFlow || restored.Status != StatusInProgress {
		t.Fatalf("unexpected restored state: %+v", restored)
	}
	if !restored.stageConfirmed(StagePreheat) {
		t.Fatalf("preheat confirmation should persist")
	}
	if phase := restarted.phase(t, "B"); phase != station.ExchangerPriming {
		t.Fatalf("reconcile should restore target priming, got %s", phase)
	}
	if phase := restarted.phase(t, "A"); phase != station.ExchangerCarrying {
		t.Fatalf("reconcile should keep source carrying, got %s", phase)
	}

	// Finish the operation on the restarted instance and restart once more.
	restarted.proveWarmup(t, "B")
	restarted.recordFlow(t, "B", operation.ID, 5.2, true)
	restarted.confirm(t, operation.ID, StageFlow)
	restarted.confirm(t, operation.ID, StageSwitchover)
	restarted.recordFlow(t, "A", operation.ID, 0.4, false)
	restarted.confirm(t, operation.ID, StageDrain)

	again := newFixtureFromDir(t, f.dir)
	restored, ok = again.service.Get(operation.ID)
	if !ok || restored.Status != StatusCompleted {
		t.Fatalf("completed operation should persist: %+v", restored)
	}
	if phase := again.phase(t, "B"); phase != station.ExchangerCarrying {
		t.Fatalf("reconcile should restore target carrying, got %s", phase)
	}
	if phase := again.phase(t, "A"); phase != station.ExchangerStandby {
		t.Fatalf("reconcile should restore source standby, got %s", phase)
	}
	list := again.service.List("s1")
	if len(list) != 1 || list[0].ID != operation.ID {
		t.Fatalf("list should return the persisted operation: %+v", list)
	}
}

func newFixtureFromDir(t *testing.T, dir string) fixture {
	t.Helper()
	stations := station.NewRegistry([]station.Station{
		{ID: "s1", Name: "测试站", ZoneID: "z1", Online: true, Exchangers: map[string]station.ExchangerPhase{
			"A": station.ExchangerCarrying, "B": station.ExchangerStandby,
		}},
	})
	latches := protection.NewLatchStore(store.NewStationSnapshotStore(dir))
	warmup := exchanger.NewWarmupService(stations)
	receipts := telemetry.NewFlowReceipts()
	primary := valve.NewPrimaryService()
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(dir, "events.jsonl")))
	service, err := NewService(dir, stations, latches, warmup, receipts, primary, recorder)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return fixture{service: service, stations: stations, warmup: warmup, receipts: receipts, primary: primary, latches: latches, dir: dir}
}
