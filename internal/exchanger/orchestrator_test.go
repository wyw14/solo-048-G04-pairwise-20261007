package exchanger

import (
	"path/filepath"
	"testing"
	"time"

	"heatpulse/internal/audit"
	"heatpulse/internal/station"
	"heatpulse/internal/store"
	"heatpulse/internal/telemetry"
	"heatpulse/internal/valve"
)

type fakeGuard struct {
	blocked bool
	reason  string
}

func (g fakeGuard) Blocked(string) (bool, string) { return g.blocked, g.reason }

type harness struct {
	stations *station.Registry
	receipts *telemetry.FlowReceipts
	primary  *valve.PrimaryService
	warmup   *WarmupService
	store    *SwitchoverStore
	audit    *audit.Recorder
}

func newHarness(t *testing.T, guard ProtectionGuard) (*Orchestrator, *harness) {
	t.Helper()
	directory := t.TempDir()
	stations := station.NewRegistry([]station.Station{{
		ID: "s1", Name: "站一", ZoneID: "north", Online: true, TargetCelsius: 68,
		TargetRevision: 1, ControllerEpoch: 1,
		Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby},
	}})
	receipts := telemetry.NewFlowReceipts()
	primary := valve.NewPrimaryService()
	warmup := NewWarmupService(stations)
	state := NewSwitchoverStore(directory)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(directory, "events.jsonl")))
	orchestrator, err := NewOrchestrator(stations, receipts, primary, warmup, guard, state, recorder)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	return orchestrator, &harness{
		stations: stations, receipts: receipts, primary: primary, warmup: warmup,
		store: state, audit: recorder,
	}
}

func (h *harness) warmupProof(t *testing.T) {
	t.Helper()
	proof, err := h.warmup.Proof("s1", "B", 1, 42)
	if err != nil || !proof.Ready {
		t.Fatalf("warmup proof not ready: ready=%v err=%v", proof.Ready, err)
	}
}

func (h *harness) flow(exchanger string, liters float64, stable bool, operationID string) {
	_ = h.receipts.Record(telemetry.FlowReceipt{
		StationID: "s1", ExchangerID: exchanger, OperationID: operationID,
		LitersSecond: liters, Stable: stable, At: time.Now().UTC(),
	})
}

func (h *harness) phase(t *testing.T, exchanger string, want station.ExchangerPhase) {
	t.Helper()
	item, ok := h.stations.Get("s1")
	if !ok {
		t.Fatal("station missing")
	}
	if got := item.Exchangers[exchanger]; got != want {
		t.Errorf("exchanger %s phase = %s, want %s", exchanger, got, want)
	}
}

func TestSwitchoverHappyPath(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})

	op, err := o.Begin("s1", "A", "B", "op")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if op.CurrentStage != StageWarmup || op.Status != StatusActive {
		t.Fatalf("unexpected initial state: stage=%s status=%s", op.CurrentStage, op.Status)
	}
	h.phase(t, "B", station.ExchangerPriming)

	// Warmup without proof must not advance.
	if _, err := o.Confirm(op.ID, "op"); err == nil {
		t.Fatal("warmup confirmation accepted without proof")
	}
	h.warmupProof(t)
	if op, err = o.Confirm(op.ID, "op"); err != nil {
		t.Fatalf("confirm warmup: %v", err)
	}
	if op.CurrentStage != StageStabilizeFlow {
		t.Fatalf("stage = %s, want stabilize_flow", op.CurrentStage)
	}

	// Flow stabilization requires a matching stable receipt.
	if _, err := o.Confirm(op.ID, "op"); err == nil {
		t.Fatal("flow confirmation accepted without receipt")
	}
	h.flow("B", 8.4, true, op.ID)
	if op, err = o.Confirm(op.ID, "op"); err != nil {
		t.Fatalf("confirm flow: %v", err)
	}
	if op.CurrentStage != StagePrimarySwitch {
		t.Fatalf("stage = %s, want primary_switch", op.CurrentStage)
	}

	// Cutover opens the target primary admission and swaps carrying load.
	if op, err = o.Confirm(op.ID, "op"); err != nil {
		t.Fatalf("confirm cutover: %v", err)
	}
	if op.CurrentStage != StageDrainOld {
		t.Fatalf("stage = %s, want drain_old", op.CurrentStage)
	}
	admission, ok := h.primary.Admission("s1", "B")
	if !ok || !admission.Open {
		t.Fatal("target primary admission not open")
	}
	h.phase(t, "B", station.ExchangerCarrying)
	h.phase(t, "A", station.ExchangerDraining)

	// Completion needs both target flow and a stable zero-flow drain receipt.
	if _, err := o.Confirm(op.ID, "op"); err == nil {
		t.Fatal("completion accepted before drain confirmed")
	}
	h.flow("A", 0, true, op.ID)
	op, err = o.Confirm(op.ID, "op")
	if err != nil {
		t.Fatalf("confirm drain: %v", err)
	}
	if op.Status != StatusCompleted {
		t.Fatalf("status = %s, want completed", op.Status)
	}
	h.phase(t, "A", station.ExchangerStandby)
	h.phase(t, "B", station.ExchangerCarrying)
	for _, stage := range StageOrder {
		if !op.Stages[stage].Confirmed {
			t.Errorf("stage %s not marked confirmed", stage)
		}
	}
}

func TestBeginRejectsUnavailableStation(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})

	if _, err := o.Begin("s1", "A", "A", "op"); err == nil {
		t.Error("identical exchangers accepted")
	}
	if _, err := o.Begin("s1", "B", "A", "op"); err == nil {
		t.Error("non-carrying source accepted")
	}

	// Maintenance state blocks the switchover.
	if _, err := h.stations.Update("s1", func(item *station.Station) error {
		item.Maintenance = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Begin("s1", "A", "B", "op"); err == nil {
		t.Fatal("switchover began while station in maintenance")
	}
	if _, err := h.stations.Update("s1", func(item *station.Station) error {
		item.Maintenance = false
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// A blocking guard (protection latch / active incident) blocks it too.
	guard := fakeGuard{blocked: true, reason: "pressure trip"}
	o2, _ := newHarness(t, guard)
	if _, err := o2.Begin("s1", "A", "B", "op"); err == nil {
		t.Fatal("switchover began while station in protection")
	}
}

func TestMaintenanceMidFlightBlocksAndRollsBack(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})
	op, err := o.Begin("s1", "A", "B", "op")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.stations.Update("s1", func(item *station.Station) error {
		item.Maintenance = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	failed, err := o.Confirm(op.ID, "op")
	if err == nil {
		t.Fatal("confirmation accepted after station entered maintenance")
	}
	if failed.Status != StatusRolledBack {
		t.Fatalf("status = %s, want rolled_back", failed.Status)
	}
	h.phase(t, "B", station.ExchangerStandby)
	h.phase(t, "A", station.ExchangerCarrying)
}

func TestPreCutoverRollback(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})
	op, err := o.Begin("s1", "A", "B", "op")
	if err != nil {
		t.Fatal(err)
	}
	h.warmupProof(t)
	if _, err := o.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	}
	rolled, err := o.Rollback(op.ID, "operator abort", "op")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rolled.Status != StatusRolledBack || !rolled.RolledBack {
		t.Fatalf("unexpected rollback result: %+v", rolled)
	}
	h.phase(t, "B", station.ExchangerStandby)
	h.phase(t, "A", station.ExchangerCarrying)

	// A terminal operation cannot be confirmed again.
	if _, err := o.Confirm(op.ID, "op"); err == nil {
		t.Fatal("confirmation accepted on rolled back operation")
	}
}

func TestPostCutoverRollbackRequiresSourceFlow(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})
	op, _ := o.Begin("s1", "A", "B", "op")
	h.warmupProof(t)
	if _, err := o.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	}
	h.flow("B", 8.4, true, op.ID)
	if _, err := o.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	}
	if op, err := o.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	} else if op.CurrentStage != StageDrainOld {
		t.Fatalf("stage = %s", op.CurrentStage)
	}

	// Reversing a completed cutover without a source-side receipt is refused;
	// the operation stays active for the operator to remediate.
	if _, err := o.Rollback(op.ID, "bad cutover", "op"); err == nil {
		t.Fatal("post-cutover rollback accepted without source flow receipt")
	}
	if current, _ := o.Get(op.ID); current.Status != StatusActive {
		t.Fatalf("status = %s, want active", current.Status)
	}

	h.flow("A", 7.9, true, op.ID)
	rolled, err := o.Rollback(op.ID, "bad cutover", "op")
	if err != nil {
		t.Fatalf("rollback with source receipt: %v", err)
	}
	if rolled.Status != StatusRolledBack {
		t.Fatalf("status = %s", rolled.Status)
	}
	h.phase(t, "A", station.ExchangerCarrying)
	h.phase(t, "B", station.ExchangerStandby)
	if admission, _ := h.primary.Admission("s1", "B"); admission.Open {
		t.Error("target primary admission still open after rollback")
	}
	if admission, _ := h.primary.Admission("s1", "A"); !admission.Open {
		t.Error("source primary admission not reopened after rollback")
	}
}

func TestMissingDrainReceiptStaysRetryableThenRollsBack(t *testing.T) {
	o, h := newHarness(t, fakeGuard{})
	op, _ := o.Begin("s1", "A", "B", "op")
	h.warmupProof(t)
	_, _ = o.Confirm(op.ID, "op")
	h.flow("B", 8.4, true, op.ID)
	_, _ = o.Confirm(op.ID, "op")
	_, _ = o.Confirm(op.ID, "op")

	// No drain receipt: completion is a retryable rejection, the operation
	// stays active at drain_old.
	rejected, err := o.Confirm(op.ID, "op")
	if err == nil {
		t.Fatal("completion accepted without drain receipt")
	}
	if rejected.Status != StatusActive || rejected.CurrentStage != StageDrainOld {
		t.Fatalf("state = %s/%s, want active/drain_old", rejected.Status, rejected.CurrentStage)
	}

	// Operator aborts; post-cutover compensation needs a source receipt first.
	if _, err := o.Rollback(op.ID, "abort after missing drain", "op"); err == nil {
		t.Fatal("post-cutover rollback accepted without source flow receipt")
	}
	h.flow("A", 7.9, true, op.ID)
	rolled, err := o.Rollback(op.ID, "abort after missing drain", "op")
	if err != nil {
		t.Fatalf("rollback after remediation: %v", err)
	}
	if rolled.Status != StatusRolledBack {
		t.Fatalf("status = %s, want rolled_back", rolled.Status)
	}
}

func TestFatalStageFailureRollsBack(t *testing.T) {
	// A forced Fail() while still at a pre-cutover stage restores the
	// source/target phases without requiring any flow receipt.
	o, h := newHarness(t, fakeGuard{})
	op, _ := o.Begin("s1", "A", "B", "op")
	h.warmupProof(t)
	if _, err := o.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	}
	failed, err := o.Fail(op.ID, "priming pump trip", "system")
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if failed.Status != StatusRolledBack {
		t.Fatalf("status = %s, want rolled_back", failed.Status)
	}
	h.phase(t, "A", station.ExchangerCarrying)
	h.phase(t, "B", station.ExchangerStandby)
}

func TestSwitchoverPersistsAcrossRestart(t *testing.T) {
	directory := t.TempDir()
	stations := station.NewRegistry([]station.Station{{
		ID: "s1", Online: true, TargetRevision: 1,
		Exchangers: map[string]station.ExchangerPhase{"A": station.ExchangerCarrying, "B": station.ExchangerStandby},
	}})
	receipts := telemetry.NewFlowReceipts()
	primary := valve.NewPrimaryService()
	warmup := NewWarmupService(stations)
	store1 := NewSwitchoverStore(directory)
	recorder := audit.NewRecorder(store.NewEventLog(filepath.Join(directory, "events.jsonl")))
	o1, err := NewOrchestrator(stations, receipts, primary, warmup, fakeGuard{}, store1, recorder)
	if err != nil {
		t.Fatal(err)
	}
	op, _ := o1.Begin("s1", "A", "B", "op")
	h := &harness{stations: stations, receipts: receipts, warmup: warmup}
	h.warmupProof(t)
	if _, err := o1.Confirm(op.ID, "op"); err != nil {
		t.Fatal(err)
	}
	receipts2 := telemetry.NewFlowReceipts()
	_ = receipts2.Record(telemetry.FlowReceipt{
		StationID: "s1", ExchangerID: "B", OperationID: op.ID,
		LitersSecond: 8.4, Stable: true, At: time.Now().UTC(),
	})

	// Simulate a restart: rebuild the orchestrator from the same directory.
	store2 := NewSwitchoverStore(directory)
	o2, err := NewOrchestrator(stations, receipts2, primary, warmup, fakeGuard{}, store2, recorder)
	if err != nil {
		t.Fatal(err)
	}
	loaded, ok := o2.Get(op.ID)
	if !ok {
		t.Fatal("switchover not found after restart")
	}
	if loaded.Status != StatusActive || loaded.CurrentStage != StageStabilizeFlow {
		t.Fatalf("reloaded state = %s/%s", loaded.Status, loaded.CurrentStage)
	}
	if !loaded.Stages[StageWarmup].Confirmed {
		t.Error("warmup confirmation lost across restart")
	}
	if len(o2.List()) != 1 {
		t.Fatalf("list after restart = %d items", len(o2.List()))
	}
	// The reloaded operation can continue to completion.
	if _, err := o2.Confirm(op.ID, "op"); err != nil {
		t.Fatalf("confirm reloaded flow stage: %v", err)
	}
	if _, err := o2.Confirm(op.ID, "op"); err != nil {
		t.Fatalf("confirm reloaded cutover: %v", err)
	}
	_ = receipts2.Record(telemetry.FlowReceipt{
		StationID: "s1", ExchangerID: "A", OperationID: op.ID,
		LitersSecond: 0, Stable: true, At: time.Now().UTC(),
	})
	completed, err := o2.Confirm(op.ID, "op")
	if err != nil {
		t.Fatalf("complete reloaded switchover: %v", err)
	}
	if completed.Status != StatusCompleted {
		t.Fatalf("status = %s", completed.Status)
	}
}
