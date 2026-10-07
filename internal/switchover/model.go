package switchover

import (
	"fmt"
	"time"
)

// Stage is one step of the primary/standby exchanger switchover sequence.
type Stage string

const (
	StagePreheat    Stage = "preheat"    // 预热
	StageFlow       Stage = "flow"       // 稳定流量
	StageSwitchover Stage = "switchover" // 主备切换
	StageDrain      Stage = "drain"      // 旧设备排空
)

var stageOrder = []Stage{StagePreheat, StageFlow, StageSwitchover, StageDrain}

func stageAfter(current Stage) (Stage, bool) {
	for index, stage := range stageOrder {
		if stage == current && index+1 < len(stageOrder) {
			return stageOrder[index+1], true
		}
	}
	return "", false
}

func validStage(candidate Stage) bool {
	for _, stage := range stageOrder {
		if stage == candidate {
			return true
		}
	}
	return false
}

// Status is the lifecycle state of a switchover operation.
type Status string

const (
	StatusInProgress Status = "in_progress"
	StatusCompleted  Status = "completed"
	StatusRolledBack Status = "rolled_back"
)

// StageStatus marks whether a stage has been confirmed.
type StageStatus string

const (
	StagePending   StageStatus = "pending"
	StageConfirmed StageStatus = "confirmed"
)

type StageRecord struct {
	Stage       Stage       `json:"stage"`
	Status      StageStatus `json:"status"`
	Actor       string      `json:"actor,omitempty"`
	Detail      string      `json:"detail,omitempty"`
	ConfirmedAt time.Time   `json:"confirmed_at,omitempty"`
}

type Operation struct {
	ID              string        `json:"id"`
	StationID       string        `json:"station_id"`
	SourceExchanger string        `json:"source_exchanger"`
	TargetExchanger string        `json:"target_exchanger"`
	Status          Status        `json:"status"`
	Stage           Stage         `json:"stage,omitempty"`
	Stages          []StageRecord `json:"stages"`
	Error           string        `json:"error,omitempty"`
	StartedAt       time.Time     `json:"started_at"`
	CompletedAt     time.Time     `json:"completed_at,omitempty"`
	UpdatedAt       time.Time     `json:"updated_at"`
}

func (o Operation) terminal() bool {
	return o.Status == StatusCompleted || o.Status == StatusRolledBack
}

func (o Operation) stageConfirmed(stage Stage) bool {
	for _, record := range o.Stages {
		if record.Stage == stage {
			return record.Status == StageConfirmed
		}
	}
	return false
}

func newOperation(id, stationID, sourceID, targetID string, now time.Time) Operation {
	stages := make([]StageRecord, 0, len(stageOrder))
	for _, stage := range stageOrder {
		stages = append(stages, StageRecord{Stage: stage, Status: StagePending})
	}
	return Operation{
		ID: id, StationID: stationID, SourceExchanger: sourceID, TargetExchanger: targetID,
		Status: StatusInProgress, Stage: StagePreheat, Stages: stages,
		StartedAt: now, UpdatedAt: now,
	}
}

func (o *Operation) confirm(stage Stage, actor, detail string, now time.Time) error {
	if o.Stage != stage {
		return fmt.Errorf("operation is awaiting stage %s, not %s", o.Stage, stage)
	}
	for index := range o.Stages {
		if o.Stages[index].Stage == stage {
			o.Stages[index].Status = StageConfirmed
			o.Stages[index].Actor = actor
			o.Stages[index].Detail = detail
			o.Stages[index].ConfirmedAt = now
			break
		}
	}
	next, ok := stageAfter(stage)
	if ok {
		o.Stage = next
	} else {
		o.Stage = ""
		o.Status = StatusCompleted
		o.CompletedAt = now
	}
	o.UpdatedAt = now
	return nil
}
