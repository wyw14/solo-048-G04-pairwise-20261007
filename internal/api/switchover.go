package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"heatpulse/internal/switchover"
)

var errSwitchoverNotFound = errors.New("switchover operation not found")

func (a *App) beginSwitchover(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Source string `json:"source"`
		Target string `json:"target"`
		Actor  string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchover.Begin(chi.URLParam(r, "stationID"), input.Source, input.Target, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, operation)
}

func (a *App) listSwitchovers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Switchover.List(chi.URLParam(r, "stationID")))
}

func (a *App) getSwitchover(w http.ResponseWriter, r *http.Request) {
	operation, ok := a.Switchover.Get(chi.URLParam(r, "operationID"))
	if !ok || operation.StationID != chi.URLParam(r, "stationID") {
		writeError(w, http.StatusNotFound, errSwitchoverNotFound)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (a *App) confirmSwitchoverStage(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Stage switchover.Stage `json:"stage"`
		Actor string           `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchover.Confirm(chi.URLParam(r, "operationID"), input.Stage, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (a *App) rollbackSwitchover(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Reason string `json:"reason"`
		Actor  string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchover.Rollback(chi.URLParam(r, "operationID"), input.Actor, input.Reason)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}
