package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// decodeOptionalBody accepts an empty body but rejects malformed JSON.
func decodeOptionalBody(r *http.Request, destination any) error {
	if err := decodeJSON(r, destination); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

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
	operation, err := a.Switchovers.Begin(chi.URLParam(r, "stationID"), input.Source, input.Target, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusAccepted, operation)
}

func (a *App) confirmSwitchover(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Actor string `json:"actor"`
	}
	// Body is optional; an empty body keeps the default (system) actor.
	if err := decodeOptionalBody(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchovers.Confirm(chi.URLParam(r, "operationID"), input.Actor)
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
	if err := decodeOptionalBody(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchovers.Rollback(chi.URLParam(r, "operationID"), input.Reason, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (a *App) failSwitchover(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Reason string `json:"reason"`
		Actor  string `json:"actor"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	operation, err := a.Switchovers.Fail(chi.URLParam(r, "operationID"), input.Reason, input.Actor)
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (a *App) getSwitchover(w http.ResponseWriter, r *http.Request) {
	operation, ok := a.Switchovers.Get(chi.URLParam(r, "operationID"))
	if !ok {
		writeError(w, http.StatusNotFound, fmt.Errorf("switchover %s not found", chi.URLParam(r, "operationID")))
		return
	}
	writeJSON(w, http.StatusOK, operation)
}

func (a *App) listSwitchovers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, a.Switchovers.List())
}
