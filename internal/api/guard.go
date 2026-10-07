package api

import (
	"fmt"

	"heatpulse/internal/protection"
)

// stationGuard adapts the protection subsystem to the exchanger
// ProtectionGuard: a station is blocked while any protection latch is set or
// an incident against it remains active.
type stationGuard struct {
	latches   *protection.LatchStore
	incidents *protection.Service
}

func newStationGuard(latches *protection.LatchStore, incidents *protection.Service) stationGuard {
	return stationGuard{latches: latches, incidents: incidents}
}

func (g stationGuard) Blocked(stationID string) (bool, string) {
	latches := g.latches.Get(stationID)
	switch {
	case latches.PressureTrip:
		return true, fmt.Sprintf("pressure trip latched at revision %d", latches.Revision)
	case latches.FreezeRisk:
		return true, fmt.Sprintf("freeze risk latched at revision %d", latches.Revision)
	case latches.HighReturn:
		return true, fmt.Sprintf("high return temperature latched at revision %d", latches.Revision)
	}
	for _, incident := range g.incidents.List() {
		if incident.StationID == stationID && incident.Active {
			return true, fmt.Sprintf("active %s incident %s", incident.Kind, incident.ID)
		}
	}
	return false, ""
}
