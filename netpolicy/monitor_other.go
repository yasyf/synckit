//go:build !darwin

package netpolicy

// NewMonitor returns a Monitor that reports StatusUnknown forever, keeping bulk
// transfer paused where no OS network cost binding exists.
func NewMonitor(manualPath string) (Monitor, error) {
	return unknownMonitor{newObserved(manualPath)}, nil
}

type unknownMonitor struct {
	*observed
}

func (unknownMonitor) Close() error {
	return nil
}
