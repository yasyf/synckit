//go:build !darwin

package netpolicy

// NewMonitor returns a Monitor that reports StatusUnknown forever, keeping bulk
// transfer paused where no OS network cost binding exists. It still watches
// manualPath's directory, creating it if absent, so manual edits publish.
func NewMonitor(manualPath string) (Monitor, error) {
	o, err := newObserved(manualPath)
	if err != nil {
		return nil, err
	}
	return unknownMonitor{o}, nil
}

type unknownMonitor struct {
	*observed
}

func (m unknownMonitor) Close() error {
	return m.close()
}
