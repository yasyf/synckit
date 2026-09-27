package netpolicy

import (
	"fmt"
	"sync"
	"time"

	"github.com/ebitengine/purego"
	"github.com/ebitengine/purego/objc"
)

const (
	networkFramework = "/System/Library/Frameworks/Network.framework/Network"
	libSystem        = "/usr/lib/libSystem.B.dylib"
	queueLabel       = "com.yasyf.synckit.netpolicy"
)

const (
	nwPathStatusSatisfied   int32 = 1
	nwPathStatusUnsatisfied int32 = 2
	nwPathStatusSatisfiable int32 = 3
	nwInterfaceTypeCellular int32 = 2
)

var (
	nwPathMonitorCreate           func() uintptr
	nwPathMonitorSetQueue         func(monitor, queue uintptr)
	nwPathMonitorSetUpdateHandler func(monitor uintptr, handler objc.Block)
	nwPathMonitorSetCancelHandler func(monitor uintptr, handler objc.Block)
	nwPathMonitorStart            func(monitor uintptr)
	nwPathMonitorCancel           func(monitor uintptr)
	nwPathGetStatus               func(path uintptr) int32
	nwPathIsExpensive             func(path uintptr) bool
	nwPathIsConstrained           func(path uintptr) bool
	nwPathUsesInterfaceType       func(path uintptr, kind int32) bool
	nwRelease                     func(object uintptr)
	dispatchQueueCreate           func(label string, attr uintptr) uintptr
	dispatchRelease               func(object uintptr)
)

var loadNetwork = sync.OnceValue(func() error {
	network, err := purego.Dlopen(networkFramework, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("dlopen %s: %w", networkFramework, err)
	}
	system, err := purego.Dlopen(libSystem, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("dlopen %s: %w", libSystem, err)
	}
	purego.RegisterLibFunc(&nwPathMonitorCreate, network, "nw_path_monitor_create")
	purego.RegisterLibFunc(&nwPathMonitorSetQueue, network, "nw_path_monitor_set_queue")
	purego.RegisterLibFunc(&nwPathMonitorSetUpdateHandler, network, "nw_path_monitor_set_update_handler")
	purego.RegisterLibFunc(&nwPathMonitorSetCancelHandler, network, "nw_path_monitor_set_cancel_handler")
	purego.RegisterLibFunc(&nwPathMonitorStart, network, "nw_path_monitor_start")
	purego.RegisterLibFunc(&nwPathMonitorCancel, network, "nw_path_monitor_cancel")
	purego.RegisterLibFunc(&nwPathGetStatus, network, "nw_path_get_status")
	purego.RegisterLibFunc(&nwPathIsExpensive, network, "nw_path_is_expensive")
	purego.RegisterLibFunc(&nwPathIsConstrained, network, "nw_path_is_constrained")
	purego.RegisterLibFunc(&nwPathUsesInterfaceType, network, "nw_path_uses_interface_type")
	purego.RegisterLibFunc(&nwRelease, network, "nw_release")
	purego.RegisterLibFunc(&dispatchQueueCreate, system, "dispatch_queue_create")
	purego.RegisterLibFunc(&dispatchRelease, system, "dispatch_release")
	return nil
})

type pathMonitor struct {
	*observed
	monitor   uintptr
	queue     uintptr
	update    objc.Block
	cancel    objc.Block
	cancelled chan struct{}
	closeOnce sync.Once
}

// NewMonitor starts a Network.framework path monitor on a private serial
// dispatch queue and a watch on manualPath's directory, creating it if absent.
// Until its first path update lands, Current reports StatusUnknown.
func NewMonitor(manualPath string) (Monitor, error) {
	if err := loadNetwork(); err != nil {
		return nil, err
	}
	o, err := newObserved(manualPath)
	if err != nil {
		return nil, err
	}
	m := &pathMonitor{observed: o, cancelled: make(chan struct{})}
	m.update = objc.NewBlock(func(_ objc.Block, path uintptr) {
		m.publish(readPath(path))
	})
	m.cancel = objc.NewBlock(func(objc.Block) {
		close(m.cancelled)
	})
	m.queue = dispatchQueueCreate(queueLabel, 0)
	m.monitor = nwPathMonitorCreate()
	nwPathMonitorSetQueue(m.monitor, m.queue)
	nwPathMonitorSetUpdateHandler(m.monitor, m.update)
	nwPathMonitorSetCancelHandler(m.monitor, m.cancel)
	nwPathMonitorStart(m.monitor)
	return m, nil
}

func readPath(path uintptr) State {
	s := State{
		Status:      StatusUnknown,
		Expensive:   nwPathIsExpensive(path),
		Constrained: nwPathIsConstrained(path),
		Cellular:    nwPathUsesInterfaceType(path, nwInterfaceTypeCellular),
		ObservedAt:  time.Now(),
	}
	switch nwPathGetStatus(path) {
	case nwPathStatusSatisfied:
		s.Status = StatusConnected
	case nwPathStatusUnsatisfied, nwPathStatusSatisfiable:
		s.Status = StatusDisconnected
	}
	return s
}

func (m *pathMonitor) Close() error {
	m.closeOnce.Do(func() {
		nwPathMonitorCancel(m.monitor)
		<-m.cancelled
		nwRelease(m.monitor)
		m.update.Release()
		m.cancel.Release()
		dispatchRelease(m.queue)
	})
	return m.close()
}
