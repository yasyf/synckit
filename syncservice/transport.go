package syncservice

import (
	"github.com/yasyf/daemonkit"

	"github.com/yasyf/synckit/helperruntime"
	"github.com/yasyf/synckit/internal/synctransport"
)

// Resident returns a persistent transport to the resident helper serving name.
// The socket derives from that helper's launchd label, so no caller declares a
// path and none can disagree with the one the helper binds.
func Resident(name string) Transport {
	spec, err := helperruntime.Spec(name, daemonkit.Program{}, 0)
	if err != nil {
		return synctransport.Failed(err)
	}
	return synctransport.Local(spec)
}
