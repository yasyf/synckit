package daemon

import (
	"strings"

	"github.com/yasyf/synckit/hostregistry"
)

// remoteCommand joins a rendered argv into a single shell command line for ssh,
// shell-quoting each field so argv boundaries survive the remote shell intact.
// hostregistry.Runner.SSH takes one remote command string (it wraps it in a brew
// shellenv eval), so the rendered argv must be re-joined here; ShellQuote is the
// same quoting hostregistry uses for remote commands.
func remoteCommand(binary string, argv []string) string {
	parts := make([]string, 0, len(argv)+1)
	parts = append(parts, binary)
	for _, a := range argv {
		parts = append(parts, hostregistry.ShellQuote(a))
	}
	return strings.Join(parts, " ")
}
