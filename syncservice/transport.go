package syncservice

import (
	"github.com/yasyf/synckit/internal/synctransport"
)

// Socket returns a persistent transport to the resident Unix socket.
func Socket(sock string) Transport { return synctransport.Socket(sock) }
