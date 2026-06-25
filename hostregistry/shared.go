package hostregistry

// MeshName is the fixed Config.Name of the single shared host mesh: it selects
// ~/.config/synckit/{state.json,rpc.sock,reconcile.lock}.
const MeshName = "synckit"

// Mesh is the canonical handle to the single shared host mesh, rpc socket, and
// reconcile lock that every synckit consumer registers against.
var Mesh = Config{Name: MeshName}
