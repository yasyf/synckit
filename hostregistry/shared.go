package hostregistry

// MeshName is the fixed Config.Name of the single shared host mesh: it selects
// ~/.config/synckit/{state.json,rpc.sock,reconcile.lock}.
const MeshName = "synckit"

// MeshBinary is the daemon binary the mesh install-probes over ssh. It differs
// from MeshName: the config dir is "synckit", but the installed binary is
// "synckitd".
const MeshBinary = "synckitd"

// Mesh is the canonical handle to the single shared host mesh, rpc socket, and
// reconcile lock that every synckit consumer registers against.
var Mesh = Config{Name: MeshName, Binary: MeshBinary}
