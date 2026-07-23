package hostregistry

import "encoding/json"

// MeshName is the fixed Config.Name of the single shared host mesh: it selects
// ~/.config/synckit/{state.json,rpc.sock,reconcile.lock}.
const MeshName = "synckit"

// MeshBinary is the daemon binary the mesh install-probes over ssh. It differs
// from MeshName: the config dir is "synckit", but the installed binary is
// "synckitd".
const MeshBinary = "synckitd"

// Mesh is the canonical handle to the single shared host mesh, rpc socket, and
// reconcile lock that every synckit consumer registers against.
const meshStateDeclaration = "schema:{identity:string,version:uint64,fingerprint:string};host_registry:{self:string,hosts:array<string>,addrs:map<string,array<string>>};synckit:{}"

// Mesh is the canonical exact-schema handle for the shared host mesh.
var Mesh = Config{Name: MeshName, Binary: MeshBinary, State: StateContract{
	Identity:         "synckit-state-v1",
	Fingerprint:      SchemaFingerprint("synckit-state-v1", meshStateDeclaration),
	ProductNamespace: "synckit",
	InitialProduct:   json.RawMessage(`{}`),
	ValidateProduct:  validateEmptyProduct,
}}

func validateEmptyProduct(raw json.RawMessage) error {
	var value struct{}
	return DecodeExactJSON(raw, &value)
}
