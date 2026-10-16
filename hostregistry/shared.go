package hostregistry

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
)

// MeshName is the fixed Config.Name of the single shared host mesh: it selects
// ~/.config/synckit/{state.json,rpc.sock,reconcile.lock}.
const MeshName = "synckit"

// MeshBinary is the daemon binary the mesh install-probes over ssh. It differs
// from MeshName: the config dir is "synckit", but the installed binary is
// "synckitd".
const MeshBinary = "synckitd"

const manifestsDirName = "manifests"

const meshStateDeclaration = "schema:{identity:string,version:uint64,fingerprint:string};host_registry:{self:string,hosts:array<{identity:string,user:string,host_key_alias:string,addresses:array<string>,synckitd_path:string}>};synckit:{}"

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

// ManifestsDir returns the directory synckitd discovers consumer manifests in:
// manifests under the mesh's config dir, so $XDG_CONFIG_HOME/synckit/manifests,
// or ~/.config/synckit/manifests when XDG_CONFIG_HOME is unset.
func ManifestsDir() (string, error) {
	dir, err := Mesh.Dir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, manifestsDirName), nil
}

// RegisteredManifestPath returns the file `synckitd register` installs the
// manifest named name to, <ManifestsDir>/<name>.json, so a consumer can read its
// own registration with manifest.Load. It refuses a name that is not a plain
// file base name.
func RegisteredManifestPath(name string) (string, error) {
	if name != filepath.Base(name) || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("manifest name %q is not a plain file base name", name)
	}
	dir, err := ManifestsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, name+".json"), nil
}
