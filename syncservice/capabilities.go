package syncservice

import "github.com/yasyf/synckit/artifact"

// AllMethods lists every svc.-namespaced method in the typed sync contract, in the
// order they appear in [Capabilities].
var AllMethods = []string{
	MethodCapabilities,
	MethodList,
	MethodReconcile,
	MethodExport,
	MethodApply,
}

// ArtifactCapabilities returns the [Capabilities] for an artifact consumer named
// name: [AllMethods], the v2 export and apply methods, and [artifact.Methods].
func ArtifactCapabilities(name string) Capabilities {
	methods := make([]string, 0, len(AllMethods)+2+len(artifact.Methods))
	methods = append(methods, AllMethods...)
	methods = append(methods, MethodExportV2, MethodApplyV2)
	methods = append(methods, artifact.Methods...)
	return Capabilities{Name: name, Methods: methods}
}

// DefaultCapabilities returns the standard [Capabilities] for a consumer named name
// with a fresh copy of [AllMethods].
func DefaultCapabilities(name string) Capabilities {
	methods := make([]string, len(AllMethods))
	copy(methods, AllMethods)
	return Capabilities{Name: name, Methods: methods}
}
