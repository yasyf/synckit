package syncservice

// AllMethods lists every svc.-namespaced method in the typed sync contract, in the
// order they appear in [Capabilities].
var AllMethods = []string{
	MethodCapabilities,
	MethodList,
	MethodReconcile,
	MethodSync,
	MethodGetState,
}

// DefaultCapabilities returns the standard [Capabilities] for a consumer named name
// with a fresh copy of [AllMethods].
func DefaultCapabilities(name string) Capabilities {
	methods := make([]string, len(AllMethods))
	copy(methods, AllMethods)
	return Capabilities{Name: name, Methods: methods}
}
