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

// DefaultCapabilities returns the standard [Capabilities] for a consumer named name:
// the current [ProtocolVersion] and a fresh copy of [AllMethods], so a mutation of
// the returned slice never aliases the package-level list.
func DefaultCapabilities(name string) Capabilities {
	methods := make([]string, len(AllMethods))
	copy(methods, AllMethods)
	return Capabilities{
		Name:            name,
		ProtocolVersion: ProtocolVersion,
		Methods:         methods,
	}
}
