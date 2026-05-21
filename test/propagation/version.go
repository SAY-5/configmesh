package propagation

import "runtime"

func runtimeGoVersion() string { return runtime.Version() }
