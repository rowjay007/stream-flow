package processor

type Runtime struct {
	ops []Operator
}

func NewRuntime(ops ...Operator) *Runtime {
	return &Runtime{ops: ops}
}

func (r *Runtime) Run(in <-chan Record) <-chan Record {
	var ch <-chan Record = in
	for _, op := range r.ops {
		ch = op.Process(ch)
	}
	return ch
}
