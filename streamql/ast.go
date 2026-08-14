package streamql

type Query struct {
	Select   []string
	From     string
	Where    string
	GroupBy  []string
	WindowMs int
}
