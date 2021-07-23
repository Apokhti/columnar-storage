package query

type Query struct {
	QueryType       QueryType
	Table           string
	ExpressionsList []Expression
}

type QueryType int

const (
	UnknownType QueryType = iota
	Select
)

type Expression struct {
	ExpressionColumns []string // {'price', 'დღგ'}
	Fullexpression    string   //"( price + დღგ ) * 0.2 "
}
