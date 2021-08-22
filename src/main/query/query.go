package query

import (
	"strings"
)

const SYMBOLS = "[{}().,;+-*/&|<>=~]"

type Query struct {
	QueryType             QueryType
	Table                 string
	SelectExpressionsList []Expression
	WhereExpressionList   []Expression
	OrderBy               string
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

// Returns if character is digit
func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}

// Returns if character is letter
func isSymbol(ch uint16) bool {
	return strings.Contains(SYMBOLS, string(ch))
}

// checks if its expression
func isExpression(value string) bool {
	for i := 0; i < len(value); i++ {
		if !(isDigit(uint16(value[i])) || isSymbol(uint16(value[i]))) {
			return false
		}
	}
	return true
}

func AddValueToExpression(expression *Expression, value string) {

	if !isExpression(value) {
		expression.ExpressionColumns = append(expression.ExpressionColumns, value)
	}

	expression.Fullexpression = expression.Fullexpression + value
}
