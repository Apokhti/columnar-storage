package query

import (
	"github.com/Knetic/govaluate"
)

// Calculates Select Expression
func CalculateSelectExpression(exp Expression, mp map[string]interface{}) (interface{}, error) {
	expression, _ := govaluate.NewEvaluableExpression(exp.Fullexpression)
	// fmt.Printf("%s\n", expression)
	result, err := expression.Evaluate(mp)
	return result, err
}
