package query

import (
	"github.com/Knetic/govaluate"
)

// Calculates Select Expression
func CalculateSelectExpression(exp Expression, mp map[string]interface{}) (interface{}, error) {
	expression, _ := govaluate.NewEvaluableExpression(exp.Fullexpression)

	// fmt.Printf("%v %v\n", exp.Fullexpression, mp)
	result, err := expression.Evaluate(mp)
	// fmt.Printf("%v result\n", result)
	return result, err
}
