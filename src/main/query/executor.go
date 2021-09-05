package query

import (
	"cs/src/main/manager"
	"fmt"
)

func ExecuteQuery(td *manager.TableData, query *Query) map[string]interface{} {
	// List of expressions
	filterExpressions := query.WhereExpressionList
	firstExpr := filterExpressions[0]
	fmt.Printf("%v Expression\n", firstExpr)

	return nil
}
