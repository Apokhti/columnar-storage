package query

import (
	"bufio"
	"cs/src/main/manager"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ExecuteQuerry Main stuff
func ExecuteQuery(td *manager.TableData, query *Query) []interface{} {
	// List of expressions
	// filterExpressions := query.WhereExpressionList
	// firstExpr := filterExpressions[0]
	// fmt.Printf("%v Expression\n", firstExpr)
	variables := variablesToSelect(query.SelectExpressionsList)
	fmt.Printf("%v\n", variables)
	result := filterResults(td, variables, query.SelectExpressionsList, query.WhereExpressionList)
	return result
}

func variablesToSelect(selectExpressions []Expression) []string {
	variables := []string{}
	for _, expression := range selectExpressions {
		variables = append(variables, expression.ExpressionColumns...)
	}
	return variables
}

func filterResults(fs *manager.TableData, variables []string, selectExpressions []Expression, filterExpressions []Expression) []interface{} {
	resultSlice := []interface{}{}
	dirpath, _ := os.Getwd()

	for _, filterExpression := range filterExpressions {
		indexed := checkIfIndexed(filterExpression.ExpressionColumns)
		fmt.Printf(" indexed %v\n", indexed)
		if !indexed {
			return resultSlice
		}

		files := make([]*os.File, len(fs.Columns))
		readers := make([]*bufio.Reader, len(fs.Columns))

		for i, column := range fs.Columns {
			files[i], _ = os.Open(dirpath + "/data/" + filterExpression.ExpressionColumns[0] + "-Indexed/" + column.ColumnName)
			readers[i] = bufio.NewReader(files[i])
		}

		for {
			records := manager.NextRecords(readers)
			if records[0] == "" {
				break
			}
			mp := createRowMap(records, fs)
			// fmt.Printf("mp %v\n", mp)

			result, _ := CalculateSelectExpression(filterExpression, mp)
			converted := fmt.Sprintf("%v", result)
			row := []interface{}{}
			if converted == "true" {
				for _, selectExpression := range selectExpressions {
					result, _ := CalculateSelectExpression(selectExpression, mp)
					row = append(row, result)
				}
				// for _, variable := range variables {
				// 	row = append(row, mp[variable])
				// }
				resultSlice = append(resultSlice, row)
				fmt.Printf("%v \n", row)
			}

		}

		break

	}
	return resultSlice
}

func createRowMap(records []string, fs *manager.TableData) map[string]interface{} {
	result := map[string]interface{}{}
	for i, column := range fs.Columns {
		record := records[i][strings.Index(records[i], ")")+1:]
		if column.ColumnName == "ID" {
			i, _ := strconv.Atoi(record)
			result[column.ColumnName] = i
		} else {
			result[column.ColumnName] = record
		}
	}
	return result
}

func checkIfIndexed(column []string) bool {
	dirpath, _ := os.Getwd()
	path := dirpath + "/data/" + column[0] + "-Indexed"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
