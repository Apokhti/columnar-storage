package query

import (
	"cs/src/main/manager"
	"cs/src/main/utils"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// ExecuteQuerry Main stuff
func ExecuteQuery(td *manager.TableData, query *Query) map[int64][]interface{} {
	// List of expressions
	// filterExpressions := query.WhereExpressionList
	// firstExpr := filterExpressions[0]
	// fmt.Printf("%v Expression\n", firstExpr)
	variables := variablesToSelect(query.SelectExpressionsList)
	fmt.Printf("%v\n", variables)
	result := filterResults(td, variables, query.SelectExpressionsList, query.WhereExpressionList, query.WhereExpression)
	return result
}

func variablesToSelect(selectExpressions []Expression) []string {
	variables := []string{}
	for _, expression := range selectExpressions {
		variables = append(variables, expression.ExpressionColumns...)
	}
	return variables
}

func splitRecordIndex(record string) (string, int64) {
	if !strings.Contains(record, ")") {
		return "", -1
	}

	recordRes := record[strings.Index(record, ")")+1:]
	index := record[:strings.Index(record, ")")]
	inInt, _ := strconv.Atoi(index)
	return recordRes, int64(inInt)

}

func satisfiesExpression(record string, column string, filterExpression Expression, columnType manager.VariableType) bool {
	mp := map[string]interface{}{}
	if columnType == manager.IntType {
		recordInt, _ := strconv.Atoi(record)
		mp[column] = recordInt
	} else {
		mp[column] = record
	}
	result, _ := CalculateSelectExpression(filterExpression, mp)
	return fmt.Sprintf("%v", result) == "true"
}

func getIndices(column string, file *os.File, filterExpression Expression, columnType manager.VariableType) map[int64]bool {
	result := map[int64]bool{}
	reader := manager.NewRecordReader(file)
	for {
		curRecord, err, _ := reader.NextRecordBuffered()
		if err == io.EOF {
			break
		}
		record, index := splitRecordIndex(curRecord)
		if satisfiesExpression(record, column, filterExpression, columnType) {
			result[index] = true
		}
	}

	return result
}

// We need to use it as a queue
func removeIndex(s []map[int64]bool, index int) []map[int64]bool {
	return append(s[:index], s[index+1:]...)
}

func getFinalIndices(indicesArr []map[int64]bool, filterExpressions []Expression) map[int64]bool {
	for {
		if len(indicesArr) == 1 {
			return indicesArr[0]
		}

		arr1, arr2 := indicesArr[0], indicesArr[1]
		joined := utils.SetIntersection(arr1, arr2)
		indicesArr = append(indicesArr, joined)
		indicesArr = removeIndex(indicesArr, 0)
		indicesArr = removeIndex(indicesArr, 0)
	}
}

func filterNotIndexed(fs *manager.TableData, filterExpressions []Expression, selectExpressions []Expression, whereExpression string, variables []string) map[int64][]interface{} {
	result := map[int64][]interface{}{}
	fmt.Printf("%v\n", filterExpressions)
	indicesArr := []map[int64]bool{}

	for _, filterExpr := range filterExpressions {
		columnName := filterExpr.ExpressionColumns[0]
		f, _ := os.Open(fs.TableDirPath + columnName)
		indicesArr = append(indicesArr, getIndices(columnName, f, filterExpr, manager.IntType))
	}

	// Boolean Algebra needed
	joined := getFinalIndices(indicesArr, filterExpressions)
	fmt.Printf("%v joined\n", joined)
	for i, variable := range variables {
		f, _ := os.Open(fs.TableDirPath + variable)
		getResultSet(f, joined, result, selectExpressions[i])
	}

	return result
}

func getResultSet(file *os.File, indices map[int64]bool, result map[int64][]interface{}, expr Expression) {
	reader := manager.NewRecordReader(file)
	for {
		curRecord, err, _ := reader.NextRecordBuffered()
		if err == io.EOF {
			break
		}
		record, index := splitRecordIndex(curRecord)
		if indices[index] == true {
			if len(result[index]) == 0 {
				result[index] = make([]interface{}, 0)
			}

			// NEEDS CHANGE
			if expr.ExpressionColumns[0] == expr.Fullexpression {
				result[index] = append(result[index], record)
			} else {
				mp := map[string]interface{}{}
				inInt, _ := strconv.Atoi(record)
				mp[expr.ExpressionColumns[0]] = inInt
				recordRes, _ := CalculateSelectExpression(expr, mp)
				result[index] = append(result[index], recordRes)
			}
		}
	}

}

func filterResults(fs *manager.TableData, variables []string, selectExpressions []Expression, filterExpressions []Expression, whereExpression string) map[int64][]interface{} {

	filteredResult := filterNotIndexed(fs, filterExpressions, selectExpressions, whereExpression, variables)
	fmt.Printf("%v\n", filteredResult)
	return filteredResult

	// dirpath, _ := os.Getwd()
	// tableName := fs.TableName

	// files := make([]*os.File, len(fs.Columns))
	// readers := make([]*bufio.Reader, len(fs.Columns))

	// for i, column := range fs.Columns {
	// 	files[i], _ = os.Open(dirpath + "/data/" + filterExpression.ExpressionColumns[0] + "-Indexed/" + column.ColumnName)
	// 	readers[i] = bufio.NewReader(files[i])
	// }

	// for {
	// 	records := manager.NextRecords(readers)
	// 	if records[0] == "" {
	// 		break
	// 	}
	// 	mp := createRowMap(records, fs)
	// 	// fmt.Printf("mp %v\n", mp)

	// 	result, _ := CalculateSelectExpression(filterExpression, mp)
	// 	converted := fmt.Sprintf("%v", result)
	// 	row := []interface{}{}
	// 	if converted == "true" {
	// 		for _, selectExpression := range selectExpressions {
	// 			result, _ := CalculateSelectExpression(selectExpression, mp)
	// 			row = append(row, result)
	// 		}
	// 		// for _, variable := range variables {
	// 		// 	row = append(row, mp[variable])
	// 		// }
	// 		resultSlice = append(resultSlice, row)
	// 		fmt.Printf("%v \n", row)
	// 	}

	// }
}

// Ceates Column -> Value map for Result
func createRowMap(records []string, fs *manager.TableData) map[string]interface{} {
	result := map[string]interface{}{}
	for i, column := range fs.Columns {
		record := records[i][strings.Index(records[i], ")")+1:]
		// Needs revision
		if column.ColumnName == "ID" {
			inInt, _ := strconv.Atoi(record)
			result[column.ColumnName] = inInt
		} else {
			result[column.ColumnName] = record
		}
	}
	return result
}

// Needs revision
func checkIfIndexed(table string, column string) bool {
	dirpath, _ := os.Getwd()
	path := dirpath + "/data/" + table + "/" + column + "-Indexed"
	fmt.Printf("%v path", path)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false
	}
	return true
}
