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

func filterNotIndexed(fs *manager.TableData, filterExpressions []Expression, whereExpression string, variables []string) map[int64][]interface{} {
	result := map[int64][]interface{}{}
	fmt.Printf("%v\n", filterExpressions)
	indicesArr := make([]map[int64]bool, 10)

	for i, filterExpr := range filterExpressions {
		columnName := filterExpr.ExpressionColumns[0]
		f, _ := os.Open(fs.TableDirPath + columnName)
		indicesArr[i] = getIndices(columnName, f, filterExpr, manager.IntType)
	}

	// Boolean Algebra needed
	joined := utils.SetIntersection(indicesArr[0], indicesArr[1])
	fmt.Printf("%v joined\n", joined)
	for _, variable := range variables {
		f, _ := os.Open(fs.TableDirPath + variable)
		getResultSet(f, joined, result)
	}

	return result
}

func getResultSet(file *os.File, indices map[int64]bool, result map[int64][]interface{}) {
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
			result[index] = append(result[index], record)
		}
	}

}

func filterResults(fs *manager.TableData, variables []string, selectExpressions []Expression, filterExpressions []Expression, whereExpression string) map[int64][]interface{} {

	filteredResult := filterNotIndexed(fs, filterExpressions, whereExpression, variables)
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
