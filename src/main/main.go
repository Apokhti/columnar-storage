package main

import (
	"fmt"
	"sort"

	"cs/src/main/manager"
)

var fs manager.TableData

func readCSV(fileName string) {
	fmt.Println("reading file", fileName)

	fs.CreateStructure("myFile", fileName)
	fmt.Println("TableName: " + fs.TableName)

	for _, value := range fs.Columns {
		fmt.Println("ColumnStructMassive", ":", value.ColumnName)
	}

	keys := make([]int, 0)
	for k := range fs.MapOfData {
		keys = append(keys, k)
	}

	sort.Ints(keys)
	// for _, k := range keys {
	// 	fmt.Println("Key:", k, "Value:", fs.MapOfData[k])
	// }

}

func main() {
	// query_str := "select my_file, bla from base where myfile < d ;"
	// // fmt.Printf("%s\n", query_str)
	// // parser.PrintTokens(query_str)
	// q, _ := parser.Parse(query_str)
	// q.PrintQuery()
	// fmt.Printf("Expression to calculate %v\n", q.SelectExpressionsList[1])
	// mp := make(map[string]interface{}, 10)
	// mp["bla"] = 1
	// mp["blu"] = 100
	// result, _ := query.CalculateSelectExpression(q.SelectExpressionsList[1], mp)
	// fmt.Printf("Result %v\n", result)
	// fs = manager.TableData{}
	// for {
	// 	command := getCommand()
	// 	if command == "1" {
	// 	} else if command == "2" {
	// 		// inputQuerry()
	// 	}
	// 	break
	// }

	fileName := "src/resources/BigData.csv"

	// for ind, fileName := range os.Args[1:] {
	// 	fmt.Printf("arg ind: %v, value: %v\n", ind, fileName)
	readCSV(fileName)
	// }
	// server.ServeRequests(fs)
	manager.IndexBy("ID", "data/myFile/"+"ID", fs, manager.StringType)
	// f, _ := os.Open("data/myFile/ID")
	// reader := manager.NewRecordReader(f)
	// for {
	// 	next, err := reader.NextRecordBuffered()
	// 	// fmt.Printf("%v nnn\n", next)
	// 	if err == io.EOF {
	// 		break
	// 	}

	// }

}
