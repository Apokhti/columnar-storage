package main

import (
	"fmt"
	"sort"

	"cs/src/main/manager"
	"cs/src/main/server"
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
	for _, k := range keys {
		fmt.Println("Key:", k, "Value:", fs.MapOfData[k])
	}

}

func inputCSV() {
	fileName := "src/resources/BigData.csv" //getFileName()
	readCSV(fileName)
}

func getCommand() string {
	return "1"
	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print(`Choose action
	// 	1: add Csv File
	// 	2: write Query
	// 	`)
	// command, _ := reader.ReadString('\n')
	// command = command[:len(command)-1]
	// return command
}

func main() {
	// query_str := "select my_file, bla+ blu from base where myfile < d and bla> 7 and kutu < 3 order by kdkw, wudia;"
	// fmt.Printf("%s\n", query_str)
	// parser.PrintTokens(query_str)
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
	// 		inputCSV()
	// 	} else if command == "2" {
	// 		// inputQuerry()
	// 	}
	// 	break
	// }

	// manager.IndexBy("id", "data/myFile/id", fs, manager.IntType)
	server.ServeRequests()
}
