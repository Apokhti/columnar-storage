package main

import (
	"fmt"
	"os"
	"path/filepath"

	"cs/src/main/manager"
	"cs/src/main/server"
)

var fs manager.TableData

func readCSV(fileName string) {
	fmt.Println("reading file", fileName)

	// extract table name from path
	tableName := filepath.Base(fileName)
	tableName = tableName[:len(tableName)-4]

	fs.CreateStructure(tableName, fileName)
	fmt.Println("TableName: " + fs.TableName)

	for _, value := range fs.Columns {
		fmt.Println("ColumnStructMassive", ":", value.ColumnName)
	}

}

func main() {

	for ind, fileName := range os.Args {
		if ind == 0 {
			continue
		}
		fmt.Printf("arg ind: %v, value: %v\n", ind, fileName)
		readCSV(fileName)
	}

	server.ServeRequests(fs)
}
