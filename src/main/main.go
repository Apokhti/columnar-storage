package main

import (
	"fmt"
	"path/filepath"

	"cs/src/main/manager"
	"cs/src/main/server"
)

var fs *manager.TableData = &manager.TableData{}

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

	// for ind, fileName := range os.Args {
	// 	if ind == 0 {
	// 		continue
	// 	}
	// 	fmt.Printf("arg ind: %v, value: %v\n", ind, fileName)
	// 	readCSV(fileName)
	// }
	// server.ServeRequests(fs)
	// manager.IndexBy("id", "data/BigData/"+"id", fs, manager.IntType)
	// }
	// manager.IndexBy("id", "data/myFile/"+"id", "myFile", fs, manager.IntType)
	fs, _ = manager.LoadTable("BigData")
	fmt.Printf("%v\n", fs)
	// f, _ := os.Open("data/myFile/ID")
	// reader := manager.NewRecordReader(f)
	// for {
	// 	next, err := reader.NextRecordBuffered()
	// 	// fmt.Printf("%v nnn\n", next)
	// 	if err == io.EOF {
	// 		break
	// 	}

	// }

	server.ServeRequests(fs)
}
