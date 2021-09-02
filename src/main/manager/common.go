package manager

import "fmt"

var tables []string

const TABLES_LIST_FILE_PATH = DATA_FILE_PATH + "/" + "tables.json"

func addTableToList(tableName string) {
	fmt.Println("Table added ", tableName)
}
