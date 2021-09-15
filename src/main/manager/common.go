package manager

import (
	"cs/src/main/utils"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

var tables []string = loadTables()

const TABLES_LIST_FILE_PATH = DATA_FILE_PATH + "/" + "tables.json"

func TableAlreadyExists(tableName string) bool {
	for _, table := range tables {
		if table == tableName {
			return true
		}
	}
	return false
}

func addTableToList(tableName string) error {
	if TableAlreadyExists(tableName) {
		return nil
	}
	fmt.Println("Table added ", tableName)
	tables = append(tables, tableName)

	file, err := os.Create(TABLES_LIST_FILE_PATH)
	if err != nil {
		return err
	}

	err = saveData(tables, file)
	return err
}

func saveData(data []string, file *os.File) error {
	storeData, err := json.Marshal(data)

	if err != nil {
		return err
	}
	file.Write(storeData)

	return nil
}

func loadTables() []string {
	var file *os.File
	var err error
	ans := make([]string, 0)

	if !utils.FileExists(TABLES_LIST_FILE_PATH) {
		file, err = utils.CreateFileRecursively(TABLES_LIST_FILE_PATH)

		// in case file does not exist store empty data first
		saveData(make([]string, 0), file)
		if err != nil {
			return ans
		}
	} else {
		file, err = os.Open(TABLES_LIST_FILE_PATH)
		if err != nil {
			return ans
		}
	}

	data, err := ioutil.ReadAll(file)
	err = json.Unmarshal(data, &ans)
	if err != nil {
		return make([]string, 0)
	}

	return ans
}
