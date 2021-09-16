package manager

import (
	"bufio"
	"cs/src/main/btree"
	"cs/src/main/utils"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
)

const DATA_FILE_PATH = "data/"

const TABLE_INFO_FILE_NAME = "TableInfo.json"

type SingleValue struct {
	VirtualId int64
	Value     interface{}
}

type TableData struct {
	TableName    string
	TableDirPath string
	Indexes      []IndexData
	Columns      []ColumnStruct
}

type IndexData struct {
	IndexDirPath    string
	IndexColumnName string
}

// fileName, columnName
type ColumnStruct struct {
	ColumnName     string
	ColumnFilePath string
	Type           string
	File           *os.File      `json:"-"`
	Tree           *btree.Tree   `json:"-"`
	OutStream      *bufio.Writer `json:"-"`
}

// Creates CSV structure
func (fs *TableData) CreateStructure(tableName, filePath string) error {
	fs.TableName = tableName

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("Error openning file ", filePath, err)
		return err
	}

	fs.TableDirPath = DATA_FILE_PATH + "/" + fs.TableName + "/"

	csvReader := csv.NewReader(file)
	if csvReader == nil {
		fmt.Println("Cant open CSV file")
		return errors.New("Can't open CSV file")
	}

	for lineNum := 0; ; lineNum++ {
		var err error = nil
		line, err := csvReader.Read()
		if err != nil {
			break
		}

		if lineNum == 0 {
			err = fs.initializeColumns(line)
		} else {
			err = fs.addDataLine(line, lineNum)
		}

		if err != nil {
			return err
		}
	}

	fs.StoreTableMap()
	fs.closeAllColumnConnections()
	addTableToList(fs.TableName)

	return nil
}

func (fs *TableData) StoreTableMap() error {

	data, err := json.Marshal(fs)
	if err != nil {
		return err
	}

	file, err := utils.CreateFileRecursively(DATA_FILE_PATH + "/" + fs.TableName + "/" + TABLE_INFO_FILE_NAME)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	_, err = writer.Write(data)

	writer.Flush()
	file.Close()
	return err
}

func (fs *TableData) initializeColumns(columnNames []string) error {

	for _, columnName := range columnNames {
		col := ColumnStruct{}
		err := col.createColumnsStruct(fs.TableDirPath+columnName, columnName)
		if err != nil {
			return err
		}
		fs.Columns = append(fs.Columns, col)
	}
	return nil
}

func (fs *TableData) closeAllColumnConnections() {
	for i := range fs.Columns {
		fs.Columns[i].OutStream.Flush()
		fs.Columns[i].File.Close()
	}
}

func (fs *TableData) addDataLine(lineOfData []string, index int) error {

	for i, columnName := range lineOfData {
		_, err := fs.Columns[i].addData(columnName, index)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (fs *TableData) loadColumnTrees() error {
	for i := 0; i < len(fs.Columns); i++ {
		err := fs.Columns[i].loadTree()
		if err != nil {
			return err
		}
	}
	return nil
}

/*
//	columnName = tableName + _ + columnName
//
*/
func (columnStruct *ColumnStruct) createColumnsStruct(columnSaveFilePath string, columnName string) error {

	columnStruct.ColumnName = columnName
	columnStruct.ColumnFilePath = columnSaveFilePath
	fmt.Println(columnName)
	file, err := utils.CreateFileRecursively(columnSaveFilePath)
	if err != nil {
		return err
	}
	tree, err := btree.CreateTree(columnSaveFilePath + ".idx")
	if err != nil {
		return err
	}
	columnStruct.Tree = tree
	writer := bufio.NewWriter(file)
	columnStruct.OutStream = writer
	columnStruct.File = file
	return nil
}

func (columnStruct *ColumnStruct) loadTree() error {
	tree, err := btree.LoadTree(columnStruct.ColumnFilePath + ".idx")
	if err != nil {
		return err
	}

	columnStruct.Tree = tree
	return nil
}

// Saves data at given index.
// Returns index at file and error writing file.
func (columnStruct *ColumnStruct) addData(data string, index int) (int64, error) {
	fi, err := columnStruct.File.Stat()
	if err != nil {
		return -1, err
	}
	writeInd := fi.Size() + int64(columnStruct.OutStream.Buffered())
	_, err = columnStruct.OutStream.WriteString(fmt.Sprintf("%v)", index))

	_, err = columnStruct.OutStream.WriteString(data)
	// Writing delimiter
	columnStruct.OutStream.WriteByte('$')

	err = columnStruct.Tree.InsertValue(int64(index), writeInd)
	return writeInd, err
}

func LoadTable(tableName string) (*TableData, error) {
	var table TableData
	var err error

	if !TableAlreadyExists(tableName) {
		return nil, fmt.Errorf("Table does not exist!")
	}
	tableInfoPath := DATA_FILE_PATH + "/" + tableName + "/" + TABLE_INFO_FILE_NAME

	infoFile, err := os.Open(tableInfoPath)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(infoFile)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &table)
	if err != nil {
		return nil, err
	}

	table.loadColumnTrees()
	return &table, nil
}
