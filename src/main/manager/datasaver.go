package manager

import (
	"bufio"
	"cs/src/main/btree"
	"cs/src/main/utils"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

const DATA_FILE_PATH = "data/"

type SingleValue struct {
	VirtualId int64
	Value     interface{}
}

type TableData struct {
	TableName    string
	TableDirPath string
	Indexes      []IndexData
	Columns      []ColumnStruct
	MapOfData    map[int][]int64 `json:"-"`
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
	ReadStram      *bufio.Reader `json:"-"`
}

// Creates CSV structure
func (fs *TableData) CreateStructure(tableName, filePath string) error {
	fs.TableName = tableName
	fs.MapOfData = make(map[int][]int64)

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

	file, err := utils.CreateFileRecursively(DATA_FILE_PATH + "/" + fs.TableName + "/DataBaseMap.json")
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
		writeInd, err := fs.Columns[i].addData(columnName, index)
		if err != nil {
			return nil
		}

		fs.MapOfData[index] = append(fs.MapOfData[index], writeInd)

	}

	// TODO
	//fs.MapOfData[index] = append(fs.MapOfData[index], writeIndexes)

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
