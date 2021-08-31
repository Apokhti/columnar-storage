package manager

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
)

const DATA_FILE_PATH = "data/"

type TableData struct {
	TableName     string
	TableDirPath  string
	Columns       []ColumnStruct
	TableSpaceDir string          `json:"-"`
	MapOfData     map[int][]int64 `json:"-"`
}

// fileName, columnName
type ColumnStruct struct {
	ColumnName     string
	ColumnFilePath string
	Type           string
	File           *os.File      `json:"-"`
	OutStream      *bufio.Writer `json:"-"`
	ReadStram      *bufio.Reader `json:"-"`
}

func (fs *TableData) CreateStructure(tableName, fileName string) error {
	// extract table name
	fs.TableName = tableName
	fs.MapOfData = make(map[int][]int64)

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error openning file ", fileName, err)
		return err
	}

	fs.TableSpaceDir = DATA_FILE_PATH + "/" + fs.TableName
	err = os.MkdirAll(fs.TableSpaceDir, os.ModePerm)
	if err != nil {
		fmt.Printf("Can not create Table space %v", fs.TableName)
	}

	csvReader := csv.NewReader(file)
	if csvReader == nil {
		fmt.Println("Cant open CSV file")
		return errors.New("Can't open CSV file")
	}

	for lineNum := 0; ; lineNum++ {
		line, err := csvReader.Read()
		if err != nil {
			break
		}

		if lineNum == 0 {
			fs.initializeColumns(line)
		} else {
			fs.addDataLine(line, lineNum)
		}
	}

	fs.createDataBaseMap()
	fs.closeAllColumnConnections()

	return nil
}

func (fs *TableData) createDataBaseMap() error {

	data, err := json.Marshal(fs)
	if err != nil {
		return err
	}

	file, err := os.Create(DATA_FILE_PATH + "/" + fs.TableName + "/DataBaseMap.json")
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	_, err = writer.Write(data)

	writer.Flush()
	file.Close()
	return err
}

func (fs *TableData) initializeColumns(columnNames []string) {

	for _, columnName := range columnNames {
		col := ColumnStruct{}
		col.creatColumnsStruct(DATA_FILE_PATH+"/"+fs.TableName+"/"+columnName, fs.TableName+"_"+columnName)
		fs.Columns = append(fs.Columns, col)
	}
}

func (fs *TableData) closeAllColumnConnections() {
	for i := range fs.Columns {
		fs.Columns[i].OutStream.Flush()
		fs.Columns[i].File.Close()
	}
}

func (fs *TableData) addDataLine(lineOfData []string, index int) {

	for i, columnName := range lineOfData {
		writeInd, _ := fs.Columns[i].addData(columnName, index)

		fs.MapOfData[index] = append(fs.MapOfData[index], writeInd)

	}

	// TODO
	//fs.MapOfData[index] = append(fs.MapOfData[index], writeIndexes)

}

/*
//	columnName = tableName + _ + columnName
//
*/
func (columnStruct *ColumnStruct) creatColumnsStruct(columnSaveFilePath string, columnName string) error {

	columnStruct.ColumnName = columnName
	fmt.Println(columnName)
	file, err := os.Create(columnSaveFilePath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	columnStruct.OutStream = writer
	columnStruct.File = file
	return nil
}

func (columnStruct *ColumnStruct) addData(data string, index int) (int64, error) {
	fi, err := columnStruct.File.Stat()
	if err != nil {
		return -1, err
	}
	writeInd := fi.Size() + int64(columnStruct.OutStream.Buffered())
	_, err = columnStruct.OutStream.WriteString(strconv.Itoa(index) + ")" + data + ",")
	return writeInd, err
}
