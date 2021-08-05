package manager

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type FileSaver struct {
	TableName           string
	ColumnStructMassive []ColumnStruct
	MapOfData           map[int][]int64 `json:"-"`
}

// fileName, columnName
type ColumnStruct struct {
	ColumnName string
	File       *os.File      `json:"-"`
	OutStream  *bufio.Writer `json:"-"`
	ReadStram  *bufio.Reader `json:"-"`
}

func (fs *FileSaver) CreateStructure(fileName string) error {
	// extract table name
	fs.TableName = filepath.Base(fileName[:len(fileName)-4])
	fs.MapOfData = make(map[int][]int64)

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Error openning file ", fileName, err)
		return err
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

func (fs *FileSaver) createDataBaseMap() error {

	data, err := json.Marshal(fs)
	if err != nil {
		return err
	}

	file, err := os.Create("DataBaseMap.json")
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	_, err = writer.Write(data)

	writer.Flush()
	file.Close()
	return err
}

func (fs *FileSaver) initializeColumns(columnNames []string) {

	for _, columnName := range columnNames {
		col := ColumnStruct{}
		col.creatColumnsStruct(fs.TableName + "_" + columnName)
		fs.ColumnStructMassive = append(fs.ColumnStructMassive, col)
	}
}

func (fs *FileSaver) closeAllColumnConnections() {
	for i := range fs.ColumnStructMassive {
		fs.ColumnStructMassive[i].OutStream.Flush()
		fs.ColumnStructMassive[i].File.Close()
	}
}

func (fs *FileSaver) addDataLine(lineOfData []string, index int) {

	for i, columnName := range lineOfData {
		writeInd, _ := fs.ColumnStructMassive[i].addData(columnName, index)

		fs.MapOfData[index] = append(fs.MapOfData[index], writeInd)

	}

	// TODO
	//fs.MapOfData[index] = append(fs.MapOfData[index], writeIndexes)

}

/*
//	columnName = tableName + _ + columnName
//
*/
func (columnStruct *ColumnStruct) creatColumnsStruct(columnName string) error {

	columnStruct.ColumnName = columnName
	fmt.Println(columnName)
	file, err := os.Create(columnName)
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
		// Could not obtain stat, handle error
	}
	writeInd := fi.Size() + int64(columnStruct.OutStream.Available())
	_, err = columnStruct.OutStream.WriteString(strconv.Itoa(index) + ")" + data + ",")
	return writeInd, err
}
