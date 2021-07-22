package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

type FileSaver struct {
	TableName           string
	ColumnStructMassive []ColumnStruct
}

// fileName, columnName
type ColumnStruct struct {
	ColumnName string
	File       *os.File
	OutStream  *bufio.Writer
}

func (fs *FileSaver) createStructure(fileName string) error {
	// extract table name
	fs.TableName = fileName[:len(fileName)-4]

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
			fs.addDataLine(line)
		}
	}

	fs.closeAllColumnConnections()

	return nil
}

func (fs *FileSaver) initializeColumns(columnNames []string) {

	for _, columnName := range columnNames {
		col := ColumnStruct{}
		col.creatColumnsStruct(columnName)
		fs.ColumnStructMassive = append(fs.ColumnStructMassive, col)
	}
}

func (fs *FileSaver) closeAllColumnConnections() {
	for i := range fs.ColumnStructMassive {
		fs.ColumnStructMassive[i].OutStream.Flush()
		fs.ColumnStructMassive[i].File.Close()
	}
}

func (fs *FileSaver) addDataLine(lineOfData []string) {

	for i, columnName := range lineOfData {
		fs.ColumnStructMassive[i].addData(columnName)
	}
}

/*
//	columnName = tableName + _ + columnName
//
*/
func (columnStruct *ColumnStruct) creatColumnsStruct(columnName string) error {

	columnStruct.ColumnName = columnName
	file, err := os.Create(columnName)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(file)
	columnStruct.OutStream = writer
	columnStruct.File = file
	return nil
}

func (columnStruct *ColumnStruct) addData(data string) error {
	_, err := columnStruct.OutStream.WriteString(data + ",")
	return err
}
