package manager

import (
	"bufio"
	"fmt"
	"os"

	"github.com/Apokhti/cs/src/main/query"
)

// Returns Rows of Data. Each row contains column names and values
func QuerryExecutor(fs *FileSaver, query *query.Query) map[int]map[string]string {

	// columns := []string{}
	// for _, element := range query.ExpressionsList {
	// 	columns = append(columns, element.ExpressionColumns...)
	// }

	// data := columnGetter(fs, columns)

	for key, element := range fs.MapOfData {
		fmt.Println("Key:", key, "=>", "Element:", element)
	}

	data := make(map[int]map[string]string)
	columnIndex := colnameToColindex("myFile_email")
	index := 3
	startIndex := int64(fs.MapOfData[index][columnIndex])
	endIndex := int64(fs.MapOfData[index+1][columnIndex])
	fmt.Printf("%d %d", startIndex, endIndex)
	// println("%s", fs.MapOfData)
	// getColumnValue(2, column)
	return data
}

// Pics column value from database for specified row
// For example if row is
//     Age	Name
// 1:  21 	Dachi
// 2:  22	Vakho
// getColumnValue(fs, 2, Name) Returns -> Vakho
func getColumnValue(index int, column string) interface{} {
	// file, err := os.Open(column)
	// if err != nil {
	// 	println(err)
	// 	return nil
	// }
	// reader := bufio.NewReader(file)
	return ""
}

// TODO
func colnameToColindex(name string) int {
	return 3
}

// Returns indices of buffer for given row index
// func getIndices(index int, column string) (int, int) {

// }

//
func columnGetter(fs *FileSaver, columns []string) map[int]map[string]string {

	data := make(map[int]map[string]string)
	fmt.Printf("%d", fs.MapOfData)
	for index, _ := range fs.MapOfData {
		println("Start", index)

		row := make(map[string]string)
		for colIndex, col := range fs.ColumnStructMassive {

			if col.ReadStram == nil {
				file, err := os.Open("myFile_email")
				if err != nil {
					println(err)
					return nil
				}
				reader := bufio.NewReader(file)
				col.ReadStram = reader
				col.File = file
			}

			startInd := int64(fs.MapOfData[index][colIndex])

			var endInd int64
			if index == len(fs.MapOfData) {
				endInd = int64(col.ReadStram.Size())
			} else {
				endInd = fs.MapOfData[index+1][colIndex]
			}

			println("St:", startInd, " en:", endInd)

			println(col.File.Name())
			col.File.Seek(startInd, 0)
			b4, _ := col.ReadStram.Peek(int(startInd - endInd))
			fmt.Printf("5 bytes: %s\n", string(b4))

			row[col.ColumnName] = string(b4)

		}

		data[index] = row

	}

	return data

}
