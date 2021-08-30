package manager

import (
	"bufio"
	"fmt"
	"os"

	"github.com/Apokhti/cs/src/main/query"
)

func QuerryExecutor(fs *FileSaver, query *query.Query) map[int]map[string]string {

	columns := map[string]bool{}

	for _, element := range query.ExpressionsList {
		for _, el := range element.ExpressionColumns {
			columns[query.Table+"_"+el] = true
		}
	}

	data := getColumnsbetweenIndexes(4, 5, fs, columns)

	return data
}

// gets data from startIndex to endIndex,
// reads columns which are in columns
func getColumnsbetweenIndexes(startIndex int, endIndex int, fs *FileSaver, columns map[string]bool) map[int]map[string]string {

	data := make(map[int]map[string]string)

	for index := startIndex; index <= endIndex; index++ {

		row := make(map[string]string)
		for colIndex, col := range fs.ColumnStructMassive {

			if columns[col.ColumnName] {
				if col.ReadStram == nil {
					file, err := os.Open(col.ColumnName)
					if err != nil {
						println(err)
						return nil
					}
					reader := bufio.NewReader(file)
					col.ReadStram = reader
					col.File = file
				}

				startInd := int64(fs.MapOfData[index][colIndex])
				endInd := int64(0)
				if index == len(fs.MapOfData) {
					endInd = int64(col.ReadStram.Size())
				} else {
					endInd = fs.MapOfData[index+1][colIndex]
				}

				println(col.File.Name())
				col.File.Seek(startInd, 0)
				b4, _ := col.ReadStram.Peek(int(endInd - startInd))
				println("ColumnName:", col.ColumnName)
				fmt.Println("index:", index)
				fmt.Printf("col.ColumnName: %s\n", string(b4))
				fmt.Println("")

				row[col.ColumnName] = string(b4)
			}
		}

		data[index] = row

	}

	return data

}
