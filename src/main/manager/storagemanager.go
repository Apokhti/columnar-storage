package manager

import (
	"bufio"
	"fmt"
	"os"

	"github.com/Apokhti/cs/src/main/query"
)

func QuerryExecutor(fs *FileSaver, query *query.Query) map[int]map[string]string {

	columns := []string{}
	for _, element := range query.ExpressionsList {
		columns = append(columns, element.ExpressionColumns...)
	}

	data := columnGetter(fs, columns)

	return data
}

func columnGetter(fs *FileSaver, columns []string) map[int]map[string]string {

	data := make(map[int]map[string]string)

	for index, _ := range fs.MapOfData {
		println("Start", index)

		row := make(map[string]string)
		for colIndex, col := range fs.ColumnStructMassive {

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

		data[index] = row

	}

	return data

}
