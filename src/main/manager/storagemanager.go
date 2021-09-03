package manager

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

const buffer_size = 40

type VariableType int64

const (
	// Int It's int
	Int VariableType = iota
	// Float is float
	Float
	// String is string
	String
)

/**
One of the most challanging problem was external sorting.
This impementation uses Merge sort to handle big data.
*/

// ListAllColumns -> returns all columns
func ListAllColumns(path string) []string {
	result := []string{}

	items, _ := ioutil.ReadDir(path)
	for _, item := range items {
		if !item.IsDir() {
			// handle file there
			result = append(result, item.Name())
		}
	}
	return result
}

// Decides if column with given name exists
func fileExists(columnPath string) bool {
	if _, err := os.Stat(columnPath); os.IsNotExist(err) {
		return false
	}
	return true
}

// Creates index by column
func IndexBy(indexName string) {
	//TODO yvela
	sortRes := sortFile(indexName, indexName+"_sorted")
	fmt.Printf("%v\n", sortRes)
}

// Sorts file by External Sorting
func sortFile(fileName string, outputName string) bool {
	if !fileExists(fileName) {
		return false
	}
	partitionFile(fileName, 2)

	return true
}

// Partition file
func partitionFile(filename string, chunkSize int) {
	f, _ := os.Open(filename)
	lastCutRecord := ""

	nBytes, nChunks := int64(0), int64(0)
	r := bufio.NewReader(f)
	buf := make([]byte, 0, buffer_size)
	for {
		n, err := r.Read(buf[:cap(buf)])
		buf = buf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		nChunks++
		nBytes += int64(len(buf))
		text := string(buf)
		lastIndex := lastCutIndex(text)
		lastCutTemp := text[lastIndex:]
		text = text[:lastIndex]
		createPartitionFile(lastCutRecord+text, filename+fmt.Sprint(nChunks))
		lastCutRecord = lastCutTemp
	}
}

// to be კოპწია
func lastCutIndex(text string) int {
	for i := len(text) - 1; i >= 0; i-- {
		if text[i] == '$' {
			return i
		}
	}
	return -1
}

// creates parition file
func createPartitionFile(text string, fileName string) {
	f, _ := os.Create(fileName)
	defer f.Close()
	f.WriteString(text)
	sortPartitionFile(fileName)
}

func sortPartitionFile(fileName string) {
	b, err := ioutil.ReadFile(fileName) // just pass the file name
	if err != nil {
		fmt.Print(err)
	}
	str := string(b) // convert content to a 'string'
	stringSlice := strings.Split(str, "$")
	fmt.Printf("-------%v\n", stringSlice)

}
