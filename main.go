package main

import (
	"bufio"
	"fmt"
	"os"
)

func getFileName() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("CSV FileName: ")
	fileName, _ := reader.ReadString('\n')
	return fileName
}

func readCSV(fileName string) {
	fmt.Println("reading file", fileName)
	fs := FileSaver{}
	fs.createStructure(fileName)
}

func main() {
	for {
		fileName := getFileName()
		readCSV(fileName[:len(fileName)-1])
	}
}
