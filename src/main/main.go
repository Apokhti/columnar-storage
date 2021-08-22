package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/Apokhti/cs/src/main/manager"
	"github.com/Apokhti/cs/src/main/parser"
)

var fs manager.FileSaver

func getQuerry() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Querry: ")
	fileName, _ := reader.ReadString('\n')
	return fileName
}

func inputQuerry() {
	querry := getQuerry()
	querry = querry[:len(querry)-1]
	fmt.Println(querry)

	pts, err := parser.Parse(querry)

	if err != nil {
		fmt.Println("parser error ", err)
		return
	}

	data := manager.QuerryExecutor(&fs, &pts)

	println(data)
}

func getFileName() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("CSV FileName: ")
	fileName, _ := reader.ReadString('\n')
	return fileName
}

func readCSV(fileName string) {
	fmt.Println("reading file", fileName)

	fs.CreateStructure(fileName)
	fmt.Println("TableName: " + fs.TableName)

	for _, value := range fs.ColumnStructMassive {
		fmt.Println("ColumnStructMassive", ":", value.ColumnName)
	}

	for key, value := range fs.MapOfData {
		fmt.Println("Key:", key, "Value:", value)
	}

}

func inputCSV() {
	fileName := getFileName()
	readCSV(fileName[:len(fileName)-1])
}

func getCommand() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(`Choose action
		1: add Csv File
		2: write Querry
		`)
	command, _ := reader.ReadString('\n')
	command = command[:len(command)-1]
	return command
}

func main() {
	fs = manager.FileSaver{}
	query := "select bla from bla"
	pts, err := parser.Parse(query)

	if err != nil {
		fmt.Println("parser error ", err)
		return
	}
	println(&pts)
	data := manager.QuerryExecutor(&fs, &pts)
	println(data)
	// for {
	// 	command := getCommand()
	// 	if command == "1" {
	// 		inputCSV()
	// 	} else if command == "2" {
	// 		inputQuerry()
	// 	}

	// }
}
