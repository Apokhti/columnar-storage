package main

import (
	"bufio"
	"bytes"
	"cs/src/main/btree"
	"cs/src/main/utils"
	"encoding/csv"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"
)

func test1() {
	var v btree.TreeNode = btree.TreeNode{M: 1000}
	var network bytes.Buffer // Stand-in for a network connection
	var network1 bytes.Buffer
	enc := gob.NewEncoder(&network)  // Will write to network.
	dec := gob.NewDecoder(&network1) // Will read from network.
	// Encode (send) the value.
	err := enc.Encode(v)
	if err != nil {
		log.Fatal("encode error:", err)
	}

	// HERE ARE YOUR BYTES!!!!
	fmt.Println(len(network.Bytes()))
	network1.Write(network.Bytes())

	var q btree.TreeNode
	err = dec.Decode(&q)
	if err != nil {
		log.Fatal("decode error:", err)
	}

	fmt.Println(q)
}

func testBTree1() {
	tree, err := btree.CreateTree("test-folder/ind.tst")
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	var mp map[int64]bool = make(map[int64]bool)
	var arr []int64
	var sum int64 = 0
	for i := 0; i < 100000; i++ {
		var v int64 = 0
		for mp[v] {
			v = rand.Int63n(100000000)
		}

		start := time.Now()
		err = tree.InsertValue(v, v)
		arr = append(arr, v)

		if err != nil {
			fmt.Printf("error on adding value, ind: %v pref: %v error: %v", v, v, err)
		} else {
			// fmt.Printf("value added key: %v value: %v\r\n", v, v)
		}
		mp[v] = true
		duration := time.Since(start)
		sum += duration.Microseconds()
	}

	fmt.Printf("ended inserting values averageTime: %vms\r\n", sum/1000000.0)

	var correct int = 0
	sum = 0
	for _, v := range arr {
		start := time.Now()
		val, err := tree.Get(v)
		if val != v {
			fmt.Printf("key %v, value %v, error: %v\r\n", v, val, err)
		} else {
			correct++
		}
		dur := time.Since(start)
		sum += dur.Microseconds()
	}
	fmt.Printf("correct values got: %v\r\naverage Get time: %vms\r\n", correct, sum/1000000.0)
}

func testBTree2() {
	tree, err := btree.CreateTree("test-folder/ind.tst")
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	var mp map[int64]bool = make(map[int64]bool)
	var arr []int64
	var sum int64 = 0
	for i := 0; i < 10000000; i++ {
		var v int64 = 0
		for mp[v] {
			v = rand.Int63n(100000000000)
		}

		start := time.Now()
		err = tree.InsertValue(v, v)
		arr = append(arr, v)

		if err != nil {
			fmt.Printf("error on adding value, ind: %v pref: %v error: %v", v, v, err)
		} else {
			// fmt.Printf("value added key: %v value: %v\r\n", v, v)
		}
		mp[v] = true
		duration := time.Since(start)
		sum += duration.Microseconds()
	}

	fmt.Printf("ended inserting values averageTime: %vms\r\n", sum/1000000.0)

	tree, err = btree.LoadTree("test-folder/ind.tst")
	if err != nil {
		fmt.Println("2nd test error: ", err)
		return
	}
	var correct int = 0
	sum = 0
	for _, v := range arr {
		start := time.Now()
		val, err := tree.Get(v)
		if val != v {
			fmt.Printf("key %v, value %v, error: %v\r\n", v, val, err)
		} else {
			correct++
		}
		dur := time.Since(start)
		sum += dur.Microseconds()
	}
	fmt.Printf("correct values got: %v\r\naverage Get time: %vms\r\n", correct, sum/1000000.0)
}

func loadNames() []string {
	var names []string

	file, _ := os.Open("names.txt")
	reader := bufio.NewReader(file)

	for {
		line, _, _ := reader.ReadLine()
		if line == nil {
			break
		}
		names = append(names, string(line[:len(line)-1]))
	}
	return names
}

func generateFile(numRows int, filePath string) {
	file, err := utils.CreateFileRecursively(filePath)
	if err != nil {
		fmt.Println("error here ", err)
	}
	headData := []string{
		"ID", "Name", "Email", "Age", "RandInt",
	}
	names := loadNames()
	writer := csv.NewWriter(file)
	writer.Write(headData)
	for i := 0; i < numRows; i++ {
		var newStr []string
		newStr = append(newStr, fmt.Sprintf("%v", i))
		newStr = append(newStr, randomPos(names))
		newStr = append(newStr, randomPos(names)+"@gmail.com")
		newStr = append(newStr, fmt.Sprintf("%v", rand.Int31n(100)))
		newStr = append(newStr, fmt.Sprintf("%v", rand.Int()))

		writer.Write(newStr)
	}
	writer.Flush()
	file.Close()
}

func randomPos(arr []string) string {
	return arr[rand.Intn(len(arr))]
}

func main() {
	// testBTree1()
	// testBTree2()
	generateFile(1000000, "src/resources/BigData.csv")
}