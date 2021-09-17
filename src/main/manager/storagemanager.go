package manager

import (
	"bufio"
	"cs/src/main/btree"
	"cs/src/main/utils"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const buffer_size = 1000
const maxRecord = 500
const delimiter = '$'
const partitionKeyword = "-Partition-"
const dataPath = "data"
const AChar = 65

type VariableType int64

const (
	// IntType It's int
	IntType VariableType = iota
	// FloatType is float
	FloatType
	// StringType is string
	StringType
)

type RecordReader struct {
	buffer     []byte
	offset     int
	fullOffset int
	r          *bufio.Reader
	n          int
}

/**
* One of the most challanging problem was external sorting.
* This impementation uses Merge sort to handle big data.
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

func makeBTree(indexColumn string, indexDirPath string, columns *[]ColumnStruct) error {
	var filesReaderMap map[string]*RecordReader = make(map[string]*RecordReader)
	var filesTreeMap map[string]*btree.Tree = make(map[string]*btree.Tree)

	for _, column := range *columns {
		file, err := os.Open(indexDirPath + "/" + column.ColumnName)
		if err != nil {
			return err
		}
		filesReaderMap[column.ColumnName] = NewRecordReader(file)
		tree, err := btree.CreateTree(indexDirPath + "/" + column.ColumnName + ".idx")
		if err != nil {
			return err
		}
		filesTreeMap[column.ColumnName] = tree
	}

	for {
		record, err, offset := filesReaderMap[indexColumn].NextRecordBuffered()
		if err != nil && err != io.EOF {
			return err
		}
		if record == "" {
			return nil
		}
		record = extractRecord(record)
		intVal, err := strconv.ParseInt(record, 10, 64)
		if err != nil {
			return err
		}

		filesTreeMap[indexColumn].InsertValue(intVal, int64(offset))

		for _, column := range *columns {
			if column.ColumnName == indexColumn {
				continue
			}
			_, err, offset = filesReaderMap[column.ColumnName].NextRecordBuffered()

			if err != nil {
				return err
			}
			err = filesTreeMap[column.ColumnName].InsertValue(intVal, int64(offset))

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// IndexBy - Creates index by column
func IndexBy(columnName string, path string, tablename string, fs *TableData, columnType VariableType) {
	//TODO yvela

	dirpath, _ := os.Getwd()

	indexDir := dirpath + "/" + dataPath + "/" + tablename + "/" + columnName + "-Indexed"
	os.MkdirAll(indexDir, os.ModePerm)
	sortFile(columnName, path, tablename, fs.Columns, columnType)
	fs.Indexes = append(fs.Indexes, IndexData{
		IndexColumnName: columnName,
		IndexDirPath:    indexDir,
	})

	makeBTree(columnName, indexDir, &fs.Columns)

	err := fs.StoreTableMap()
	if err != nil {
		fmt.Println("error storing data table map: ", fs)
	}
}

// Sorts file by External Sorting
func sortFile(columnName string, fileName string, tablename string, columns []ColumnStruct, columnType VariableType) bool {
	if !utils.FileExists(fileName) {
		return false
	}
	partitionFilenames := partitionFile(fileName, columnType)
	for _, column := range columns {
		partitionFile(column.ColumnFilePath, columnType)
	}

	for _, partitionName := range partitionFilenames {
		fullPartitionName := fileName + partitionKeyword + partitionName
		sortPartitionFile(fullPartitionName, columnType)
		indices := getVirtualIndices(fullPartitionName)
		for i := range columns {
			sortByVirtualIndices(indices, columns[i].ColumnFilePath+partitionKeyword+partitionName)
		}
		break
	}

	mergePartitions(columnName, fileName, tablename, partitionFilenames, columns, columnType)

	return true
}

// Sort sort sort sort sort sort sort sort sort
func sortByVirtualIndices(indices []int, partitionFilename string) {
	f, _ := os.Open(partitionFilename)
	defer f.Close()
	r := NewRecordReader(f)

	result := []string{}
	for {
		curRecord, err, _ := r.NextRecordBuffered()
		if err == io.EOF && curRecord == "" {
			break
		}
		result = append(result, curRecord)
	}

	mp := mapOfIndices(indices)

	final := make([]string, maxRecord)
	for _, record := range result {
		index := record[:strings.Index(record, ")")]
		i, _ := strconv.Atoi(index)
		realInd := mp[i]
		final[realInd] = record
	}

	f1, _ := os.OpenFile(partitionFilename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer f1.Close()
	for _, record := range final {
		if record != "" {
			f1.WriteString(record + string(delimiter))
		}
	}
}

// Creates map of indices.
// Which record goes where according to sort
func mapOfIndices(indices []int) map[int]int {
	result := make(map[int]int)
	for i, val := range indices {
		result[val] = i
	}
	return result
}

// Partition file
func partitionFile(filename string, columnType VariableType) []string {
	f, _ := os.Open(filename)
	defer f.Close()
	text := ""
	curNumRecords := 0
	nChunks := int64(0)
	r := NewRecordReader(f)
	paritionFilenames := []string{}
	for {
		curRecord, err, _ := r.NextRecordBuffered()
		if curRecord != "" {
			text += curRecord + string(delimiter)
		}
		if err == io.EOF {
			if text != "" {
				nChunks++
				paritionName := filename + partitionKeyword + fmt.Sprint(nChunks)
				paritionFilenames = append(paritionFilenames, fmt.Sprint(nChunks))

				createPartitionFile(text, paritionName)
			}
			break
		}

		curNumRecords++
		if curNumRecords == maxRecord {
			nChunks++
			paritionName := filename + partitionKeyword + fmt.Sprint(nChunks)
			paritionFilenames = append(paritionFilenames, fmt.Sprint(nChunks))
			createPartitionFile(text, paritionName)
			text, curNumRecords = "", 0
		}
	}
	return paritionFilenames
}

// Returns virtual indices sequence
func getVirtualIndices(filename string) []int {
	fmt.Printf("filename %v\n", filename)
	result := []int{}
	f, _ := os.Open(filename)
	defer f.Close()
	r := NewRecordReader(f)

	for {
		curRecord, err, _ := r.NextRecordBuffered()
		if err == io.EOF && curRecord == "" {
			break
		}
		index := curRecord[:strings.Index(curRecord, ")")]
		i, err := strconv.Atoi(index)
		result = append(result, i)
	}
	return result
}

// We need to use it as a queue
func removeIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

// Remove unnecessary middle partition files
func removePartitions(columns []ColumnStruct, partitionName string) {
	for i := range columns {
		filename := columns[i].ColumnFilePath + partitionKeyword + partitionName
		os.Remove(filename)
	}
}

func generateNewFilename(stringArr []byte) ([]byte, string) {
	curArr := stringArr
	result := ""
	for i, char := range curArr {
		if char == 0 {
			curArr[i] = AChar
			break
		}
		curArr[i]++
		if curArr[i] == AChar+25 {
			for j := 0; j <= i; j++ {
				curArr[j] = AChar
			}
		} else {
			break
		}

	}
	for _, char := range curArr {
		if char == 0 {
			break
		}
		result += string(char)
	}
	return curArr, result
}

// Patition merge code
func mergePartitions(columnName string, filename string, tablename string, partitionFilenames []string, columns []ColumnStruct, columnType VariableType) {
	dirpath, _ := os.Getwd()
	fmt.Printf("Starting merging: %v dirpath %v\n", partitionFilenames, dirpath)

	newFilename := ""
	stringArr := make([]byte, 10)
	for {
		if len(partitionFilenames) == 1 {
			for i := range columns {
				filename := columns[i].ColumnFilePath + partitionKeyword + partitionFilenames[0]
				os.Rename(filename, dirpath+"/"+dataPath+"/"+tablename+"/"+columnName+"-Indexed/"+columns[i].ColumnName)
			}
			break
		}

		stringArr, newFilename = generateNewFilename(stringArr)
		mergeTwoFiles(filename, partitionFilenames[0], partitionFilenames[1], newFilename, columns, columnType)
		partitionFilenames = append(partitionFilenames, newFilename)
		removePartitions(columns, partitionFilenames[0])
		removePartitions(columns, partitionFilenames[1])
		partitionFilenames = removeIndex(partitionFilenames, 0)
		partitionFilenames = removeIndex(partitionFilenames, 0)
	}

}

// Returns next records for all paritions
func NextRecords(readers []*bufio.Reader) []string {
	result := make([]string, len(readers))
	for i, r := range readers {
		result[i], _ = NextRecord(r)
	}
	// fmt.Printf("%v next\n", result)
	return result
}

func NextRecordsBuffered(readers []*RecordReader) []string {
	result := make([]string, len(readers))
	for i, r := range readers {
		result[i], _, _ = r.NextRecordBuffered()
	}
	return result
}

func writeRecords(files []*os.File, records []string) {
	for i, record := range records {
		files[i].WriteString(record + string(delimiter))
	}
}

// Merging two files is great tool that be used in mergin K files yo
func mergeTwoFiles(filename string, partitionFirst string, partitionSecond string, newFilename string, columns []ColumnStruct, columnType VariableType) {

	fmt.Printf("Merging %v %v to %v \n", partitionFirst, partitionSecond, newFilename)
	f, _ := os.Create(filename + partitionKeyword + newFilename)

	f1, _ := os.Open(filename + partitionKeyword + partitionFirst)
	f2, _ := os.Open(filename + partitionKeyword + partitionSecond)
	defer f.Close()
	defer f1.Close()
	defer f2.Close()

	r1 := NewRecordReader(f1)
	r2 := NewRecordReader(f2)

	r1End, r2End := false, false

	a, err1, _ := r1.NextRecordBuffered()
	b, err2, _ := r2.NextRecordBuffered()
	files := make([]*os.File, len(columns))

	files1 := make([]*os.File, len(columns))
	files2 := make([]*os.File, len(columns))

	readers1 := make([]*RecordReader, len(columns))
	readers2 := make([]*RecordReader, len(columns))
	for i, col := range columns {
		files[i], _ = os.Create(col.ColumnFilePath + partitionKeyword + newFilename)
		files1[i], _ = os.Open(col.ColumnFilePath + partitionKeyword + partitionFirst)
		files2[i], _ = os.Open(col.ColumnFilePath + partitionKeyword + partitionSecond)

		readers1[i] = NewRecordReader(files1[i])
		readers2[i] = NewRecordReader(files2[i])
	}

	as := NextRecordsBuffered(readers1)
	bs := NextRecordsBuffered(readers2)

	for {

		// fmt.Printf("%v %v\n", a, b)

		if err1 == io.EOF && a == "" {
			r1End = true
			break
		}
		if err2 == io.EOF && b == "" {
			r2End = true
			break
		}

		if cmp(a, b, columnType) > 0 {
			writeRecords(files, bs)
			// f.WriteString(b + string(delimiter))
			bs = NextRecordsBuffered(readers2)
			b, err2, _ = r2.NextRecordBuffered()
		} else {
			writeRecords(files, as)
			// f.WriteString(a + string(delimiter))
			as = NextRecordsBuffered(readers1)
			a, err1, _ = r1.NextRecordBuffered()
		}

	}
	// fmt.Printf("%v %v\n", r1End, r2End)

	if r1End {
		for {
			if err2 == io.EOF && b == "" {
				break
			}
			writeRecords(files, bs)
			// f.WriteString(b + string(delimiter))
			bs = NextRecordsBuffered(readers2)
			b, err2, _ = r2.NextRecordBuffered()
		}
	}

	if r2End {
		for {
			if err1 == io.EOF && a == "" {
				break
			}
			writeRecords(files, as)
			// f.WriteString(a + string(delimiter))
			as = NextRecordsBuffered(readers1)
			a, err1, _ = r1.NextRecordBuffered()
		}
	}
}

// creates parition file
func createPartitionFile(text string, fileName string) {
	f, _ := os.Create(fileName)
	defer f.Close()
	f.WriteString(text)
}

// Sorts Parition File in RAM!
func sortPartitionFile(fileName string, columnType VariableType) {
	// fmt.Printf("Sorting")
	records := []string{}
	f, _ := os.Open(fileName)
	r := NewRecordReader(f)
	defer f.Close()
	for {
		curRecord, err, _ := r.NextRecordBuffered()

		if err == io.EOF && curRecord == "" {
			break
		} else if curRecord == string(delimiter) || curRecord == "" {
			continue
		}
		records = append(records, curRecord)
	}
	sorted := sortInMemory(records, columnType)
	// fmt.Printf("%v\n", sorted)

	f1, _ := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer f1.Close()
	for _, record := range sorted {
		f1.WriteString(record + string(delimiter))
	}

	// indices := getVirtualIndices(fileName)
	// fmt.Printf("%v indices", indices)
}

// Sorts in RAM
func sortInMemory(records []string, columnType VariableType) []string {
	sorted := heapSort(records, columnType)
	return sorted
}

// HEAP SORT algorithm is copied from
// https://www.tutorialdocs.com/article/golang-sort-algorithms.html
// Please, this is not plagiarism
func heapSort(data []string, columnType VariableType) []string {
	heapify(data, columnType)
	for i := len(data) - 1; i > 0; i-- {
		data[0], data[i] = data[i], data[0]
		siftDown(data, columnType, 0, i)
	}
	return data
}

func heapify(data []string, columnType VariableType) {
	for i := (len(data) - 1) / 2; i >= 0; i-- {
		siftDown(data, columnType, i, len(data))
	}
}

func siftDown(heap []string, columnType VariableType, lo, hi int) {
	root := lo
	for {
		child := root*2 + 1
		if child >= hi {
			break
		}
		if child+1 < hi && cmp(heap[child], heap[child+1], columnType) < 0 {
			child++
		}
		if cmp(heap[root], heap[child], columnType) < 0 {
			heap[root], heap[child] = heap[child], heap[root]
			root = child
		} else {
			break
		}
	}
}

/*
	Return 0, if a == b.
	Return 1, if a > b.
	Return -1, if a < b.
*/
func cmp(a string, b string, columnType VariableType) int {
	a = a[strings.Index(a, ")")+1:]
	b = b[strings.Index(b, ")")+1:]

	if columnType == IntType {
		aInt, _ := strconv.Atoi(a)
		bInt, _ := strconv.Atoi(b)
		if aInt > bInt {
			return 1
		} else if aInt < bInt {
			return -1
		}
		return 0
	} else if columnType == StringType {
		return strings.Compare(a, b)
	} else if columnType == FloatType {
		aFloat, _ := strconv.ParseFloat(a, 64)
		bFloat, _ := strconv.ParseFloat(b, 64)
		if aFloat > bFloat {
			return 1
		} else if aFloat < bFloat {
			return -1
		}
		return 0
	}
	return 0
}

// Return next record in file if exists
func NextRecord(r *bufio.Reader) (string, error) {
	record := ""
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			return record, err
		} else if b == delimiter {
			break
		}
		record = record + string(b)
	}
	return record, nil
}

func NewRecordReader(f *os.File) *RecordReader {
	result := RecordReader{offset: 0, fullOffset: 0}
	r := bufio.NewReader(f)
	result.r = r
	result.buffer = make([]byte, buffer_size)
	result.n, _ = result.r.Read(result.buffer)
	return &result
}

//
func (rd *RecordReader) NextRecordBuffered() (string, error, int) {
	record := ""
	resOffset := rd.fullOffset
	nn := rd.n
	for {
		if rd.offset >= len(rd.buffer) {
			n, _ := rd.r.Read(rd.buffer[:cap(rd.buffer)])

			rd.buffer = rd.buffer[:n]
			nn = n

			if n == 0 {
				return record, io.EOF, resOffset
			}
			rd.offset = 0
		}
		if rd.offset == nn {
			return record, io.EOF, resOffset
		}

		character := rd.buffer[rd.offset]
		if character == delimiter {
			rd.offset++
			rd.fullOffset++
			break
		}
		record = record + string(character)
		rd.offset++
		rd.fullOffset++
	}
	return record, nil, resOffset
}

func nextRecordAndOffset(offset int64, r *bufio.Reader) (string, error, int64) {
	record := ""
	for {
		b, err := r.ReadByte()
		if err == io.EOF {
			return record, err, -1
		} else if b == delimiter {
			break
		}
		offset++
		record = record + string(b)
	}
	return record, nil, offset
}
