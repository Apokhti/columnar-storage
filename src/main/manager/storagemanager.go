package manager

import (
	"bufio"
	"cs/src/main/btree"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

const buffer_size = 5
const maxRecord = 5000
const delimiter = '$'
const partitionKeyword = "-Partition-"
const dataPath = "/data/"

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
	buffer []byte
	offset int
	r      *bufio.Reader
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

// Decides if column with given name exists
func fileExists(columnPath string) bool {
	if _, err := os.Stat(columnPath); os.IsNotExist(err) {
		return false
	}
	return true
}

func makeBTree(indexColumn string, indexDirPath string, columns *[]ColumnStruct) error {
	var filesReaderMap map[string]*bufio.Reader = make(map[string]*bufio.Reader)
	var filesTreeMap map[string]*btree.Tree = make(map[string]*btree.Tree)
	var filesOffsetMap map[string]int64 = make(map[string]int64)

	for _, name := range *columns {
		file, err := os.Open(indexDirPath + "/" + name.ColumnName)
		if err != nil {
			return err
		}
		filesReaderMap[name.ColumnName] = bufio.NewReader(file)
		tree, err := btree.CreateTree(indexDirPath + "/" + name.ColumnName + ".idx")
		if err != nil {
			return err
		}
		filesTreeMap[name.ColumnName] = tree
		filesOffsetMap[name.ColumnName] = 0
	}

	for {
		record, err, newOffset := nextRecordAndOffset(filesOffsetMap[indexColumn], filesReaderMap[indexColumn])
		if err != nil {
			return err
		}
		if record == "" {
			return nil
		}

		arr := strings.Split(record, ")")
		filesOffsetMap[indexColumn] = newOffset
	}

	return nil
}

// IndexBy - Creates index by column
func IndexBy(columnName string, path string, fs TableData, columnType VariableType) {
	//TODO yvela
	fmt.Printf("%v\n", fs.Columns)

	dirpath, _ := os.Getwd()
	fmt.Printf("diiir %v\n", dirpath)

	os.MkdirAll(dirpath+dataPath+columnName+"-Indexed", os.ModePerm)
	sortFile(columnName, path, fs.Columns, columnType)
	fs.Indexes = append(fs.Indexes, IndexData{
		IndexColumnName: columnName,
		IndexDirPath:    dirpath + dataPath + columnName + "-Indexed",
	})

	makeBTree(columnName, dirpath+dataPath+columnName+"-Indexed/", &fs.Columns)

	err := fs.StoreTableMap()
	if err != nil {
		fmt.Println("error storing data table map: ", fs)
	}
}

// Sorts file by External Sorting
func sortFile(columnName string, fileName string, columns []ColumnStruct, columnType VariableType) bool {
	if !fileExists(fileName) {
		return false
	}
	partitionFilenames := partitionFile(fileName, columnType)
	fmt.Printf("%v filenames\n", partitionFilenames)
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

	mergePartitions(columnName, fileName, partitionFilenames, columns, columnType)

	return true
}

// Sort sort sort sort sort sort sort sort sort
func sortByVirtualIndices(indices []int, partitionFilename string) {
	f, _ := os.Open(partitionFilename)
	defer f.Close()
	r := NewRecordReader(f)

	result := []string{}
	for {
		curRecord, err := r.NextRecordBuffered()
		fmt.Printf("%v\n", curRecord)
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
		curRecord, err := r.NextRecordBuffered()
		if curRecord != "" {
			text += curRecord + string(delimiter)
		}
		if err == io.EOF {
			if text != "" {
				nChunks++
				paritionName := filename + partitionKeyword + fmt.Sprint(nChunks)
				paritionFilenames = append(paritionFilenames, fmt.Sprint(nChunks))
				fmt.Printf("TXT %v\n", text)

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
			fmt.Printf("TXT %v\n", text)
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
		curRecord, err := r.NextRecordBuffered()
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

// Patition merge code
func mergePartitions(columnName string, filename string, partitionFilenames []string, columns []ColumnStruct, columnType VariableType) {
	dirpath, _ := os.Getwd()
	fmt.Printf("Starting merging: %v dirpath %v\n", partitionFilenames, dirpath)

	for {
		if len(partitionFilenames) == 1 {
			for i := range columns {
				filename := columns[i].ColumnFilePath + partitionKeyword + partitionFilenames[0]
				os.Rename(filename, dirpath+dataPath+columnName+"-Indexed/"+columns[i].ColumnName)

			}
			break
		}

		mergeTwoFiles(filename, partitionFilenames[0], partitionFilenames[1], columns, columnType)
		partitionFilenames = append(partitionFilenames, partitionFilenames[0]+"-"+partitionFilenames[1])
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

func writeRecords(files []*os.File, records []string) {
	for i, record := range records {
		files[i].WriteString(record + string(delimiter))
	}
}

// Merging two files is great tool that be used in mergin K files yo
func mergeTwoFiles(filename string, partitionFirst string, partitionSecond string, columns []ColumnStruct, columnType VariableType) {

	fmt.Printf("Merging %v %v\n", partitionFirst, partitionSecond)
	f, _ := os.Create(filename + partitionKeyword + partitionFirst + "-" + partitionSecond)

	f1, _ := os.Open(filename + partitionKeyword + partitionFirst)
	f2, _ := os.Open(filename + partitionKeyword + partitionSecond)
	defer f.Close()
	defer f1.Close()
	defer f2.Close()

	r1 := NewRecordReader(f1)
	r2 := NewRecordReader(f2)

	r1End, r2End := false, false

	a, err1 := r1.NextRecordBuffered()
	b, err2 := r2.NextRecordBuffered()
	files := make([]*os.File, len(columns))

	files1 := make([]*os.File, len(columns))
	files2 := make([]*os.File, len(columns))

	readers1 := make([]*bufio.Reader, len(columns))
	readers2 := make([]*bufio.Reader, len(columns))
	for i, col := range columns {
		files[i], _ = os.Create(col.ColumnFilePath + partitionKeyword + partitionFirst + "-" + partitionSecond)
		files1[i], _ = os.Open(col.ColumnFilePath + partitionKeyword + partitionFirst)
		files2[i], _ = os.Open(col.ColumnFilePath + partitionKeyword + partitionSecond)

		readers1[i] = bufio.NewReader(files1[i])
		readers2[i] = bufio.NewReader(files2[i])
	}

	as := NextRecords(readers1)
	bs := NextRecords(readers2)

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
			bs = NextRecords(readers2)
			b, err2 = r2.NextRecordBuffered()
		} else {
			writeRecords(files, as)
			// f.WriteString(a + string(delimiter))
			as = NextRecords(readers1)
			a, err1 = r1.NextRecordBuffered()
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
			bs = NextRecords(readers2)
			b, err2 = r2.NextRecordBuffered()
		}
	}

	if r2End {
		for {
			if err1 == io.EOF && a == "" {
				break
			}
			writeRecords(files, as)
			// f.WriteString(a + string(delimiter))
			as = NextRecords(readers1)
			a, err1 = r1.NextRecordBuffered()
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
		curRecord, err := r.NextRecordBuffered()

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
	result := RecordReader{offset: 0}
	r := bufio.NewReader(f)
	result.r = r
	result.buffer = make([]byte, buffer_size)
	result.r.Read(result.buffer)
	return &result
}

//
func (rd *RecordReader) NextRecordBuffered() (string, error) {
	record := ""
	nn := -1
	for {
		if rd.offset >= len(rd.buffer) {
			n, err := rd.r.Read(rd.buffer)

			rd.buffer = rd.buffer[:n]
			nn = n
			if n == 0 {
				if err == nil {
					continue
				}
				return record, io.EOF
			}
			rd.offset = 0
		}
		if rd.offset == nn {
			return record, io.EOF
		}
		character := rd.buffer[rd.offset]
		if character == delimiter {
			rd.offset++
			break
		}
		record = record + string(character)
		rd.offset++
	}

	return record, nil
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
