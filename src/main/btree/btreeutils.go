package btree

import (
	"io"
	"os"
)

const NODE_SIZE = 124

func readNodeFromFile(offset int64, file *os.File) (*TreeNode, error) {
	var err error

	_, err = file.Seek(offset, 0)

	if err != nil {
		return nil, err
	}
	var data []byte = make([]byte, NODE_SIZE)
	_, err = file.Read(data)

	if err != nil {
		return nil, err
	}

	return decodeNode(data)
}

func writeNodeToFile(node *TreeNode, file *os.File) error {
	if node.SelfOffset == -1 {
		file.Seek(0, io.SeekEnd)
		info, err := file.Stat()

		if err != nil {
			return err
		}
		node.SelfOffset = info.Size()
	} else {
		file.Seek(int64(node.SelfOffset), io.SeekStart)
	}
	b := encodeNode(node)
	_, err := file.Write(b)

	return err
}

func decodeInt(b []byte) int {
	var ans int
	ans = int(b[0]) << 24
	ans += int(b[1]) << 16
	ans += int(b[2]) << 8
	ans += int(b[3])

	return ans
}

func decodeInt64(b []byte) int64 {
	var ans int64
	ans = int64(b[0]) << 56
	ans += int64(b[1]) << 48
	ans += int64(b[2]) << 40
	ans += int64(b[3]) << 32
	ans += int64(b[4]) << 24
	ans += int64(b[5]) << 16
	ans += int64(b[6]) << 8
	ans += int64(b[7])

	return ans
}

func decodeIntArray(finalSize int, bytes []byte) []int {
	var ansArr []int
	for i := 0; i < finalSize; i++ {
		ansArr = append(ansArr, decodeInt(bytes[i*4:(i+1)*4]))
	}
	return ansArr
}

func decodeInt64Array(finalSize int, bytes []byte) []int64 {
	var ansArr []int64
	for i := 0; i < finalSize; i++ {
		ansArr = append(ansArr, decodeInt64(bytes[i*8:(i+1)*8]))
	}
	return ansArr
}

func encodeInt(val int) []byte {
	var arr []byte
	arr = append(arr, byte(val>>24))
	arr = append(arr, byte(val>>16))
	arr = append(arr, byte(val>>8))
	arr = append(arr, byte(val))

	return arr
}

func encodeInt64(val int64) []byte {
	var arr []byte
	arr = append(arr, byte(val>>56))
	arr = append(arr, byte(val>>48))
	arr = append(arr, byte(val>>40))
	arr = append(arr, byte(val>>32))
	arr = append(arr, byte(val>>24))
	arr = append(arr, byte(val>>16))
	arr = append(arr, byte(val>>8))
	arr = append(arr, byte(val))

	return arr
}

func encodeIntArray(initialSize int, arr []int) []byte {
	var ansArr []byte
	for i := 0; i < initialSize; i++ {
		ansArr = append(ansArr, encodeInt(arr[i])...)
	}

	return ansArr
}

func encodeInt64Array(initialSize int, arr []int64) []byte {
	var ansArr []byte
	for i := 0; i < initialSize; i++ {
		ansArr = append(ansArr, encodeInt64(arr[i])...)
	}

	return ansArr
}

// M            int                  // 4 bytes
// State        int                  // 4 bytes
// KeysPresent  int                  // 4 bytes
// SelfOffset   int                  // 4 bytes
// Keys         [BTREE_INDEX]int     // 4 * 4 bytes
// RecordOffset [BTREE_INDEX]int     // 4 * 4 bytes
// ChildOffset  [BTREE_INDEX + 1]int // 5 * 4 bytes

func encodeNode(node *TreeNode) []byte {
	var arr []byte
	arr = append(arr, encodeInt(node.M)...)
	arr = append(arr, encodeInt(node.State)...)
	arr = append(arr, encodeInt(node.KeysPresent)...)
	arr = append(arr, encodeInt64(node.SelfOffset)...)
	arr = append(arr, encodeInt64Array(BTREE_INDEX, node.Keys)...)
	arr = append(arr, encodeInt64Array(BTREE_INDEX, node.RecordOffset)...)
	arr = append(arr, encodeInt64Array(BTREE_INDEX+1, node.ChildOffset)...)

	return arr
}

// M            int                  // 4 bytes
// State        int                  // 4 bytes
// KeysPresent  int                  // 4 bytes
// SelfOffset   int                  // 4 bytes
// Keys         [BTREE_INDEX]int     // 4 * 4 bytes
// RecordOffset [BTREE_INDEX]int     // 4 * 4 bytes
// ChildOffset  [BTREE_INDEX + 1]int // 5 * 4 bytes

func decodeNode(bytes []byte) (*TreeNode, error) {
	var ansNode TreeNode
	ansNode = TreeNode{
		M:            decodeInt(bytes[:4]),
		State:        decodeInt(bytes[4:8]),
		KeysPresent:  decodeInt(bytes[8:12]),
		SelfOffset:   decodeInt64(bytes[12:20]),
		Keys:         decodeInt64Array(BTREE_INDEX, bytes[20:20+BTREE_INDEX*4]),
		RecordOffset: decodeInt64Array(BTREE_INDEX, bytes[20+BTREE_INDEX*8:20+BTREE_INDEX*8*2]),
		ChildOffset:  decodeInt64Array(BTREE_INDEX+1, bytes[20+BTREE_INDEX*8*2:20+BTREE_INDEX*8*2+(BTREE_INDEX+1)]),
	}
	return &ansNode, nil
}

// creates new node and appends it to node file
func createNode(file *os.File) (*TreeNode, error) {
	newNode := TreeNode{
		SelfOffset:   -1,
		Keys:         make([]int64, BTREE_INDEX),
		RecordOffset: make([]int64, BTREE_INDEX),
		ChildOffset:  make([]int64, BTREE_INDEX+1),
	}
	err := writeNodeToFile(&newNode, file)

	return &newNode, err
}

func updateRootOffset(offset int64, file *os.File) error {
	_, err := file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}
	_, err = file.Write(encodeInt64(offset))

	return err
}

func loadRootNodeFromFile(file *os.File) (*TreeNode, error) {
	var rootOffsetBytes []byte = make([]byte, 8)
	_, err := file.Read(rootOffsetBytes)
	if err != nil {
		return nil, err
	}

	return readNodeFromFile(decodeInt64(rootOffsetBytes), file)
}
