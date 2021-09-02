package btree

const BTREE_INDEX = 4

type Tree struct {
	Root  *TreeNode
	Order int
}

type TreeNode struct {
	M            int     // 4 bytes
	KeysPresent  int     // 4 bytes
	Keys         []int64 // 3*8 bytes
	RecordOffset []int64 // 3*8 bytes
	ChildOffset  []int64 // 4*8 bytes
}

func (*Tree) InsertValue(key, recordOffset int64) {
}

func CreateTree() {
	tree := Tree{}
	tree.Root = &TreeNode{}
}
