package btree

import (
	"cs/src/main/utils"
	"os"
)

const BTREE_INDEX = 4

const (
	LEAF int = iota
	INTERNAL_NODE
)

type Tree struct {
	Root      *TreeNode
	Order     int
	StoreFile *os.File
}

type TreeNode struct {
	M            int   // 4 bytes
	State        int   // 4 bytes
	KeysPresent  int   // 4 bytes
	SelfOffset   int   // 4 bytes
	Keys         []int // 4 * 4 bytes
	RecordOffset []int // 4 * 4 bytes
	ChildOffset  []int // 5 * 4 bytes
}

// inserting single value in array of keys
// in the node
func (node *TreeNode) insertValue(key, recordOffset int) {
	var inserted bool = false
	for i := node.KeysPresent - 1; i > -1; i-- {
		if node.Keys[i] > key {
			node.Keys[i+1] = node.Keys[i]
			node.RecordOffset[i+1] = node.RecordOffset[i]
		} else {
			inserted = true
			node.Keys[i+1] = key
			node.RecordOffset[i+1] = recordOffset
			break
		}
	}
	if !inserted {
		node.Keys[0] = key
		node.RecordOffset[0] = recordOffset
	}
	node.KeysPresent++
}

func (node *TreeNode) splitChild(index int, child *TreeNode, nodesToUpdate map[int]*TreeNode, nodesFile *os.File) error {
	newNode, err := createNode(nodesFile)
	if err != nil {
		return err
	}

	// moving all records to the rigth
	node.ChildOffset[node.KeysPresent+1] = node.ChildOffset[node.KeysPresent]
	for i := node.KeysPresent; i > index; i-- {
		node.Keys[i] = node.Keys[i-1]
		node.RecordOffset[i] = node.RecordOffset[i-1]
		node.ChildOffset[i] = node.ChildOffset[i-1]
	}

	newNode.KeysPresent = 1
	newNode.State = child.State
	newNode.M = child.M
	newNode.Keys[0] = child.Keys[child.M-1]
	newNode.RecordOffset[0] = child.RecordOffset[child.M-1]
	newNode.ChildOffset[0] = child.ChildOffset[child.M-1]
	newNode.ChildOffset[1] = child.ChildOffset[child.M]

	node.Keys[index] = child.Keys[child.M-2]
	node.RecordOffset[index] = child.RecordOffset[child.M-2]
	node.ChildOffset[index+1] = newNode.SelfOffset
	node.KeysPresent++

	child.KeysPresent -= 2

	nodesToUpdate[child.SelfOffset] = child
	nodesToUpdate[node.SelfOffset] = node
	nodesToUpdate[newNode.SelfOffset] = newNode

	return nil
}

/// aq internals gulisxmob?
func (node *TreeNode) internalInsert(key, recordOffset int, nodesToUpdate map[int]*TreeNode, nodeFile *os.File) (*TreeNode, bool) {
	if node.State == LEAF {
		node.insertValue(key, recordOffset)
		nodesToUpdate[node.SelfOffset] = node
		return node, node.KeysPresent == node.M
	}

	var travelled bool = false
	for i := 0; i < node.KeysPresent; i++ {
		if key < node.Keys[i] {
			nextNode, _ := readNodeFromFile(node.ChildOffset[i], nodeFile)
			nextNode, limitReached := nextNode.internalInsert(key, recordOffset, nodesToUpdate, nodeFile)

			if limitReached {
				err := node.splitChild(i, nextNode, nodesToUpdate, nodeFile)
				if err != nil {
					return nil, false
				}
			}
			travelled = true
			break
		}
	}

	if !travelled {
		nextNode, _ := readNodeFromFile(node.ChildOffset[node.KeysPresent], nodeFile)
		nextNode, limitReached := nextNode.internalInsert(key, recordOffset, nodesToUpdate, nodeFile)

		if limitReached {
			err := node.splitChild(node.KeysPresent, nextNode, nodesToUpdate, nodeFile)
			if err != nil {
				return nil, false
			}
		}
		travelled = true
	}
	return node, node.KeysPresent == node.M
}

func (tree *Tree) updateRoot(changeMap map[int]*TreeNode) error {
	oldRoot := tree.Root
	newRootNode, err := createNode(tree.StoreFile)
	if err != nil {
		return err
	}
	newChildNode, err := createNode(tree.StoreFile)
	if err != nil {
		return err
	}

	// root node redefinition
	newRootNode.KeysPresent = 1
	newRootNode.State = INTERNAL_NODE
	newRootNode.M = oldRoot.M
	newRootNode.Keys[0] = oldRoot.Keys[oldRoot.M-2]
	newRootNode.RecordOffset[0] = oldRoot.RecordOffset[oldRoot.M-2]
	newRootNode.ChildOffset[0] = oldRoot.SelfOffset
	newRootNode.ChildOffset[1] = newChildNode.SelfOffset

	// child node initialization
	newChildNode.KeysPresent = 1
	newChildNode.State = oldRoot.State
	newChildNode.M = oldRoot.M
	newChildNode.Keys[0] = oldRoot.Keys[oldRoot.M-1]
	newChildNode.RecordOffset[0] = oldRoot.RecordOffset[oldRoot.M-1]
	newChildNode.ChildOffset[0] = oldRoot.ChildOffset[oldRoot.M-1]
	newChildNode.ChildOffset[1] = oldRoot.ChildOffset[oldRoot.M]

	// update existing node
	oldRoot.KeysPresent -= 2

	changeMap[oldRoot.SelfOffset] = oldRoot
	changeMap[newRootNode.SelfOffset] = newRootNode
	changeMap[newChildNode.SelfOffset] = newChildNode

	tree.Root = newRootNode
	err = updateRootOffset(tree.Root.SelfOffset, tree.StoreFile)

	return err
}

func (tree *Tree) InsertValue(key, recordOffset int) error {
	changeMap := make(map[int]*TreeNode)
	_, split := tree.Root.internalInsert(key, recordOffset, changeMap, tree.StoreFile)

	if split {
		err := tree.updateRoot(changeMap)
		if err != nil {
			return err
		}
	}

	for _, v := range changeMap {
		err := writeNodeToFile(v, tree.StoreFile)

		if err != nil {
			return err
		}
	}

	return nil
}

func (node *TreeNode) getInternal(key int, file *os.File) (int, error) {
	for i := node.KeysPresent - 1; i > -1; i-- {
		if key == node.Keys[i] {
			return node.RecordOffset[i], nil
		}
		if key > node.Keys[i] && node.State == INTERNAL_NODE {
			child, err := readNodeFromFile(node.ChildOffset[i+1], file)
			if err != nil {
				return -1, err
			}
			return child.getInternal(key, file)
		}
	}
	child, err := readNodeFromFile(node.ChildOffset[0], file)

	if err != nil {
		return -1, err
	}
	return child.getInternal(key, file)
}

func (tree *Tree) Get(key int) (int, error) {
	return tree.Root.getInternal(key, tree.StoreFile)
}

func CreateTree(filePath string) (*Tree, error) {
	tree := Tree{}
	file, err := utils.CreateFileRecursively(filePath)
	if err != nil {
		return nil, err
	}
	tree.StoreFile = file
	updateRootOffset(0, tree.StoreFile)
	node, err := createNode(file)

	if err != nil {
		return nil, err
	}

	node.M = BTREE_INDEX
	node.State = LEAF
	node.KeysPresent = 0

	tree.Root = node
	err = updateRootOffset(tree.Root.SelfOffset, tree.StoreFile)

	return &tree, err
}
