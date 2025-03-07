package btree

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/harish876/scratchdb/src/utils"
)

type C struct {
	tree  BTree
	ref   map[string]string // the reference data
	pages map[uint64]BNode  // in-memory pages
}

func newC() *C {
	pages := map[uint64]BNode{}
	return &C{
		tree: BTree{
			get: func(ptr uint64) BNode {
				node, ok := pages[ptr]
				utils.Assert(ok)
				return node
			},
			new: func(node []byte) uint64 {
				utils.Assert(BNode(node).nbytes() <= BTREE_PAGE_SIZE)
				ptr := uint64(uintptr(unsafe.Pointer(&node[0])))
				utils.Assert(pages[ptr] == nil)
				pages[ptr] = node
				return ptr
			},
			del: func(ptr uint64) {
				utils.Assert(pages[ptr] != nil)
				delete(pages, ptr)
			},
		},
		ref:   map[string]string{},
		pages: pages,
	}
}

func TestHeaders(t *testing.T) {
	node := BNode(make(BNode, BTREE_PAGE_SIZE))
	node.setHeader(BNODE_LEAF, 1)

	utils.Assert(node.btype() == BNODE_LEAF, "Assertion Failed. Node type is not BNODE_LEAF")
	utils.Assert(node.nkeys() == 1, "Assertion Failed. Node keys")
}

func TestBNodePtr(t *testing.T) {
	testNode := BNode(make([]byte, BTREE_PAGE_SIZE))
	testNode.setHeader(BNODE_NODE, 10)
	utils.Assert(testNode.getPtr(0) == uint64(0))

	r := uint64(999999999)
	testNode.setPtr(0, r)
	utils.Assert(testNode.getPtr(0) == r)

	r = uint64(5201314)
	testNode.setPtr(1, r)
	utils.Assert(testNode.getPtr(1) == r)
}

func TestBTreeInsert(t *testing.T) {
	container := newC()
	utils.Assert(container.tree.root == uint64(0))

	keyTooLong := make([]byte, 1001)
	utils.AssertPanic(t, func() { container.tree.Insert(keyTooLong, nil) }, "Key too long")
	valueTooLong := make([]byte, 3001)
	utils.AssertPanic(t, func() { container.tree.Insert([]byte{byte(0)}, valueTooLong) }, "Value too long")

	utils.AssertPanic(t, func() { container.tree.Insert(nil, nil) }, "Null KV pair")

	// Insert a valid key-value pair
	key5 := make([]byte, 1000)
	key5[0] = byte(5)
	val5 := make([]byte, 200)
	container.tree.Insert(key5, val5)

	utils.Assert(container.tree.root != 0, "Root should not be 0")
	root := container.tree.get(container.tree.root)
	utils.Assert(root.btype() == uint16(BNODE_LEAF), "Root should be a leaf node")
	utils.Assert(root.nkeys() == uint16(2), "Root should have 2 keys")
	utils.Assert(bytes.Equal(root.getKey(0), []byte{}), "First key should be empty")
	utils.Assert(bytes.Equal(root.getValue(0), []byte{}), "First value should be empty")
	utils.Assert(bytes.Equal(root.getKey(1), key5), "Second key should be key5")
	utils.Assert(bytes.Equal(root.getValue(1), val5), "Second value should be val5")

	// Update the value for the existing key
	val5 = make([]byte, 3000)
	container.tree.Insert(key5, val5)

	// Insert a long key-value pair
	key7 := make([]byte, 1000)
	key7[0] = byte(7)
	val7 := make([]byte, 3000)
	container.tree.Insert(key7, val7)

	root = container.tree.get(container.tree.root)
	utils.Assert(root.btype() == uint16(BNODE_NODE), "Root should be an internal node")
	utils.Assert(root.nkeys() == uint16(2), "Root should have 2 keys")
	utils.Assert(bytes.Equal(root.getKey(0), []byte{}), "First key should be empty")
	utils.Assert(bytes.Equal(root.getKey(1), key7), "Second key should be key7")

	leftChild := container.tree.get(root.getPtr(0))
	utils.Assert(leftChild.btype() == uint16(BNODE_LEAF), "Left child should be a leaf node")
	utils.Assert(leftChild.nkeys() == uint16(2), "Left child should have 2 keys")
	utils.Assert(bytes.Equal(leftChild.getKey(0), []byte{}), "First key in left child should be empty")
	utils.Assert(bytes.Equal(leftChild.getValue(0), []byte{}), "First value in left child should be empty")
	utils.Assert(bytes.Equal(leftChild.getKey(1), key5), "Second key in left child should be key5")
	utils.Assert(bytes.Equal(leftChild.getValue(1), val5), "Second value in left child should be val5")

	rightChild := container.tree.get(root.getPtr(1))
	utils.Assert(rightChild.btype() == uint16(BNODE_LEAF), "Right child should be a leaf node")
	utils.Assert(rightChild.nkeys() == uint16(1), "Right child should have 1 key")
	utils.Assert(bytes.Equal(rightChild.getKey(0), key7), "First key in right child should be key7")
	utils.Assert(bytes.Equal(rightChild.getValue(0), val7), "First value in right child should be val7")

	// Insert another long key-value pair
	key9 := make([]byte, 1000)
	key9[0] = byte(9)
	val9 := make([]byte, 3000)
	container.tree.Insert(key9, val9)
	root = container.tree.get(container.tree.root)
	utils.Assert(root.nkeys() == uint16(3), "Root should have 3 keys")

	// Insert another long key-value pair
	key11 := make([]byte, 1000)
	key11[0] = byte(11)
	val11 := make([]byte, 3000)
	container.tree.Insert(key11, val11)
	root = container.tree.get(container.tree.root)
	utils.Assert(root.nkeys() == uint16(4), "Root should have 4 keys")

	// Insert another long key-value pair
	key13 := make([]byte, 1000)
	key13[0] = byte(13)
	val13 := make([]byte, 3000)
	container.tree.Insert(key13, val13)
	root = container.tree.get(container.tree.root)
	utils.Assert(root.nkeys() == uint16(5), "Root should have 5 keys")

	// Insert another long key-value pair
	key15 := make([]byte, 1000)
	key15[0] = byte(15)
	val15 := make([]byte, 3000)
	container.tree.Insert(key15, val15)
	root = container.tree.get(container.tree.root)
	utils.Assert(root.nkeys() == uint16(2), "Root should have 2 keys")
	utils.Assert(bytes.Equal(root.getKey(0), []byte{}), "First key should be empty")
	utils.Assert(bytes.Equal(root.getKey(1), key9), "Second key should be key9")

	leftInternal := container.tree.get(root.getPtr(0))
	utils.Assert(leftInternal.btype() == uint16(BNODE_NODE), "Left internal node should be an internal node")
	utils.Assert(leftInternal.nkeys() == uint16(2), "Left internal node should have 2 keys")
	utils.Assert(bytes.Equal(leftInternal.getKey(0), []byte{}), "First key in left internal node should be empty")
	utils.Assert(bytes.Equal(leftInternal.getKey(1), key7), "Second key in left internal node should be key7")

	rightInternal := container.tree.get(root.getPtr(1))
	utils.Assert(rightInternal.btype() == uint16(BNODE_NODE), "Right internal node should be an internal node")
	utils.Assert(rightInternal.nkeys() == uint16(4), "Right internal node should have 4 keys")
	utils.Assert(bytes.Equal(rightInternal.getKey(0), key9), "First key in right internal node should be key9")
	utils.Assert(bytes.Equal(rightInternal.getKey(1), key11), "Second key in right internal node should be key11")
	utils.Assert(bytes.Equal(rightInternal.getKey(2), key13), "Third key in right internal node should be key13")
	utils.Assert(bytes.Equal(rightInternal.getKey(3), key15), "Fourth key in right internal node should be key15")

}
