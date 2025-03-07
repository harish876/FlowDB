package btree

import (
	"encoding/binary"

	"github.com/harish876/scratchdb/src/utils"
)

const HEADER = 4

const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000
const (
	BNODE_NODE = 1
	BNODE_LEAF = 2
)

/*
		### Node Structure

		| type | nkeys |  pointers  |   offsets  | key-values | unused |
		|------|-------|------------|------------|------------|--------|
		|  2B  |   2B  | nkeys * 8B | nkeys * 2B |     ...    |        |

		| klen | vlen | key | val |
		|------|------|-----|-----|
		|  2B  |  2B  | ... | ... |


		+----------------+----------------+----------------+----------------+----------------+--------+
		|      type      |      nkeys     |    pointers    |    offsets     |  key-values    | unused |
		|      2B        |      2B        |  nkeys * 8B    |  nkeys * 2B    |     ...        |        |
		+----------------+----------------+----------------+----------------+----------------+--------+


		How do the offsets work?
		-------------------------------------------------
		| KV1 | KV2 | KV3 | KV4 | KV5 |	KV6 | KV7 | ....|
		-------------------------------------------------

		So if getOffset(0) gives the start of KV1
		and getOffset(1) gives the start of KV2,
	    and so on.
*/

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	utils.Assert(node1max <= BTREE_PAGE_SIZE)
}

type BNode []byte

type BTree struct {
	//pointer  ( a non zero page number)
	root uint64

	//callback functions to manage on-disk storage
	get func(uint64) BNode  //get reads a page from disk.
	new func([]byte) uint64 //new allocates and writes a new page (copy-on-write).
	del func(uint64)        //del deallocates a page.
}

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	utils.Assert(idx < node.nkeys(), "Assert failed at getPtr")
	pos := HEADER + idx*8
	return binary.LittleEndian.Uint64(node[pos:])
}

func (node BNode) setPtr(idx uint16, value uint64) {
	utils.Assert(idx < node.nkeys())
	pos := HEADER + idx*8
	binary.LittleEndian.PutUint64(node[pos:], value)
}

func offsetPos(node BNode, idx uint16) uint16 {
	utils.Assert(1 <= idx && idx <= node.nkeys(), "Assertion failed at offsetPos")
	return HEADER + 8*(node.nkeys()) + 2*(idx-1)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	pos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node[pos:])
}

func (node BNode) setOffset(idx, value uint16) {
	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node[pos:], value)
}

func (node BNode) kvPos(idx uint16) uint16 {
	utils.Assert(idx <= node.nkeys())
	return HEADER + 8*(node.nkeys()) + 2*node.nkeys() + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

func (node BNode) getValue(idx uint16) []byte {
	utils.Assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// insert a new key or update an existing key
func (tree *BTree) Insert(key []byte, val []byte) {
	utils.Assert(len(key) != 0)
	utils.Assert(len(key) <= BTREE_MAX_KEY_SIZE)
	utils.Assert(len(val) <= BTREE_MAX_VAL_SIZE)
	if tree.root == 0 {
		//create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		// a dummy key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		tree.root = tree.new(root)
		return
	}

	node := treeInsert(tree, tree.get(tree.root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.del(tree.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i, knode := range split[:nsplit] {
			ptr, key := tree.new(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}
		tree.root = tree.new(root)
	} else {
		tree.root = tree.new(split[0])
	}
}

// delete a key and returns whether the key was there
func (tree *BTree) Delete(key []byte) bool {
	utils.Assert(len(key) != 0)
	utils.Assert(len(key) <= BTREE_PAGE_SIZE)
	if tree.root == 0 {
		return false
	}
	updated := treeDelete(tree, tree.get(tree.root), key)
	if len(updated) == 0 {
		return false
	}

	tree.del(tree.root)
	if updated.btype() == BNODE_NODE && updated.nkeys() == 1 {
		tree.root = updated.getPtr(0)
	} else {
		tree.root = tree.new(updated)
	}
	return true
}
