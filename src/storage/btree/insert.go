package btree

import (
	"bytes"

	"github.com/harish876/scratchdb/src/utils"
)

// replace a link with one or multiple links
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16,
	kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
		//                ^position      ^pointer        ^key            ^val
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// splits from idx to end, determine if it could be fit into a page
func splitFromIdxFitInOnePage(node BNode, idx uint16) bool {
	utils.Assert(idx < node.nkeys())

	nkeys := node.nkeys() - idx
	kvSize := node.nbytes() - node.kvPos(idx)
	return (HEADER + nkeys*8 + nkeys*2 + kvSize) <= BTREE_PAGE_SIZE
}

// split a bigger-than-allowed node into two.
// the second node always fits on a page
func nodeSplit2(left BNode, right BNode, old BNode) {
	// binary search on old node to find the biggest kvPos < BTREE_PAGE_SIZE
	l := uint16(0)
	r := old.nkeys() - 1
	for l+1 < r {
		m := (l + r) / 2
		if splitFromIdxFitInOnePage(old, m) {
			r = m
		} else {
			l = m
		}
	}
	var startIdx uint16
	if splitFromIdxFitInOnePage(old, l) {
		startIdx = l
	} else {
		startIdx = r
	}
	// 0 ... startIdx - 1 are the smallest possible way to store in left
	left.setHeader(old.btype(), startIdx)
	nodeAppendRange(left, old, 0, 0, startIdx)
	// startIdx ... end will be biggest possible way to fit inside of a page
	right.setHeader(old.btype(), old.nkeys()-startIdx)
	nodeAppendRange(right, old, 0, startIdx, old.nkeys()-startIdx)
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // 2 nodes
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	utils.Assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16,
	key []byte, val []byte,
) {
	kptr := node.getPtr(idx)
	// recursive insertion to the kid node
	knode := treeInsert(tree, tree.get(kptr), key, val)
	// split the result
	nsplit, split := nodeSplit3(knode)
	// deallocate the kid node
	tree.del(kptr)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
}

func leafInsert(new BNode, old BNode, idx uint16, key, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, value)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key, value []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, value)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-idx-1)
}

// insert a KV into a node, the result might be split.
// the caller is responsible for deallocating the input node
// and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node.
	// it's allowed to be bigger than 1 page and will be split if so
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position.
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}
