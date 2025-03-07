package btree

import (
	"bytes"

	"github.com/harish876/scratchdb/src/utils"
)

// remove a key from a leaf node
func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(old.btype(), old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-idx-1)
}

// sizeof(merge(left, right)) <= BTREE_PAGE_SIZE, this condition
// must be checked by the caller
func nodeMerge(merged BNode, left BNode, right BNode) {
	utils.Assert(left.btype() == right.btype())
	merged.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(merged, left, 0, 0, left.nkeys())
	nodeAppendRange(merged, right, left.nkeys(), 0, right.nkeys())
}

// replace 2 adjacent links with 1
func nodeReplace2Kid(
	new BNode, old BNode, idx uint16,
	ptr uint64, key []byte,
) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

// delete a key from the tree
func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if !bytes.Equal(key, node.getKey(idx)) {
			return BNode{} // key does not exist
		}
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	case BNODE_NODE:
		return nodeDelete(tree, node, idx, key)
	default:
		panic("unknown BNode type")
	}
}

// should the updated kid be merged with a sibling?
func shouldMerge(
	tree *BTree, node BNode,
	idx uint16, updated BNode,
) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}
	if idx+1 < node.nkeys() {
		sibling := BNode(tree.get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}
	return 0, BNode{}
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.new(merged), merged.getKey(0))
	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:
		utils.Assert(node.nkeys() == 1 && idx == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)                // the parent becomes empty too
	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}
	return new
}
