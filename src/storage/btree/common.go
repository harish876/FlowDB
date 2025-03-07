package btree

import (
	"bytes"
	"encoding/binary"

	"github.com/harish876/scratchdb/src/utils"
)

// returns the first kid node whose range intersects the key. (kid[i] <= key)
func nodeLookupLE(node BNode, key []byte, useBSearch ...bool) uint16 {
	nkeys := node.nkeys()
	found := uint16(0)

	if len(useBSearch) > 0 && useBSearch[0] {
		left, right := uint16(1), nkeys+1
		for left < right {
			mid := (left + right) / 2
			cmp := bytes.Compare(node.getKey(mid), key)
			if cmp <= 0 {
				found = mid
				left = mid + 1
			} else {
				right = mid
			}
		}
		return found
	} else {
		for i := uint16(1); i < nkeys; i++ {
			cmp := bytes.Compare(node.getKey(i), key)
			if cmp <= 0 {
				found = i
			}

			if cmp >= 0 {
				break
			}
		}
		return found
	}
}

// copy a KV into the position
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// copy multiple KVs into the position from the old node
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	utils.Assert(srcOld+n <= old.nkeys())
	utils.Assert(dstNew+n <= new.nkeys())

	for i := uint16(0); i < n; i++ {
		// Copy pointer
		ptr := old.getPtr(srcOld + i)
		new.setPtr(dstNew+i, ptr)

		// Copy key-value pair
		key := old.getKey(srcOld + i)
		val := old.getValue(srcOld + i)
		nodeAppendKV(new, dstNew+i, ptr, key, val)
	}
}
