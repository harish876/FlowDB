package utils_test

import (
	"testing"

	"github.com/harish876/scratchdb/src/utils"
)

func TestNoAssert(t *testing.T) {
	utils.Assert(true)
}
func TestAssert(t *testing.T) {
	utils.Assert(false)
}
