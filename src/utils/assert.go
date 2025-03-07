package utils

import (
	"fmt"
	"testing"
)

const DEFAULT_MESSAGE = "Default Assertiong Message"

func Assert(result bool, message ...string) {
	formatted_message := DEFAULT_MESSAGE
	if len(message) > 0 {
		formatted_message = message[0]
	}
	if !result {
		panic(fmt.Sprintf("Assertion failed at %s", formatted_message))
	}
}

// AssertPanic asserts that the provided function panics.
func AssertPanic(t *testing.T, f func(), message string) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic, but did not panic: %s", message)
		}
	}()
	f()
}
