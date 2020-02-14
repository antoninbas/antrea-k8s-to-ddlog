package ddlog

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unsafe"
)

var nullPointer = unsafe.Pointer(nil)

func TestEmptyString(t *testing.T) {
	emptyString := ""
	r := RecordString(emptyString)
	// cannot use NotNil here apparently
	assert.NotEqual(t, nullPointer, r.ptr)
}
