package ddlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptyString(t *testing.T) {
	emptyString := ""
	r := NewRecordString(emptyString)
	defer r.Free()
	assert.False(t, r.IsNull())
}
