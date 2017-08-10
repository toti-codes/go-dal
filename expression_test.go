package dal

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestExp(t *testing.T) {
	current := And(
		Or(
			Eq("column_1", "?"),
			Neq("column_2", "?")),
		Gt("column_3", "?"),
		Lte("column_4", "?"))

	expected := "((column_1 = ? OR column_2 != ?) AND column_3 > ? AND column_4 <= ?)"

	assert.Equal(t, expected, current)
}
