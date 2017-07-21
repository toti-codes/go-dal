package dal

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"strings"
)

func TestSelect(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From("table_1").
		Where("id = ?")

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE (ID = ?) ", strings.ToUpper(b.GetSQL()))

}

func TestOrderBy(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From("table_1").
		Where("id = ?").
		OrderBy("id", "column_1").ASC()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE (ID = ?) ORDER BY ID, COLUMN_1 ASC ", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From("table_1").
		Where("id = ?").
		OrderBy("id", "column_1").ASC().
		OrderBy("column_2").DESC()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE (ID = ?) ORDER BY ID, COLUMN_1 ASC , COLUMN_2 DESC ", strings.ToUpper(b.GetSQL()))

}

func TestInsert(t *testing.T) {

	b := NewBuilder()

	b.Insert("table_1").
		Columns("column_1", "column_2", "column_3")

	assert.Equal(t, "INSERT INTO TABLE_1 (COLUMN_1, COLUMN_2, COLUMN_3) VALUES (?, ?, ?) ", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Insert("table_1").
		Columns("column_1", "column_2", "column_3").
		Column("column_4", "?::bit")

	assert.Equal(t, "INSERT INTO TABLE_1 (COLUMN_1, COLUMN_2, COLUMN_3, COLUMN_4) VALUES (?, ?, ?, ?::BIT) ", strings.ToUpper(b.GetSQL()))

}

func TestInsertParams(t *testing.T) {

	b := NewBuilder()

	b.Insert("table_1").
		Columns("column_1", "column_2", "column_3").
		SetParameter(0, 1).
		SetParameter(1, 2).
		SetParameter(2, 3)

	assert.Equal(t, "INSERT INTO TABLE_1 (COLUMN_1, COLUMN_2, COLUMN_3) VALUES (?, ?, ?) ", strings.ToUpper(b.GetSQL()))

	assert.Equal(t, []interface{}{1,2,3}, b.GetParameters())

}
