package dal

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestQuerySelectFrom(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From(As("table_1", "t1")).Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 T1", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From(As("table_1", "t1")).From("table_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 T1, TABLE_2", strings.ToUpper(b.GetSQL()))

}

func TestQuerySelectWhere(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").Where("column_1 = ?").
		SetParameter(0, "first").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE (COLUMN_1 = $1)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first"}, b.GetParameters())

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").Where("column_1 = ?").Where("column_2 = ?").
		SetParameter(0, "first").SetParameter(1, "second").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE ((COLUMN_1 = $1) AND (COLUMN_2 = $2))", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second"}, b.GetParameters())

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").Where("column_1 = ?").Where("column_2 = ?").OrWhere("column_3 = ?").
		SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 WHERE ((COLUMN_1 = $1) AND (COLUMN_2 = $2) OR (COLUMN_3 = $3))", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third"}, b.GetParameters())

}

func TestQuerySelectJoin(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From("table_1").
		InnerJoin(Join{FromAlias: "table_1", JoinTable: "table_2", JoinCondition: "table_2.id = table_1.table_2_id"}).Build()

	expected := "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 " +
		"INNER JOIN TABLE_2 ON TABLE_2.ID = TABLE_1.TABLE_2_ID"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From(As("table_1", "t1")).
		InnerJoin(Join{FromAlias: "t1", JoinTable: "table_2", JoinCondition: "table_2.id = t1.table_2_id"}).Build()

	expected = "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 T1 " +
		"INNER JOIN TABLE_2 ON TABLE_2.ID = T1.TABLE_2_ID"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From(As("table_1", "t1")).
		InnerJoin(Join{FromAlias: "t1", JoinTable: As("table_2", "t2"), JoinCondition: "t2.id = t1.table_2_id"}).Build()

	expected = "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 T1 " +
		"INNER JOIN TABLE_2 T2 ON T2.ID = T1.TABLE_2_ID"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From(As("table_1", "t1")).
		InnerJoin(Join{FromAlias: "t1", JoinTable: As("table_2", "t2"), JoinCondition: "t2.id = t1.table_2_id"}).
		InnerJoin(Join{FromAlias: "t2", JoinTable: As("table_3", "t3"), JoinCondition: "t3.id = t2.table_3_id"}).Build()

	expected = "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 T1 " +
		"INNER JOIN TABLE_2 T2 ON T2.ID = T1.TABLE_2_ID " +
		"INNER JOIN TABLE_3 T3 ON T3.ID = T2.TABLE_3_ID"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From(As("table_1", "t1")).
		InnerJoin(Join{FromAlias: "t1", JoinTable: As("table_2", "t2"), JoinCondition: "t2.id = t1.table_2_id"}).
		LeftJoin(Join{FromAlias: "t2", JoinTable: As("table_3", "t3"), JoinCondition: "t3.id = t2.table_3_id"}).
		RightJoin(Join{FromAlias: "t3", JoinTable: As("table_4", "t4"), JoinCondition: "t4.id = t3.table_4_id"}).Build()

	expected = "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 T1 " +
		"INNER JOIN TABLE_2 T2 ON T2.ID = T1.TABLE_2_ID " +
		"LEFT JOIN TABLE_3 T3 ON T3.ID = T2.TABLE_3_ID " +
		"RIGHT JOIN TABLE_4 T4 ON T4.ID = T3.TABLE_4_ID"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))

}

func TestQuerySelectGroupHaving(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").
		From("table_1").
		InnerJoin(Join{FromAlias: "table_1", JoinTable: "table_2", JoinCondition: "table_2.id = table_1.table_2_id"}).
		GroupBy("column_1").
		Having("column_1 = ?").SetParameter(0, "first").Build()

	expected := "SELECT COLUMN_1, COLUMN_2, COLUMN_3 " +
		"FROM TABLE_1 " +
		"INNER JOIN TABLE_2 ON TABLE_2.ID = TABLE_1.TABLE_2_ID " +
		"GROUP BY COLUMN_1 HAVING COLUMN_1 = $1"

	assert.Equal(t, expected, strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first"}, b.GetParameters())

}

func TestQuerySelectOrder(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderASC("column_1").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1 ASC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderASC("column_1", "column_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1, COLUMN_2 ASC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderASC("column_1").OrderASC("column_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1, COLUMN_2 ASC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderDESC("column_1").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1 DESC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderDESC("column_1", "column_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1, COLUMN_2 DESC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderDESC("column_1").OrderDESC("column_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1, COLUMN_2 DESC", strings.ToUpper(b.GetSQL()))

	b = NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").OrderASC("column_1").OrderDESC("column_2").Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 ORDER BY COLUMN_1 ASC, COLUMN_2 DESC", strings.ToUpper(b.GetSQL()))

}

func TestQueryTestSelectLimit(t *testing.T) {

	b := NewBuilder()

	b.Select("column_1", "column_2", "column_3").From("table_1").FirstResult(10).MaxResult(20).Build()

	assert.Equal(t, "SELECT COLUMN_1, COLUMN_2, COLUMN_3 FROM TABLE_1 LIMIT 20 OFFSET 9", strings.ToUpper(b.GetSQL()))
}

type testDB struct {
	Id       int64  `db:"id, autoincrement"`
	Name     string `db:"name"`
	LastName string `db:"last_name"`
}

func TestQueryInsert(t *testing.T) {

	b := NewBuilder()

	b.Insert("table_1").
		Columns("column_1", "column_2", "column_3").SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").Build()

	assert.Equal(t, "INSERT INTO TABLE_1(COLUMN_1, COLUMN_2, COLUMN_3) VALUES ($1, $2, $3)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third"}, b.GetParameters())

	b = NewBuilder()

	b.Insert("table_1").
		Columns("column_1", "column_2", "column_3").
		Column("column_4", "?::bit").
		SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").SetParameter(3, "fourth").Build()

	assert.Equal(t, "INSERT INTO TABLE_1(COLUMN_1, COLUMN_2, COLUMN_3, COLUMN_4) VALUES ($1, $2, $3, $4::BIT)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third", "fourth"}, b.GetParameters())

	persist := testDB{Name: "first", LastName: "second"}

	b = NewBuilder()

	b.Insert("table_persist").Type(persist).LastInsertId().Build()

	assert.Equal(t, "INSERT INTO TABLE_PERSIST(NAME, LAST_NAME) VALUES ($1, $2) RETURNING ID", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second"}, b.GetParameters())

}

func TestQueryUpdate(t *testing.T) {

	b := NewBuilder()

	b.Update("table_1").
		Sets("column_1", "column_2", "column_3").
		SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").Build()

	assert.Equal(t, "UPDATE TABLE_1 SET COLUMN_1 = $1, COLUMN_2 = $2, COLUMN_3 = $3", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third"}, b.GetParameters())

	b = NewBuilder()

	b.Update("table_1").
		Sets("column_1", "column_2", "column_3").
		Set("column_4", "?::bit").
		SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").SetParameter(3, "fourth").Build()

	assert.Equal(t, "UPDATE TABLE_1 SET COLUMN_1 = $1, COLUMN_2 = $2, COLUMN_3 = $3, COLUMN_4 = $4::BIT", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third", "fourth"}, b.GetParameters())

	b = NewBuilder()

	b.Update("table_1").
		Sets("column_1", "column_2", "column_3").
		Set("column_4", "?::bit").
		Where("id = :id").
		SetParameter(0, "first").SetParameter(1, "second").SetParameter(2, "third").SetParameter(3, "fourth").SetParameter("id", "10").Build()

	assert.Equal(t, "UPDATE TABLE_1 SET COLUMN_1 = $1, COLUMN_2 = $2, COLUMN_3 = $3, COLUMN_4 = $4::BIT WHERE (ID = $5)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", "third", "fourth", "10"}, b.GetParameters())

	b = NewBuilder()

	b.Update("table_1").
		Set("column_1", ":columna").
		Set("column_2", ":columnb").
		Where("id = :id").
		SetParameter("columna", "first").SetParameter("columnb", "second").SetParameter("id", 10).Build()

	assert.Equal(t, "UPDATE TABLE_1 SET COLUMN_1 = $1, COLUMN_2 = $2 WHERE (ID = $3)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", 10}, b.GetParameters())

	persist := testDB{Id: 1, Name: "first", LastName: "second"}

	b = NewBuilder()

	b.Update("table_persist").Type(persist).Build()

	assert.Equal(t, "UPDATE TABLE_PERSIST SET NAME = $1, LAST_NAME = $2 WHERE (ID = $3)", strings.ToUpper(b.GetSQL()))
	assert.Equal(t, []interface{}{"first", "second", int64(1)}, b.GetParameters())

}
