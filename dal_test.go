package dal

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

var config = map[string]string{"database": "test_go","user": "postgres", "password": "123456", "ssl": "disable"}

func createData(t *testing.T) *Session {

	err := GetConnectionManager().AddSingleDB(config)

	assert.NoError(t, err)

	session := GetConnectionManager().GetSingleConnection().GetSession()

	b := NewBuilder()

	b.SQL("CREATE TABLE IF NOT EXISTS TEST (id serial, name varchar(50), active bool, json json, birth_date_long bigint, birth_date_date date, primary key(id))").Build()

	err = session.Exec(*b)

	assert.NoError(t, err)

	b = NewBuilder()

	b.Delete("test").Build()

	err = session.Exec(*b)

	assert.NoError(t, err)

	insert(t, session)

	return session
}

func insert(t *testing.T, sess *Session) {

	var b Builder
	b.Insert("Test").
		Columns("name", "active", "json", "birth_date_long", "birth_date_date").
		SetParameter(0, "daniel").
		SetParameter(1, true).
		SetParameter(2, "[]").
		SetParameter(3, time.Now().UnixNano() / int64(time.Millisecond)).
		SetParameter(4, time.Now()).
		Build()

	err := sess.Exec(b)

	assert.NoError(t, err)

	b = Builder{}

	b.Insert("Test").
		Columns("name", "active", "json", "birth_date_long", "birth_date_date").
		SetParameter(0, "johan").
		SetParameter(1, true).
		SetParameter(2, "[]").
		SetParameter(3, time.Now().UnixNano() / int64(time.Millisecond)).
		SetParameter(4, time.Now()).
		Build()

	err = sess.Exec(b)

	assert.NoError(t, err)

}

func TestQuery(t *testing.T) {

	sess := createData(t)

	var b Builder
	b.Select(As("count(1)", "count")).
		From("TEST").Build()

	first, err := sess.FirstResult(b)

	assert.NoError(t, err)

	var scan int64

	err = sess.Scan(b, &scan)

	assert.NoError(t, err)

	assert.Equal(t, first["count"], scan)

	if first != nil && first["count"].(int64) > 0 {
		var b Builder
		b.Select("id", "name", "active", "json", "birth_date_long", "birth_date_date").
			From("test").Build()

		d, err := sess.Query(b)

		assert.NoError(t, err)

		band := len(d) >= 0

		assert.Equal(t, true, band)
	}

}

type test struct{
	Id int
	Name string
	History string
	Active bool
	//Lived bool
	Json string
	Birth_date_long int
	Birth_date_date time.Time
}

func TestQueryType(t *testing.T) {

	sess := createData(t)

	var b Builder
	b.Select("id", "name", "active", "json", "birth_date_long", "birth_date_date").
		From("table_1").Build()

	var o = []test{}

	err := sess.QueryType(b, &o)

	if err != nil {
		assert.NoError(t, err)
	}

	band := len(o) >= 0

	assert.Equal(t, true, band)

}