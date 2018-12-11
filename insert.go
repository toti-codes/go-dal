package dal

import (
	"reflect"
	"strings"
)

/**
INSERT Section
 */

type InsertBuilder struct {
	b *Builder
	returnLastId bool
}

func (b *InsertBuilder) Column(column, parameter string) *InsertBuilder {
	p := columnPartSQL{part:columnsPartEnum}

	if p.parts == nil {
		p.parts = make([]columnSQL, 1)
	}
	p.parts[0] = columnSQL{name: column, parameter:parameter}

	if ok, part := b.b.getPart(columnsPartEnum); ok {
		parts := part.(columnPartSQL).parts
		p.parts = append(parts, columnSQL{name: column, parameter:parameter})

		b.b.removePart(columnsPartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

func (b *InsertBuilder) Type(entity interface{}) *InsertBuilder {
	e := reflect.ValueOf(entity)
	var columnNames []string

	count := 0
	for i := 0; i < e.NumField(); i++ {
		columnsConfig := strings.Split(e.Type().Field(i).Tag.Get("db"), ", ")
		columnName := columnsConfig[0]

		if columnName == "" {
			columnName = e.Type().Field(i).Name
		}

		value := e.Field(i).Interface()
		if columnName == "id" {
			value = int64(value.(int64))
		}

		if len(columnsConfig) > 1 && columnsConfig[1] != "autoincrement" || len(columnsConfig) == 1 {
			columnNames = append(columnNames, columnName)
			b.SetParameter(count, value)
			count += 1
		}
	}

	b.Columns(columnNames...)

	return b
}

func (b *InsertBuilder) Columns(columns ...string) *InsertBuilder {
	if len(columns) == 0 {
		return b
	}

	p := columnPartSQL{part:columnsPartEnum}

	p.parts = make([]columnSQL, len(columns))
	for c, i := range columns {
		p.parts[c] = columnSQL{name:i, parameter:"?"}
	}

	if ok, part := b.b.getPart(columnsPartEnum); ok {
		parts := part.(columnPartSQL).parts
		p.parts = append(parts, p.parts...)

		b.b.removePart(columnsPartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

func (b *InsertBuilder) LastInsertId() *InsertBuilder {
	b.returnLastId = true

	return b
}

func (b *InsertBuilder) Build() {
	b.b.Build()
}

func (b *InsertBuilder) GetBuilder() *Builder {
	return b.b
}

func (b *InsertBuilder) GetSQL() (q string) {

	q = "INSERT INTO "

	if ok, bSelect := b.b.getPart(tablePartEnum); ok {
		q += bSelect.getSQL()
	}

	if ok, bSelect := b.b.getPart(columnsPartEnum); ok {
		cols := make([]string, len(bSelect.(columnPartSQL).parts))
		vals := make([]string, len(cols))
		for i, o := range bSelect.(columnPartSQL).parts {
			cols[i] = o.name
			vals[i] = o.parameter
		}
		q += "(" + strings.Join(cols, ", ") + ") "
		q += "VALUES (" + strings.Join(vals, ", ") + ")"
	}

	if b.returnLastId {
		q += " RETURNING id"
	}

	return
}

func (b *InsertBuilder) SetParameter(p, v interface{}) *InsertBuilder {
	b.b.SetParameter(p, v)

	return b
}
