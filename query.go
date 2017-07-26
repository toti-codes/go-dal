package dal

import (
	"strings"
)

var (
	firstResult = 0
	maxResult   = 0
)

//Builder -
type Builder struct {
	sqlType  sqlEnum
	state    stateEnum
	sql      string
	sqlParts []part
	params   map[interface{}]interface{}
}

type OrderBuilder struct {
	b *Builder
	sqlParts []string
}

func NewBuilder() *Builder {
	b := Builder{}
	if b.sqlParts == nil {
		b.state = stateDirty
		b.sqlParts = make([]part, 0)
	}
	return &b
}

/**
SELECT Section
 */

//Select -
func (b *Builder) Select(columns ...string) *Builder {
	b.sqlType = selectEnum

	if len(columns) > 0 {
		b.add(partSQL{part: selectPartEnum, parts: columns})
	}

	return b
}

//From -
func (b *Builder) From(tables string) *Builder {
	p := fromPartSQL{part: fromPartEnum}
	if p.parts == nil {
		p.parts = make([]string, 1)
	}
	p.parts[0] = tables
	b.add(p)

	return b
}

//InnerJoin -
func (b *Builder) InnerJoin(from, j, a, c string) *Builder {
	b.addJoin(inner, from, j, a, c)

	return b
}

//LeftJoin -
func (b *Builder) LeftJoin(f, j, a, c string) *Builder {
	b.addJoin(left, f, j, a, c)

	return b
}

//RightJoin -
func (b *Builder) RightJoin(f, j, a, c string) *Builder {
	b.addJoin(right, f, j, a, c)

	return b
}

func (b *Builder) OrderBy(columns ...string) *OrderBuilder {
	o := OrderBuilder{b:b}
	o.sqlParts = columns

	return &o
}

func (b *OrderBuilder) ASC() *Builder {
	b.b.add(partSQL{part: orderByASCPartEnum, parts: []string{b.getSQL()}})

	return b.b
}

func (b *OrderBuilder) DESC() *Builder {
	b.b.add(partSQL{part: orderByDESCPartEnum, parts: []string{b.getSQL()}})

	return b.b
}

func (b *OrderBuilder) getSQL() string {
	return strings.Join(b.sqlParts, ", ")
}

/**
INSERT Section
 */

func (b *Builder) Insert(table string) *Builder {
	b.sqlType = insertEnum

	b.add(fromPartSQL{part: insertPartEnum, parts: []string {table}})

	return b
}

func (b *Builder) Column(column, parameter string) *Builder {
	appendPart := []insertValueSQL{{name: column, parameter:parameter}}

	var newParts []insertValueSQL

	if ok, part := b.getPart(columnsPartEnum); ok {
		parts := part.(insertValuePartSQL).parts
		newParts = append(parts, appendPart...)

		b.removePart(columnsPartEnum)
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: newParts})
	} else {
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: appendPart})
	}

	return b
}

func (b *Builder) Columns(columns ...string) *Builder {
	if len(columns) == 0 {
		return b
	}

	appendPart := make([]insertValueSQL, len(columns))
	for c, i := range columns {
		appendPart[c] = insertValueSQL{name:i, parameter:"?"}
	}

	if ok, part := b.getPart(columnsPartEnum); ok {
		parts := part.(insertValuePartSQL).parts
		parts = append(parts, appendPart...)
	} else {
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: appendPart})
	}

	return b
}

/**
UPDATE Section
 */

func (b *Builder) Update(table string) *Builder {
	b.sqlType = updateEnum

	return b.From(table)
}

func (b *Builder) Set(column, parameter string) *Builder {
	appendPart := []insertValueSQL{{name: column, parameter:parameter}}

	var newParts []insertValueSQL

	if ok, part := b.getPart(columnsPartEnum); ok {
		parts := part.(insertValuePartSQL).parts
		newParts = append(parts, appendPart...)

		b.removePart(columnsPartEnum)
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: newParts})
	} else {
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: appendPart})
	}

	return b
}

func (b *Builder) Sets(columns ...string) *Builder {
	if len(columns) == 0 {
		return b
	}

	appendPart := make([]insertValueSQL, len(columns))
	for c, i := range columns {
		appendPart[c] = insertValueSQL{name:i, parameter:"?"}
	}

	if ok, part := b.getPart(columnsPartEnum); ok {
		parts := part.(insertValuePartSQL).parts
		parts = append(parts, appendPart...)
	} else {
		b.add(insertValuePartSQL{part: columnsPartEnum, parts: appendPart})
	}

	return b
}

/**
WHERE Section
 */

//Where -
func (b *Builder) Where(condition string) *Builder {
	p := partSQL{part: wherePartEnum}
	if p.parts == nil {
		p.parts = make([]string, 1)
	}
	p.parts[0] = "(" + condition + ")"
	b.add(p)

	return b
}

//AndWhere -
func (b *Builder) AndWhere(condition string) *Builder {
	p := partSQL{part: wherePartEnum}
	p.parts = []string{"AND (" + condition + ")"}
	b.add(p)

	return b
}

//OrWhere -
func (b *Builder) OrWhere(condition string) *Builder {
	p := partSQL{part: wherePartEnum}
	p.parts = []string{"OR (" + condition + ")"}
	b.add(p)

	return b
}

//SetParameter -
func (b *Builder) SetParameter(p, v interface{}) *Builder {
	if b.params == nil {
		b.params = make(map[interface{}]interface{})
	}
	if _, ok := b.params[p]; !ok {
		b.params[p] = v
	}
	return b
}

func (b *Builder) GetParameters() []interface{} {
	values := make([]interface{}, len(b.params))

	i := 0
	for _, v := range b.params {
		values[i] = v
		i++
	}

	return values
}

//GetSQL -
func (b *Builder) GetSQL() string {
	if b.sql != "" && b.state == stateClean {
		return b.sql
	}

	switch b.sqlType {
	case selectEnum:
		b.sql = b.getSQLForSelect()
		break
	case insertEnum:
		b.sql = b.getSQLForInsert()
		break
	case updateEnum:
		b.sql = b.getSQLForUpdate()
		break
	}

	return b.sql
}

/*
Utils functions
 */

func (b *Builder) getPart(e partEnum) (bool, part) {
	r := -1
	var p part
	for i, v := range b.sqlParts {
		if v.getPartEnum() == e {
			p = v
			r = i
			break
		}
	}
	/*if r > -1 {
		b.sqlParts = append(b.sqlParts[:r], b.sqlParts[r+1:]...)
	}*/

	return r > -1, p
}

func (b *Builder) removePart(e partEnum) bool {
	r := -1
	for i, v := range b.sqlParts {
		if v.getPartEnum() == e {
			r = i
			break
		}
	}

	if r > -1 {
		b.sqlParts = append(b.sqlParts[:r], b.sqlParts[r+1:]...)
	}

	return r > -1
}

func (b *Builder) add(p part) {
	//b.init()
	ok, tmp := b.getPart(p.getPartEnum())
	enum := p.getPartEnum()
	switch enum {
	case selectPartEnum:
		tmp = p
		break
	case fromPartEnum, insertPartEnum:
		if !ok {
			tmp = p
		} else {
			f := tmp.(fromPartSQL)
			f.parts = append(f.parts, p.(fromPartSQL).parts...)
			tmp = f
		}
		break
	case joinPartEnum:
		if tmp == nil {
			tmp = p
		} else {
			j := tmp.(joinPartSQL)
			j.parts = append(j.parts, p.(joinPartSQL).parts...)
			tmp = j
		}
		break
	case wherePartEnum:
		if !ok {
			tmp = p
		} else {
			j := tmp.(partSQL)
			j.parts = append(j.parts, p.(partSQL).parts...)
			tmp = j
		}
		break
	case orderByASCPartEnum:
		if tmp == nil {
			tmp = p
		} else {
			j := tmp.(partSQL)
			j.parts = append(j.parts, strings.Join(p.(partSQL).parts, ","))
			tmp = j
		}
		break
	case orderByDESCPartEnum:
		if !ok {
			tmp = p
		} else {
			j := tmp.(partSQL)
			j.parts = append(j.parts, strings.Join(p.(partSQL).parts, ","))
			tmp = j
		}
		break
	case columnsPartEnum:
		if !ok {
			tmp = p
		} else {
			j := tmp.(insertValuePartSQL)
			j.parts = append(j.parts, p.(insertValuePartSQL).parts...)
			tmp = j
		}
		break

	}

	b.sqlParts = append(b.sqlParts, tmp)
}

func (b *Builder) addJoin(e joinEnum, f, j, a, c string) {
	p := joinPartSQL{part: joinPartEnum}
	if p.parts == nil {
		p.parts = make([]join, 1)
	}
	p.parts[0] = join{join: e, fromAlias: f, joinTable: j, joinAlias: a, joinCondition: c}
	b.add(p)
}

func (b *Builder) getSQLForSelect() string {

	var q string

	if ok, bSelect := b.getPart(selectPartEnum); ok {
		q = "SELECT " + strings.Join(bSelect.(partSQL).parts, ", ") + " "
	}

	if ok, bFrom := b.getPart(fromPartEnum); ok {
		q += "FROM " + bFrom.(fromPartSQL).getFrom() + " "
	}

	if ok, bJoin := b.getPart(joinPartEnum); ok {
		q += bJoin.(joinPartSQL).getJoin() + " "
	}

	if ok, bWhere := b.getPart(wherePartEnum); ok {
		q += "WHERE " + bWhere.(partSQL).getWhere()
	}

	existOrder := false

	if ok, bOrder := b.getPart(orderByASCPartEnum); ok {
		q += "ORDER BY " + bOrder.(partSQL).getOrderBy() + " ASC "
		existOrder = true
	}

	if ok, bOrder := b.getPart(orderByDESCPartEnum); ok {
		if existOrder {
			q += ", "
		} else {
			q += "ORDER BY "
		}
		q += bOrder.(partSQL).getOrderBy() + " DESC "
	}

	return q

}

func (b *Builder) getSQLForInsert() string {

	q := "INSERT INTO "

	if ok, bFrom := b.getPart(insertPartEnum); ok {
		q += bFrom.(fromPartSQL).getFrom() + " "
	}

	if ok, bSelect := b.getPart(columnsPartEnum); ok {
		cols := make([]string, len(bSelect.(insertValuePartSQL).parts))
		vals := make([]string, len(cols))
		for i, o := range bSelect.(insertValuePartSQL).parts {
			cols[i] = o.name
			vals[i] = o.parameter
		}
		q += "(" + strings.Join(cols, ", ") + ") "
		q += "VALUES (" + strings.Join(vals, ", ") + ") "
	}

	return q

}

func (b *Builder) getSQLForUpdate() string {

	q := "UPDATE "

	if ok, bFrom := b.getPart(fromPartEnum); ok {
		q += bFrom.(fromPartSQL).getFrom() + " SET "
	}

	if ok, bSelect := b.getPart(columnsPartEnum); ok {
		p := make([]string, len(bSelect.(insertValuePartSQL).parts))
		for i, o := range bSelect.(insertValuePartSQL).parts {
			p[i] = o.name + " = " + o.parameter
		}
		q += strings.Join(p, ", ") + " "
	}

	if ok, bWhere := b.getPart(wherePartEnum); ok {
		q += "WHERE " + bWhere.(partSQL).getWhere() + " "
	}

	return q

}