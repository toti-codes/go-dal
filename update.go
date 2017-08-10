package dal

import "strings"

/**
UPDATE Section
 */

type UpdateBuilder struct {
	b *Builder
}

func (b *UpdateBuilder) Set(column, parameter string) *UpdateBuilder {
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

func (b *UpdateBuilder) Sets(columns ...string) *UpdateBuilder {
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

func (b *UpdateBuilder) Where(condition string) *UpdateBuilder {
	return b.addWhere("AND", condition)
}

func (b *UpdateBuilder) OrWhere(condition string) *UpdateBuilder {
	return b.addWhere("OR", condition)
}

func (b *UpdateBuilder) Build() {
	b.b.Build()
}

func (b *UpdateBuilder) GetSQL() (q string) {

	q = "UPDATE "

	if ok, bFrom := b.b.getPart(tablePartEnum); ok {
		q += bFrom.getSQL() + " SET "
	}

	if ok, bSelect := b.b.getPart(columnsPartEnum); ok {
		p := make([]string, len(bSelect.(columnPartSQL).parts))
		for i, o := range bSelect.(columnPartSQL).parts {
			p[i] = o.name + " = " + o.parameter
		}
		q += strings.Join(p, ", ")
	}

	if ok, bWhere := b.b.getPart(wherePartEnum); ok {
		q += bWhere.getSQL()
	}

	return
}

func (b *UpdateBuilder) SetParameter(p, v interface{}) *UpdateBuilder {
	b.b.SetParameter(p, v)

	return b
}

/**
PRIVATE methods
 */

func (b *UpdateBuilder) addWhere(conditiontype, condition string) *UpdateBuilder {
	p := wherePartSQL{}
	if p.parts == nil {
		p.parts = make([]whereContainer, 1)
	}
	p.parts[0] = whereContainer{conditionType:conditiontype, Where: &Where{condition:condition}}

	if ok, part := b.b.getPart(wherePartEnum); ok {
		parts := part.(wherePartSQL).parts
		p.parts = append(parts, whereContainer{conditionType:conditiontype, Where: &Where{condition:condition}})

		b.b.removePart(wherePartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}