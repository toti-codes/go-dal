package dal

import (
	"strings"
)

/**
INSERT Section
 */

type InsertBuilder struct {
	b *Builder
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

	return
}

func (b *InsertBuilder) SetParameter(p, v interface{}) *InsertBuilder {
	b.b.SetParameter(p, v)

	return b
}
