package dal

/**
DELETE Section
 */

type DeleteBuilder struct {
	b *Builder
}

func (b *DeleteBuilder) Where(condition string) *DeleteBuilder {
	return b.addWhere("AND", condition)
}

func (b *DeleteBuilder) OrWhere(condition string) *DeleteBuilder {
	return b.addWhere("OR", condition)
}

func (b *DeleteBuilder) Build() {
	b.b.Build()
}

func (b *DeleteBuilder) GetSQL() (q string) {

	q = "DELETE FROM "

	if ok, bFrom := b.b.getPart(tablePartEnum); ok {
		q += bFrom.getSQL() + " "
	}

	if ok, bWhere := b.b.getPart(wherePartEnum); ok {
		q += bWhere.getSQL()
	}

	return
}

func (b *DeleteBuilder) SetParameter(p, v interface{}) *DeleteBuilder {
	b.b.SetParameter(p, v)

	return b
}

/**
PRIVATE methods
 */

func (b *DeleteBuilder) addWhere(conditiontype, condition string) *DeleteBuilder {
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
