package dal

import "strconv"

/**
SELECT Section
 */

type SelectBuilder struct {
	b *Builder
	isDistinct bool
	firstResult int64
	maxResults int64
}

func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.isDistinct = true

	return b
}

//From -
func (b *SelectBuilder) From(table string) *SelectBuilder {
	p := partSQL{part: fromPartEnum}

	if ok, part := b.b.getPart(fromPartEnum); ok {
		parts := part.(partSQL).parts
		p.parts = append(parts, table)

		b.b.removePart(fromPartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		p.parts = []string{table}
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

//InnerJoin -
func (b *SelectBuilder) InnerJoin(join Join) *SelectBuilder {
	b.addJoin(inner, join)

	return b
}

//LeftJoin -
func (b *SelectBuilder) LeftJoin(join Join) *SelectBuilder {
	b.addJoin(left, join)

	return b
}

//RightJoin -
func (b *SelectBuilder) RightJoin(join Join) *SelectBuilder {
	return b.addJoin(right, join)
}

func (b *SelectBuilder) Where(condition string) *SelectBuilder {
	return b.addWhere("AND", condition)
}

func (b *SelectBuilder) OrWhere(condition string) *SelectBuilder {
	return b.addWhere("OR", condition)
}

func (b *SelectBuilder) GroupBy(columns ...string) *SelectBuilder {

	p := partSQL{part: groupPartEnum}

	if ok, part := b.b.getPart(groupPartEnum); ok {
		parts := part.(partSQL).parts
		p.parts = append(parts, columns...)

		b.b.removePart(groupPartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		p.parts = columns
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

func (b *SelectBuilder) Having(condition string) *SelectBuilder {
	p := partSQL{part: havingPartEnum}

	if ok, _ := b.b.getPart(havingPartEnum); ok {
		b.b.removePart(havingPartEnum)
	}

	p.parts = []string{condition}
	b.b.sqlParts = append(b.b.sqlParts, p)

	return b
}

func (b *SelectBuilder) OrderASC(columns ...string) *SelectBuilder {
	return b.addOrder(asc, columns)
}

func (b *SelectBuilder) OrderDESC(columns ...string) *SelectBuilder {
	return b.addOrder(desc, columns)
}

func (b *SelectBuilder) FirstResult(firstResult int64) *SelectBuilder {
	b.firstResult = firstResult

	return b
}

func (b *SelectBuilder) MaxResult(maxResults int64) *SelectBuilder {
	b.maxResults = maxResults

	return b
}

func (b *SelectBuilder) Build() {
	b.b.Build()
}

func (b *SelectBuilder) GetBuilder() *Builder {
	return b.b
}

func (b *SelectBuilder) GetSQL() (q string) {

	q = "SELECT "

	if b.isDistinct {
		q += "DISTINCT "
	}

	if ok, bSelect := b.b.getPart(selectPartEnum); ok {
		q += bSelect.getSQL()
	}

	if ok, bFrom := b.b.getPart(fromPartEnum); ok {
		q += bFrom.getSQL()
	}

	if ok, bJoin := b.b.getPart(joinPartEnum); ok {
		q += bJoin.(joinPartSQL).getSQL()
	}

	if ok, bWhere := b.b.getPart(wherePartEnum); ok {
		q += bWhere.(wherePartSQL).getSQL()
	}

	if ok, bGroup := b.b.getPart(groupPartEnum); ok {
		q += bGroup.getSQL()
	}

	if ok, bHaving := b.b.getPart(havingPartEnum); ok {
		q += bHaving.getSQL()
	}

	if ok, bJoin := b.b.getPart(orderByPartEnum); ok {
		q += bJoin.(orderPartSQL).getSQL()
	}

	if b.isLimitQuery() {
		q += " LIMIT " + strconv.Itoa(int(b.maxResults))
		if b.firstResult > 0 {
			q += " OFFSET " + strconv.Itoa(int(b.firstResult) - 1)
		}
	}

	return
}

func (b *SelectBuilder) GetCountSQL() (q string) {

	q = "SELECT "

	if b.isDistinct {
		q += "DISTINCT "
	}

	q += "COUNT(1) "

	if ok, bFrom := b.b.getPart(fromPartEnum); ok {
		q += bFrom.getSQL()
	}

	if ok, bJoin := b.b.getPart(joinPartEnum); ok {
		q += bJoin.(joinPartSQL).getSQL()
	}

	if ok, bWhere := b.b.getPart(wherePartEnum); ok {
		q += bWhere.(wherePartSQL).getSQL()
	}

	if ok, bGroup := b.b.getPart(groupPartEnum); ok {
		q += bGroup.getSQL()
	}

	if ok, bHaving := b.b.getPart(havingPartEnum); ok {
		q += bHaving.getSQL()
	}

	q, _ = b.b.build(q)

	return
}

func (b *SelectBuilder) SetParameter(p, v interface{}) *SelectBuilder {
	b.b.SetParameter(p, v)

	return b
}

/**
PRIVATE methods
 */

func (b *SelectBuilder) addJoin(e joinEnum, join Join) *SelectBuilder {
	p := joinPartSQL{}
	if p.parts == nil {
		p.parts = make([]joinContainer, 1)
	}
	p.parts[0] = joinContainer{join:e, Join: &join}

	if ok, part := b.b.getPart(joinPartEnum); ok {
		parts := part.(joinPartSQL).parts
		p.parts = append(parts, joinContainer{join:e, Join: &join})

		b.b.removePart(joinPartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

func (b *SelectBuilder) addOrder(o orderEnum, columns []string) *SelectBuilder {
	p := orderPartSQL{}
	if p.parts == nil {
		p.parts = make([]orderContainer, 1)
	}
	p.parts[0] = orderContainer{order:o, Order: &Order{columns: columns}}

	if ok, part := b.b.getPart(orderByPartEnum); ok {
		parts := part.(orderPartSQL).parts
		exist := false
		for _, c := range parts {
			if c.order == o {
				exist = true
				c.columns = append(c.columns, columns...)
			}
		}
		if !exist {
			p.parts = append(parts, orderContainer{order: o, Order: &Order{columns: columns}})

			b.b.removePart(orderByPartEnum)
			b.b.sqlParts = append(b.b.sqlParts, p)
		}
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}

func (b *SelectBuilder) addWhere(conditiontype, condition string) *SelectBuilder {
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

func (b *SelectBuilder) isLimitQuery() bool {
	return b.firstResult > 0 || b.maxResults > 0
}