package dal

import (
	"strings"
)

const (
	inner joinEnum = "INNER"
	left  joinEnum = "LEFT"
	right joinEnum = "RIGHT"
)

const (
	asc orderEnum = "ASC"
	desc orderEnum = "DESC"
)

const (
	selectPartEnum      partEnum = 0
	fromPartEnum        partEnum = 1
	tablePartEnum       partEnum = 2
	joinPartEnum        partEnum = 3
	wherePartEnum       partEnum = 4
	groupPartEnum		partEnum = 5
	havingPartEnum		partEnum = 6
	orderByPartEnum  	partEnum = 7

	insertPartEnum  partEnum = 10
	columnsPartEnum partEnum = 11
)

type sqlEnum int

type joinEnum string

type orderEnum string

type partEnum int

type part interface {
	getPartEnum() partEnum
	getSQL() string
}

type partSQL struct {
	part  partEnum
	parts []string
}

func (p partSQL) getPartEnum() partEnum {
	return p.part
}

func (p partSQL) getSQL() string {
	if p.part == selectPartEnum {
		return strings.Join(p.parts, ", ")
	} else if p.part == fromPartEnum {
		return " FROM " + strings.Join(p.parts, ", ")
	} else if p.part == groupPartEnum {
		return " GROUP BY " + strings.Join(p.parts, ", ")
	} else if p.part == havingPartEnum {
		return " HAVING " + strings.Join(p.parts, ", ")
	} else if p.part == insertPartEnum {
		return strings.Join(p.parts, ", ")
	} else if p.part == tablePartEnum {
		return p.parts[0]
	}

	return ""
}

type Join struct {
	fromAlias, joinTable, joinCondition string
}

type joinContainer struct {
	*Join
	join joinEnum
}

type joinPartSQL struct {
	parts []joinContainer
}

func (p joinPartSQL) getPartEnum() partEnum {
	return joinPartEnum
}

func (p joinPartSQL) getSQL() (join string) {
	for _, v := range p.parts {
		join += " " + string(v.join) + " JOIN " + v.joinTable
		if v.joinCondition != "" {
			join += " ON " + v.joinCondition
		}
	}
	return
}

type Order struct {
	columns []string
}

type orderContainer struct {
	*Order
	order orderEnum
}

type orderPartSQL struct {
	parts []orderContainer
}

func (p orderPartSQL) getPartEnum() partEnum {
	return orderByPartEnum
}

func (p orderPartSQL) getSQL() (order string) {
	order += " ORDER BY "

	for i, c := range p.parts {
		if i > 0 {
			order += ", "
		}
		order += strings.Join(c.columns, ", ") + " " + string(c.order)
	}

	return
}

type Where struct {
	condition string
}

type whereContainer struct {
	*Where
	conditionType string
}

type wherePartSQL struct {
	parts []whereContainer
}

func (p wherePartSQL) getPartEnum() partEnum {
	return wherePartEnum
}

func (p wherePartSQL) getSQL() (order string) {
	order += " WHERE "
	if len(p.parts) > 1 {
		order += "("
	}

	for i, c := range p.parts {
		if i > 0 {
			order += " " + c.conditionType + " "
		}
		order += "(" + c.condition + ")"
	}

	if len(p.parts) > 1 {
		order += ")"
	}

	return
}

type columnSQL struct {
	name, parameter string
}

type columnPartSQL struct {
	part  partEnum
	parts []columnSQL
}

func (p columnPartSQL) getPartEnum() partEnum {
	return p.part
}

func (p columnPartSQL) getSQL() (order string) {
	return ""
}
