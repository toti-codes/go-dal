package dal

import "strings"

const (
	selectEnum sqlEnum = 0
	insertEnum sqlEnum = 1
	updateEnum sqlEnum = 2
	deleteEnum sqlEnum = 3
)

const (
	inner joinEnum = "INNER"
	left joinEnum = "LEFT"
	right joinEnum = "RIGHT"
)

const (
	stateDirty stateEnum = 0
	stateClean stateEnum = 1
)

const (
	selectPartEnum partEnum = 0
	fromPartEnum partEnum = 1
	wherePartEnum partEnum = 2
	joinPartEnum partEnum = 3
	orderByASCPartEnum partEnum = 4
	orderByDESCPartEnum partEnum = 5

	insertPartEnum partEnum = 6
	columnsPartEnum partEnum = 7
)

type sqlEnum int

type stateEnum int

type joinEnum string

type partEnum int

type part interface{
	getPartEnum() partEnum
}

type partSQL struct {
	part partEnum
	parts []string
}

type fromSQL struct {
	name, alias string
}

type fromPartSQL struct {
	part partEnum
	parts []string
}

type join struct {
	join joinEnum
	fromAlias, joinTable, joinAlias, joinCondition string
}

type joinPartSQL struct {
	part partEnum
	parts []join
}

type insertValueSQL struct {
	name, parameter string
}

type insertValuePartSQL struct {
	part partEnum
	parts []insertValueSQL
}

func (p partSQL) getPartEnum() partEnum {
	return p.part
}

func (p fromPartSQL) getPartEnum() partEnum {
	return p.part
}

func (p joinPartSQL) getPartEnum() partEnum {
	return p.part
}

func (p insertValuePartSQL) getPartEnum() partEnum {
	return p.part
}

func (p fromPartSQL) getFrom() string {
	return strings.Join(p.parts, ",")
}

func (p partSQL) getWhere() (where string) {
	for _, v := range p.parts {
		where += v + " "
	}
	return
}

func (p joinPartSQL) getJoin() (join string) {
	for _, v := range p.parts {
		join += string(v.join) + " JOIN " + v.joinTable + " " + v.joinAlias + " "
		if v.joinCondition != "" {
			join += "ON " + v.joinCondition + " "
		}
	}
	return
}

func (p partSQL) getOrderBy() string {
	return strings.Join(p.parts, ", ")
}