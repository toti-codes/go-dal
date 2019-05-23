package dal

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type IBuilder interface {
	GetSQL() string
}

//Builder -
type Builder struct {
	b           IBuilder
	sqlType     sqlEnum
	sql         string
	sqlParts    []part
	params      map[interface{}]interface{}
	finalParams []interface{}
}

func NewBuilder() *Builder {
	b := Builder{}
	if b.sqlParts == nil {
		b.sqlParts = make([]part, 0)
	}
	return &b
}

//Select -
func (b *Builder) Select(columns ...string) *SelectBuilder {
	sb := SelectBuilder{b: b}
	b.b = &sb
	if len(columns) > 0 {
		b.sqlParts = append(b.sqlParts, partSQL{part: selectPartEnum, parts: columns})
	}

	return &sb
}

func (b *Builder) Insert(table string) *InsertBuilder {
	sb := InsertBuilder{b: b}
	b.b = &sb
	b.sqlParts = []part{partSQL{part: tablePartEnum, parts: []string{table}}}

	return &sb
}

func (b *Builder) Update(table string) *UpdateBuilder {
	sb := UpdateBuilder{b: b}
	b.b = &sb
	b.sqlParts = []part{partSQL{part: tablePartEnum, parts: []string{table}}}

	return &sb
}

func (b *Builder) Delete(table string) *DeleteBuilder {
	sb := DeleteBuilder{b: b}
	b.b = &sb
	b.sqlParts = []part{partSQL{part: tablePartEnum, parts: []string{table}}}

	return &sb
}

func (b *Builder) SQL(sql string) *SQLBuilder {
	sb := SQLBuilder{b: b}
	b.b = &sb

	b.sqlParts = []part{partSQL{part: tablePartEnum, parts: []string{sql}}}

	return &sb
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
	return b.finalParams
}

func (b *Builder) Build() (*Builder, error) {
	if b.sql != "" {
		return nil, fmt.Errorf("Query was already build")
	}

	sql := b.b.GetSQL()

	sql, err := b.build(sql)

	if err != nil {
		return b, err
	}

	b.sql = sql

	return b, nil
}

//GetSQL -
func (b *Builder) GetSQL() string {
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

func getPlaceHolder(consecutive int) string {

	return "$" + strconv.Itoa(consecutive)

}

func (b *Builder) build(sql string) (string, error) {

	b.finalParams = make([]interface{}, 0)

	r, _ := regexp.Compile("\\?|[^:][:][a-zA-Z_\\-]+")

	matches := r.FindAllStringSubmatchIndex(sql, -1)

	var iParam, sParam int

	for i, v := range matches {
		param := "?"
		if v[1]-1 == v[0] {
			if _, ok := b.params[iParam]; !ok {
				error := ""
				if iParam == 0 {
					if _, ok := b.params[1]; !ok {
						error = "The first index param must be 0 or 1"
					}
				} else {
					error = "Can not find parameter with index " + strconv.Itoa(iParam)
				}

				if error != "" {
					return "", fmt.Errorf(error)
				}
			}
			b.finalParams = append(b.finalParams, b.params[iParam])

			iParam++
		} else {
			params := r.FindAllString(sql, 1)
			param = params[0]
			r1, _ := regexp.Compile("[:][a-zA-Z_\\-]+")
			param = r1.FindAllString(param, 1)[0]
			paramName := strings.Replace(param, ":", "", 1)
			if _, ok := b.params[paramName]; !ok {
				return "", fmt.Errorf("can not find parameter with name %s", params[i])
			}

			b.finalParams = append(b.finalParams, b.params[paramName])

			sParam++
		}

		sql = strings.Replace(sql, param, getPlaceHolder(iParam+sParam), 1)
	}

	return sql, nil
}
