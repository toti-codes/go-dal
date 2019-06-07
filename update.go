package dal

import (
	"encoding/json"
	"reflect"
	"strings"
)

/**
UPDATE Section
*/

type UpdateBuilder struct {
	b *Builder
}

func (b *UpdateBuilder) Set(column, parameter string) *UpdateBuilder {
	p := columnPartSQL{part: columnsPartEnum}

	if p.parts == nil {
		p.parts = make([]columnSQL, 1)
	}
	p.parts[0] = columnSQL{name: column, parameter: parameter}

	if ok, part := b.b.getPart(columnsPartEnum); ok {
		parts := part.(columnPartSQL).parts
		p.parts = append(parts, columnSQL{name: column, parameter: parameter})

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

	p := columnPartSQL{part: columnsPartEnum}

	p.parts = make([]columnSQL, len(columns))
	for c, i := range columns {
		p.parts[c] = columnSQL{name: i, parameter: "?"}
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

func (b *UpdateBuilder) Type(entity interface{}) *UpdateBuilder {
	e := reflect.ValueOf(entity)
	var columnNames []string

	count := 0
	var id interface{}
	for i := 0; i < e.NumField(); i++ {
		dbConfig := strings.Replace(e.Type().Field(i).Tag.Get("db"), " ", "", -1)
		columnsConfig := strings.Split(dbConfig, ",")
		columnName := columnsConfig[0]
		value := e.Field(i).Interface()

		config := make(map[string]bool)
		for _, v := range columnsConfig {
			config[v] = true
		}

		if value == nil || config["autoincrement"] || config["omitted"] {
			continue
		}

		if columnName == "" {
			columnName = e.Type().Field(i).Name
		}

		if columnName == "id" {
			value = int64(value.(int64))
			id = value
		}

		if config["json"] || config["jsonb"] {
			if reflect.ValueOf(value).IsNil() {
				continue
			}
			if byts, err := json.Marshal(value); err != nil {
				panic("la columna: " + columnName + " no contiene un valor válido")
			} else {
				value = string(byts)
			}
		}

		columnNames = append(columnNames, columnName)
		b.SetParameter(count, value)
		count += 1
	}

	b.Sets(columnNames...)

	b.Where("id = ?")
	b.SetParameter(count, id)

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

func (b *UpdateBuilder) GetBuilder() *Builder {
	return b.b
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
	p.parts[0] = whereContainer{conditionType: conditiontype, Where: &Where{condition: condition}}

	if ok, part := b.b.getPart(wherePartEnum); ok {
		parts := part.(wherePartSQL).parts
		p.parts = append(parts, whereContainer{conditionType: conditiontype, Where: &Where{condition: condition}})

		b.b.removePart(wherePartEnum)
		b.b.sqlParts = append(b.b.sqlParts, p)
	} else {
		b.b.sqlParts = append(b.b.sqlParts, p)
	}

	return b
}
