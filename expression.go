package dal

import "strings"

func As(name, alias string) string {
	return name + " " + alias
}

func And(exp ...string) (and string) {
	if len(exp) > 1 {
		and += "("
	}

	and += strings.Join(exp, " AND ")

	if len(exp) > 1 {
		and += ")"
	}
	return
}

func Or(exp ...string) (or string) {
	if len(exp) > 1 {
		or += "("
	}

	or += strings.Join(exp, " OR ")

	if len(exp) > 1 {
		or += ")"
	}
	return
}

func Eq(column, placeholder string) string {
	return column + " = " + placeholder
}

func Neq(column, placeholder string) string {
	return column + " != " + placeholder
}

func Gt(column, placeholder string) string {
	return column + " > " + placeholder
}

func Gte(column, placeholder string) string {
	return column + " >= " + placeholder
}

func Lt(column, placeholder string) string {
	return column + " < " + placeholder
}

func Lte(column, placeholder string) string {
	return column + " <= " + placeholder
}