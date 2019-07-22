package dal

type SQLBuilder struct {
	b *Builder
}

func (b *SQLBuilder) Build() error {
	_, err := b.b.Build()
	return err
}

func (b *SQLBuilder) GetSQL() (q string) {

	if ok, bSelect := b.b.getPart(tablePartEnum); ok {
		q += bSelect.getSQL()
	}

	return
}

func (b *SQLBuilder) SetParameter(p, v interface{}) *SQLBuilder {
	b.b.SetParameter(p, v)

	return b
}
