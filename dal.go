package dal

import (
	"database/sql"
)

var (
	_ SessionHandler = (*Transaction)(nil)
	_ SessionHandler = (*Session)(nil)
)

type SessionHandler interface { }

type Connection struct {
	db *sql.DB
}

type base interface {
	Query(b Builder) []map[string]interface{}
	QueryType(b Builder, t interface{})
}

type handlerConn interface {
	//Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type Session struct {
	*Connection
	handler handlerConn
}

type Transaction struct {
	tx *sql.Tx
	handler handlerConn
}

func (c *Connection) GetSession() *Session {
	return &Session{Connection: c, handler: c.db}
}

func (c *Connection) GetTransaction() (*Transaction, error) {
	tx, err := c.db.Begin()

	if err != nil {
		return nil, err
	}

	return &Transaction{tx: tx, handler: tx}, nil
}

func (tx *Transaction) Commit() error {
	err := tx.tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (tx *Transaction) Rollback() error {
	err := tx.tx.Rollback()
	if err != nil && err != sql.ErrTxDone {
		return err
	}

	return nil
}

func (s *Session) Query(b Builder) ([]map[string]interface{}, error) {
	return query(s.handler, b)
}

func (t *Transaction) Query(b Builder) ([]map[string]interface{}, error) {
	return query(t.handler, b)
}

func (s *Session) QueryType(b Builder, o interface{}) error {
	return queryType(s.handler, b, o)
}

func (t *Transaction) QueryType(b Builder, o interface{}) error {
	return queryType(t.handler, b, o)
}

func query(handler handlerConn, b Builder) ([]map[string]interface{}, error) {
	rows, err := handler.Query(b.GetSQL())

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	columns, _ := rows.Columns()

	values := make([]interface{}, len(columns))
	scanArgs := make([]interface{}, len(values))
	for j := range values {
		scanArgs[j] = &values[j]
	}

	result := []map[string]interface{}{}

	for rows.Next() {
		record := make(map[string]interface{})

		rows.Scan(scanArgs...)
		for i, colName := range columns {
			record[string(colName)] = values[i]
		}

		result = append(result, record)
	}

	return result, nil
}

func queryType(handler handlerConn, b Builder, d interface{}) error {
	rows, err := handler.Query(b.GetSQL())

	if err != nil {
		return err
	}

	defer rows.Close()

	_, err = Load(rows, d)

	if err != nil {
		return err
	}

	return nil
}