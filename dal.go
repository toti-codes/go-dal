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
	Exec(b Builder) interface{}
	Query(b Builder) []map[string]interface{}
	QueryType(b Builder, t interface{})
}

type handlerConn interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
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

/**
	SCAN
 */

func (s *Session) Scan(b Builder, v ...interface{}) error {
	return scan(s.handler, b, v...)
}

func (t *Transaction) Scan(b Builder, v ...interface{}) error {
	return scan(t.handler, b, v...)
}

/**
	QUERY
 */

func (s *Session) Query(b Builder) ([]map[string]interface{}, error) {
	return query(s.handler, b)
}

func (t *Transaction) Query(b Builder) ([]map[string]interface{}, error) {
	return query(t.handler, b)
}

func (s *Session) QueryArray(b Builder) ([][]interface{}, error) {
	return queryArray(s.handler, b)
}

func (t *Transaction) QueryArray(b Builder) ([][]interface{}, error) {
	return queryArray(t.handler, b)
}

func (s *Session) QueryType(b Builder, o interface{}) error {
	return queryType(s.handler, b, o)
}

func (t *Transaction) QueryType(b Builder, o interface{}) error {
	return queryType(t.handler, b, o)
}

/**
	FIRST
 */

func (s *Session) FirstResult(b Builder) (map[string]interface{}, error) {
	return firstResult(s.handler, b)
}

func (t *Transaction) FirstResult(b Builder) (map[string]interface{}, error) {
	return firstResult(t.handler, b)
}

func (s *Session) FirstResultArray(b Builder) ([]interface{}, error) {
	return firstResultArray(s.handler, b)
}

func (t *Transaction) FirstResultArray(b Builder) ([]interface{}, error) {
	return firstResultArray(t.handler, b)
}

func (s *Session) FirstResultType(b Builder, o interface{}) error {
	return firstResultType(s.handler, b, o)
}

func (t *Transaction) FirstResultType(b Builder, o interface{}) error {
	return firstResultType(t.handler, b, o)
}

/**
	Exec
 */

func (s *Session) Exec(b Builder) error {
	return exec(s.handler, b)
}

func (t *Transaction) Exec(b Builder) error {
	return exec(t.handler, b)
}

/**
	Private Methods
 */

func scan(handler handlerConn, b Builder, v ...interface{}) error {
	return handler.QueryRow(b.GetSQL(), b.GetParameters()...).Scan(v...)
}

func query(handler handlerConn, b Builder) ([]map[string]interface{}, error) {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

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

func queryArray(handler handlerConn, b Builder) ([][]interface{}, error) {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

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

	result := make([][]interface{}, 0)

	for rows.Next() {
		record := make([]interface{}, 1)

		rows.Scan(scanArgs...)
		for i, _ := range columns {
			record[i] = values[i]
		}

		result = append(result, record)
	}

	return result, nil
}

func queryType(handler handlerConn, b Builder, d interface{}) error {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

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

func exec(handler handlerConn, b Builder) (err error) {
	_, err = handler.Exec(b.GetSQL(), b.GetParameters()...)

	return
}

func firstResult(handler handlerConn, b Builder) (map[string]interface{}, error) {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

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

	if rows.Next() {
		result := make(map[string]interface{})

		rows.Scan(scanArgs...)
		for i, colName := range columns {
			result[string(colName)] = values[i]
		}

		return result, nil
	} else {
		return nil, nil
	}

}

func firstResultArray(handler handlerConn, b Builder) ([]interface{}, error) {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

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

	if rows.Next() {
		result := make([]interface{}, 1)

		rows.Scan(scanArgs...)
		for i := range columns {
			result[i] = values[i]
		}
		return result, nil
	} else {
		return nil, nil
	}

}

func firstResultType(handler handlerConn, b Builder, d interface{}) error {
	rows, err := handler.Query(b.GetSQL(), b.GetParameters()...)

	if err != nil {
		return err
	}

	defer rows.Close()

	_, err = LoadOne(rows, d)

	if err != nil {
		return err
	}

	return nil
}