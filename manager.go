package dal

import (
	"fmt"
	"sync"
	"database/sql"
	_ "github.com/lib/pq"
)

const UNIQUE_CONNECTION = "DAL_UNIQUE"

var once sync.Once
var instance *connectionManager

type connectionManager struct {
	configured bool
	connections map[string]*Connection
	sync.Mutex
}

func GetConnectionManager() *connectionManager {
	once.Do(func() {
		instance = &connectionManager{}
		instance.connections = make(map[string]*Connection)
	})
	return instance
}

func (m *connectionManager) AddSingleDB(c map[string]string) error {
	return m.configure(UNIQUE_CONNECTION, c)
}

func (m *connectionManager) AddDB(name string, c map[string]string) error {
	return m.configure(name, c)
}

func (m *connectionManager) GetSingleConnection() *Connection {
	return m.connections[UNIQUE_CONNECTION]
}

func (m *connectionManager) GetConnection(name string) *Connection {
	return m.connections[name]
}

func (m *connectionManager) GetSession() *Session {
	return m.connections[UNIQUE_CONNECTION].GetSession()
}

func (m *connectionManager) GetTransaction() (*Transaction, error) {
	return m.connections[UNIQUE_CONNECTION].GetTransaction()
}

func (m *connectionManager) configure(name string, config map[string]string) error {

	m.Lock()
	
	conn, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s", config["host"], config["port"], config["database"], config["user"], config["password"], config["ssl"]))

	if err != nil {
		return err
	}

	//db.SetMaxOpenConns(10)
	//db.SetMaxIdleConns(5)
	err = conn.Ping()

	if err != nil {
		return err
	}

	if _, ok := m.connections[name]; !ok {
		m.connections[name] = &Connection{db:conn}
	}

	m.configured = true

	m.Unlock()

	return nil

}

