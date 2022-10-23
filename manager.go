package dal

import (
	"database/sql"
	"fmt"
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/postgres"
	_ "github.com/lib/pq"
	"strconv"
	"sync"
)

const UNIQUE_CONNECTION = "DAL_UNIQUE"

var once sync.Once
var instance *connectionManager

type connectionManager struct {
	configured  bool
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

	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s", config["host"], config["port"], config["database"], config["user"], config["password"], config["ssl"])

	var conn *sql.DB
	var err error

	if driver, b := config["driver"]; b {
		conn, err = sql.Open(driver, dsn)
	} else {
		conn, err = sql.Open("postgres", dsn)
	}

	if err != nil {
		return err
	}

	if val, ok := config["maxOpenConns"]; ok {
		if max, err := strconv.Atoi(val); err != nil {
			conn.SetMaxOpenConns(max)
		}
	}

	if val, ok := config["maxIdleConns"]; ok {
		if max, err := strconv.Atoi(val); err != nil {
			conn.SetMaxIdleConns(max)
		}
	}

	err = conn.Ping()

	if err != nil {
		return err
	}

	if _, ok := m.connections[name]; !ok {
		m.connections[name] = &Connection{db: conn}
	}

	m.configured = true

	m.Unlock()

	return nil

}
