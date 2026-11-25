package db

import (
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // driver SQLite via cgo (precisa de um compilador C)
)

// InDiskEngine guarda os pares chave/valor numa tabela SQLite. Diferente do
// InMemEngine, não há MemTable nem SSTables: toda escrita vai direto pro disco
// e a durabilidade, a ordenação e a compactação ficam por conta do SQLite.
type InDiskEngine struct {
	db *sql.DB
}

// NewInDiskEngine abre (ou cria) o banco SQLite no caminho indicado.
func NewInDiskEngine(path string) (*InDiskEngine, error) {
	// WAL deixa leitores e o escritor trabalharem ao mesmo tempo; o busy_timeout
	// faz uma escrita concorrente esperar o lock em vez de falhar com SQLITE_BUSY.
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000&_synchronous=NORMAL", path)
	conn, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("abrindo sqlite %s: %w", path, err)
	}

	_, err = conn.Exec(`CREATE TABLE IF NOT EXISTS kv (
		key   BLOB PRIMARY KEY,
		value BLOB NOT NULL
	) WITHOUT ROWID`)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("criando tabela kv: %w", err)
	}

	return &InDiskEngine{db: conn}, nil
}

func (e *InDiskEngine) Put(key []byte, value []byte) error {
	// nil viraria NULL e violaria o NOT NULL; valor vazio é um valor válido.
	if value == nil {
		value = []byte{}
	}
	_, err := e.db.Exec(
		`INSERT INTO kv (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value,
	)
	return err
}

func (e *InDiskEngine) Get(key []byte) ([]byte, error) {
	var value []byte
	err := e.db.QueryRow(`SELECT value FROM kv WHERE key = ?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

// Delete remove a chave. Assim como no InMemEngine, apagar uma chave que não
// existe não é erro.
func (e *InDiskEngine) Delete(key []byte) error {
	_, err := e.db.Exec(`DELETE FROM kv WHERE key = ?`, key)
	return err
}

// Close fecha a conexão com o arquivo do banco.
func (e *InDiskEngine) Close() error {
	return e.db.Close()
}
