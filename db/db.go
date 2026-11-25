package db

// Adicionado para o bytes.Compare

// Adicionado para o os.Remove

type DBEngine interface {
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
}
