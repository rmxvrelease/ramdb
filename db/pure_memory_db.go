package db

// PureMemEngine guarda todos os pares chave/valor num HAMT na RAM. Diferente do
// InMemEngine, não há limite, flush nem SSTables: nenhuma leitura toca o disco, e
// tudo se perde quando o processo termina. O HAMT já tem um mutex por shard, então
// o motor não precisa de outro.
type PureMemEngine struct {
	table *HAMT
}

// NewPureMemEngine cria um motor vazio.
func NewPureMemEngine() *PureMemEngine {
	return &PureMemEngine{table: NewHAMT()}
}

// Put copia chave e valor antes de guardar: o HAMT guarda os slices como vêm, e
// quem chama pode reaproveitar o buffer (ex.: o leitor da conexão).
func (e *PureMemEngine) Put(key []byte, value []byte) error {
	e.table.Put(cloneBytes(key), cloneBytes(value))
	return nil
}

// Get devolve uma cópia, para que quem chama não consiga mexer no valor guardado.
// Copiar fora do lock é seguro: o HAMT troca o slice num Put, nunca altera os bytes.
func (e *PureMemEngine) Get(key []byte) ([]byte, error) {
	value, err := e.table.Get(key)
	if err != nil {
		return nil, err
	}
	return cloneBytes(value), nil
}

// Delete remove a chave de verdade, liberando a memória dela. Apagar uma chave que
// não existe não é erro.
func (e *PureMemEngine) Delete(key []byte) error {
	e.table.Delete(key)
	return nil
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
