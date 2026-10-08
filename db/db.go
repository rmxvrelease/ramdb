package db

import (
	"bytes" // Adicionado para o bytes.Compare
	"errors"
	"fmt"
	"os" // Adicionado para o os.Remove
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrKeyNotFound = errors.New("chave não encontrada")
var errTombstone = errors.New("internal: tombstone")

// KVPair é a struct usada pelo rbtree.go e sstable.go
type KVPair struct {
	Key       []byte
	Value     []byte
	Tombstone bool
}

const MemTableLimit = 4096

// BloomFalsePositiveRate é a chance de o filtro dizer "talvez" para uma chave que
// não está no arquivo. 1% custa ~9,6 bits por chave (~4,9 KB para 4096 chaves).
const BloomFalsePositiveRate = 0.01

// ssTable é uma entrada do catálogo em RAM: um arquivo no disco e o filtro das chaves dele.
type ssTable struct {
	path   string
	filter *BloomFilter
}

// Engine agora controla a memória Ativa, a Imutável e a fila de disco
type Engine struct {
	mu            sync.RWMutex
	activeTree    *RBTree
	immutableTree *RBTree
	sstables      []ssTable // catálogo do disco, do mais antigo para o mais novo
	flushCh       chan *RBTree
	diskLookups   atomic.Int64 // quantas vezes o Get precisou abrir um SSTable
}

// NewEngine inicializa o motor de armazenamento
func NewEngine() *Engine {
	sstables := loadSSTables() // Removido o nextFileIndex
	e := &Engine{
		activeTree: NewRBTree(),
		sstables:   sstables,
		flushCh:    make(chan *RBTree, 1),
	}
	go e.compactionWorker()
	go e.flushWorker() // Removido o argumento
	return e
}

// loadSSTables encontra os SSTables deixados por execuções anteriores e remonta o
// filtro de cada um. Os filtros ficam só na RAM: gravados em texto puro, deixariam
// qualquer um testar se uma chave existe no banco sem precisar da chave AES.
func loadSSTables() []ssTable {
	paths, _ := filepath.Glob("sstable_*.data")

	type file struct {
		timestamp int64
		path      string
	}
	var files []file
	for _, path := range paths {
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "sstable_"), ".data")
		// Usamos ParseInt com 64 bits por causa dos nanosegundos do UnixNano
		ts, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		files = append(files, file{ts, path})
	}

	sort.Slice(files, func(i, j int) bool { return files[i].timestamp < files[j].timestamp })

	var tables []ssTable
	for _, f := range files {
		keys, err := ReadSSTableKeys(f.path)
		var filter *BloomFilter
		if err != nil {
			fmt.Printf("[Maestro - AVISO] Sem filtro para %s: %v\n", f.path, err)
		} else {
			filter = newFilterFromKeys(keys)
		}
		tables = append(tables, ssTable{path: f.path, filter: filter})
	}
	return tables
}

func newFilterFromKeys(keys [][]byte) *BloomFilter {
	filter := NewBloomFilter(len(keys), BloomFalsePositiveRate)
	for _, key := range keys {
		filter.Add(key)
	}
	return filter
}

// flushWorker salva as árvores congeladas no disco
func (e *Engine) flushWorker() {
	for treeToFlush := range e.flushCh {
		fmt.Printf("[Worker de Disco] Iniciando flush de %d chaves...\n", treeToFlush.Size)

		orderedData := treeToFlush.GetAllInOrder()
		filename := fmt.Sprintf("sstable_%d.data", time.Now().UnixNano()) // Nomenclatura temporal

		filter := NewBloomFilter(len(orderedData), BloomFalsePositiveRate)
		for _, kv := range orderedData {
			filter.Add(kv.Key)
		}

		err := WriteSSTable(orderedData, filename)
		if err != nil {
			fmt.Printf("[Worker de Disco - ERRO CRÍTICO] Falha ao salvar %s: %v\n", filename, err)
		} else {
			fmt.Printf("[Worker de Disco] %s salvo e encriptado com sucesso!\n", filename)
		}

		e.mu.Lock()
		if err == nil {
			e.sstables = append(e.sstables, ssTable{path: filename, filter: filter})
		}
		e.immutableTree = nil
		e.mu.Unlock()
	}
}

func (e *Engine) compactionWorker() {
	// Acorda a cada 15 segundos em background para ver se precisa limpar a casa
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		e.mu.RLock()
		// Se tivermos menos que 3 arquivos, não vale a pena gastar CPU
		if len(e.sstables) < 3 {
			e.mu.RUnlock()
			continue
		}

		// Tira um "Snapshot" de quais arquivos vamos compactar.
		// Vamos pegar TODOS os arquivos existentes neste exato milissegundo.
		tablesToCompact := make([]ssTable, len(e.sstables))
		copy(tablesToCompact, e.sstables)
		e.mu.RUnlock()

		fmt.Printf("[Worker de Compactação] Iniciando fusão de %d arquivos antigos...\n", len(tablesToCompact))

		// 1. Resolver conflitos: Como tablesToCompact está ordenado do mais antigo pro mais novo,
		// ao jogarmos no mapa, as chaves mais recentes sobrescrevem automaticamente as velhas!
		compactedMap := make(map[string]KVPair)
		for _, table := range tablesToCompact {
			pairs, err := ReadAllFromSSTable(table.path)
			if err != nil {
				continue
			}
			for _, kv := range pairs {
				compactedMap[string(kv.Key)] = kv
			}
		}

		// 2. O GRANDE FILTRO: Descartar as Lápides!
		var finalPairs []KVPair
		for _, kv := range compactedMap {
			if !kv.Tombstone {
				// Se for lápide, ela morre aqui e o HD agradece.
				finalPairs = append(finalPairs, kv)
			}
		}

		// 3. Ordenar lexicograficamente para permitir Busca Binária futura
		sort.Slice(finalPairs, func(i, j int) bool {
			return bytes.Compare(finalPairs[i].Key, finalPairs[j].Key) < 0
		})

		// 4. Salvar o arquivão compactado e limpo
		compactedFilename := fmt.Sprintf("sstable_%d.data", time.Now().UnixNano())
		err := WriteSSTable(finalPairs, compactedFilename)
		if err != nil {
			fmt.Println("[Worker de Compactação] Erro na escrita:", err)
			continue
		}

		newFilter := NewBloomFilter(len(finalPairs), BloomFalsePositiveRate)
		for _, kv := range finalPairs {
			newFilter.Add(kv.Key)
		}
		newTable := ssTable{path: compactedFilename, filter: newFilter}

		// 5. Atualizar o Catálogo Seguro no Engine
		e.mu.Lock()
		// Atenção matemática absurda: enquanto compactávamos, o cliente pode ter gerado
		// NOVOS arquivos. Nós preservamos os novos, mas substituímos os velhos pelo arquivão.
		var updatedSSTables []ssTable
		updatedSSTables = append(updatedSSTables, newTable)
		updatedSSTables = append(updatedSSTables, e.sstables[len(tablesToCompact):]...)
		e.sstables = updatedSSTables
		e.mu.Unlock()

		// 6. Faxina física no Sistema Operacional
		for _, table := range tablesToCompact {
			os.Remove(table.path)
		}

		fmt.Printf("[Worker de Compactação] 🟢 Sucesso! %d chaves vivas consolidadas. %d arquivos mortos deletados.\n", len(finalPairs), len(tablesToCompact))
	}
}

// Put roteia a escrita e verifica limites
func (e *Engine) Put(key []byte, value []byte) error {
	e.mu.Lock()

	if e.activeTree.Size >= MemTableLimit {
		if e.immutableTree != nil {
			e.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			return e.Put(key, value)
		}

		fmt.Println("[Maestro] Limite atingido. Congelando MemTable ativa...")
		e.immutableTree = e.activeTree
		e.activeTree = NewRBTree()
		e.flushCh <- e.immutableTree
	}

	currentActive := e.activeTree
	e.mu.Unlock()

	return currentActive.Put(key, value)
}

// Get lê primeiro da ativa, depois da imutável
func (e *Engine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	active := e.activeTree
	immutable := e.immutableTree
	tables := e.sstables
	e.mu.RUnlock()

	// 1. Busca na MemTable Ativa
	val, err := active.Get(key)
	if err == nil {
		return val, nil
	}
	if err == errTombstone {
		return nil, ErrKeyNotFound // Lápide encontrada: aborta a busca e esconde do usuário
	}

	// 2. Busca na Imutável
	if immutable != nil {
		val, err = immutable.Get(key)
		if err == nil {
			return val, nil
		}
		if err == errTombstone {
			return nil, ErrKeyNotFound
		}
	}

	// 3. Busca no Disco (Do mais novo para o mais antigo)
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]

		if !table.filter.MightContain(key) {
			continue
		}

		e.diskLookups.Add(1)
		val, err := FindInSSTable(key, table.path)
		if err == nil {
			return val, nil
		}
		if err == errTombstone {
			return nil, ErrKeyNotFound // Lápide no disco: aborta a busca
		}
	}

	return nil, ErrKeyNotFound
}

func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()

	// Mesma lógica de limite do Put: se a RAM lotou, congela e joga pro worker
	if e.activeTree.Size >= MemTableLimit {
		if e.immutableTree != nil {
			e.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			return e.Delete(key)
		}

		fmt.Println("[Maestro] Limite atingido via Delete. Congelando MemTable ativa...")
		e.immutableTree = e.activeTree
		e.activeTree = NewRBTree()
		e.flushCh <- e.immutableTree
	}

	currentActive := e.activeTree
	e.mu.Unlock()

	// Insere a chave disfarçada de Lápide
	return currentActive.PutTombstone(key)
}
