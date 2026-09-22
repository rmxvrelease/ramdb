package db

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var ErrKeyNotFound = errors.New("chave não encontrada")

// KVPair é a struct usada pelo rbtree.go e sstable.go
type KVPair struct {
	Key   []byte
	Value []byte
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
	sstables, nextFileIndex := loadSSTables()
	e := &Engine{
		activeTree: NewRBTree(),
		sstables:   sstables,
		flushCh:    make(chan *RBTree, 1),
	}
	go e.flushWorker(nextFileIndex)
	return e
}

// loadSSTables encontra os SSTables deixados por execuções anteriores e remonta o
// filtro de cada um. Os filtros ficam só na RAM: gravados em texto puro, deixariam
// qualquer um testar se uma chave existe no banco sem precisar da chave AES.
func loadSSTables() ([]ssTable, int) {
	paths, _ := filepath.Glob("sstable_*.data") // só dá erro com padrão malformado

	type file struct {
		index int
		path  string
	}
	var files []file
	for _, path := range paths {
		name := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "sstable_"), ".data")
		index, err := strconv.Atoi(name)
		if err != nil {
			continue
		}
		files = append(files, file{index, path})
	}
	// Ordena pelo número, não pelo texto: como string, "sstable_10000" viria antes de "sstable_9999".
	sort.Slice(files, func(i, j int) bool { return files[i].index < files[j].index })

	tables := make([]ssTable, 0, len(files))
	nextFileIndex := 1
	for _, f := range files {
		keys, err := ReadSSTableKeys(f.path)
		var filter *BloomFilter
		if err != nil {
			// Filtro nil responde "talvez": o Get sempre abre este arquivo. Mais lento, mas nunca perde chave.
			fmt.Printf("[Maestro - AVISO] Sem filtro de Bloom para %s: %v\n", f.path, err)
		} else {
			filter = newFilterFromKeys(keys)
		}
		tables = append(tables, ssTable{path: f.path, filter: filter})
		nextFileIndex = f.index + 1
	}
	return tables, nextFileIndex
}

func newFilterFromKeys(keys [][]byte) *BloomFilter {
	filter := NewBloomFilter(len(keys), BloomFalsePositiveRate)
	for _, key := range keys {
		filter.Add(key)
	}
	return filter
}

// flushWorker salva as árvores congeladas no disco
func (e *Engine) flushWorker(fileIndex int) {
	for treeToFlush := range e.flushCh {
		fmt.Printf("[Worker] Iniciando flush de %d chaves...\n", treeToFlush.Size)

		orderedData := treeToFlush.GetAllInOrder()
		filename := fmt.Sprintf("sstable_%04d.data", fileIndex)

		// O filtro é montado com exatamente as chaves que vão para este arquivo.
		filter := NewBloomFilter(len(orderedData), BloomFalsePositiveRate)
		for _, kv := range orderedData {
			filter.Add(kv.Key)
		}

		err := WriteSSTable(orderedData, filename)
		if err != nil {
			fmt.Printf("[Worker - ERRO CRÍTICO] Falha ao salvar %s: %v\n", filename, err)
		} else {
			fmt.Printf("[Worker] %s salvo e encriptado com sucesso!\n", filename)
			fileIndex++
		}

		// Publica o arquivo no catálogo e libera a imutável no MESMO lock. Assim nenhum
		// Get pega o instante em que a chave não está nem na RAM nem no catálogo.
		e.mu.Lock()
		if err == nil {
			e.sstables = append(e.sstables, ssTable{path: filename, filter: filter})
		}
		e.immutableTree = nil
		e.mu.Unlock()
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
	tables := e.sstables // copia só o cabeçalho do slice; um flush depois disso não mexe neste loop
	e.mu.RUnlock()

	// 1. Busca na MemTable Ativa (Tempo real)
	val, err := active.Get(key)
	if err == nil {
		return val, nil
	}

	// 2. Busca na Imutável (Aguardando o worker de disco)
	if immutable != nil {
		val, err = immutable.Get(key)
		if err == nil {
			return val, nil
		}
	}

	// 3. Busca no Disco (SSTables Encriptadas)
	// Do mais novo para o mais antigo: é vital para pegar a versão mais atualizada da chave!
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]

		// Barreira do filtro de Bloom: se ele diz "não", é certeza, nem abre o arquivo.
		if !table.filter.MightContain(key) {
			continue
		}

		e.diskLookups.Add(1)
		val, err := FindInSSTable(key, table.path)
		if err == nil {
			// Achou no disco! Retorna com sucesso.
			return val, nil
		}
		// Falso positivo do filtro (ou erro ao ler o arquivo): segue para o próximo.
	}

	// Se chegou aqui, a chave realmente não existe em lugar nenhum do banco.
	return nil, ErrKeyNotFound
}

func (e *Engine) Delete(key []byte) error {
	return nil
}
