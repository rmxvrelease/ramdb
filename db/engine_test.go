package db

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestEngine_Lifecycle_And_Flush(t *testing.T) {

	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()

	for i := 0; i < 4100; i++ {
		key := []byte(fmt.Sprintf("chave_%04d", i))
		val := []byte(fmt.Sprintf("valor_%04d", i))
		err := engine.Put(key, val)
		if err != nil {
			t.Fatalf("Erro no Put da iteração %d: %v", i, err)
		}
	}

	time.Sleep(1 * time.Second)

	valMem, err := engine.Get([]byte("chave_4098"))
	if err != nil || !bytes.Equal(valMem, []byte("valor_4098")) {
		t.Errorf("Falha ao ler chave que deveria estar na MemTable Ativa: %v", err)
	}

	valDisco, err := engine.Get([]byte("chave_0500"))
	if err != nil || !bytes.Equal(valDisco, []byte("valor_0500")) {
		t.Errorf("Falha ao ler chave que deveria estar no Disco: %v", err)
	}

	_, err = engine.Get([]byte("chave_9999"))
	if err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound, recebeu: %v", err)
	}
}

func limparSSTables() {
	files, _ := filepath.Glob("sstable_*.data")
	for _, f := range files {
		os.Remove(f)
	}
}

func TestEngine_ConcurrentStress_WithLogs(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	logFile, err := os.OpenFile("stress_test.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatalf("Falha ao criar arquivo de log: %v", err)
	}
	defer logFile.Close()

	logger := log.New(logFile, "[STRESS TEST] ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("Iniciando bateria de testes de concorrência massiva...")

	engine := NewEngine()
	var wg sync.WaitGroup

	numWorkers := 500
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			key := []byte(fmt.Sprintf("chave_concorrente_%d", workerID))
			val := []byte(fmt.Sprintf("valor_%d", workerID))

			err := engine.Put(key, val)
			if err != nil {
				logger.Printf("ERRO no Worker %d: %v\n", workerID, err)
				t.Errorf("Worker %d falhou no Put: %v", workerID, err)
				return
			}
			logger.Printf("Worker %d escreveu %s com sucesso.\n", workerID, string(key))
		}(i)
	}

	wg.Wait()
	logger.Println("Escritas finalizadas. Iniciando validação de leitura...")

	for i := 0; i < numWorkers; i++ {
		key := []byte(fmt.Sprintf("chave_concorrente_%d", i))
		expectedVal := []byte(fmt.Sprintf("valor_%d", i))

		val, err := engine.Get(key)
		if err != nil {
			logger.Printf("FALHA DE LEITURA: %s não encontrada!\n", string(key))
			t.Errorf("Falha de concorrência: chave %s sumiu", string(key))
		} else if !bytes.Equal(val, expectedVal) {
			logger.Printf("FALHA DE INTEGRIDADE: %s tem valor errado!\n", string(key))
			t.Errorf("Corrupção de dado na chave %s", string(key))
		}
	}
	logger.Println("Teste de concorrência finalizado com 100% de integridade.")
}

func TestEngine_Tombstone_Lifecycle(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()

	t.Run("Cenario 1: Delecao pura na MemTable", func(t *testing.T) {
		key := []byte("heroi1")
		engine.Put(key, []byte("batman"))

		engine.Delete(key)

		_, err := engine.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Esperava ErrKeyNotFound, recebeu: %v", err)
		}
	})

	t.Run("Cenario 2: Lápide na RAM mascarando dado no Disco", func(t *testing.T) {
		key := []byte("heroi2")
		engine.Put(key, []byte("superman"))

		for i := 0; i < MemTableLimit; i++ {
			engine.Put([]byte(fmt.Sprintf("lixo_%d", i)), []byte("dado"))
		}

		esperarFlush(t, engine, 1)

		engine.Delete(key)

		_, err := engine.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("A lápide na RAM falhou em mascarar o dado do disco! Erro: %v", err)
		}
	})

	t.Run("Cenario 3: A própria Lápide foi pro Disco", func(t *testing.T) {
		key := []byte("heroi3")
		engine.Put(key, []byte("flash"))
		engine.Delete(key)

		for i := 0; i < MemTableLimit; i++ {
			engine.Put([]byte(fmt.Sprintf("lixo2_%d", i)), []byte("dado"))
		}

		esperarFlush(t, engine, 2)

		_, err := engine.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("O motor não reconheceu a lápide no disco! Erro: %v", err)
		}
	})
}
func TestEngine_Compaction(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()

	engine.Put([]byte("heroi1"), []byte("batman"))
	engine.Put([]byte("heroi2"), []byte("superman"))

	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo1_%d", i)), []byte("dado"))
	}

	esperarFlush(t, engine, 1)

	engine.Put([]byte("heroi1"), []byte("o_cavaleiro_das_trevas"))
	engine.Delete([]byte("heroi2"))
	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo2_%d", i)), []byte("dado"))
	}

	esperarFlush(t, engine, 2)

	engine.Put([]byte("vilao1"), []byte("coringa"))
	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo3_%d", i)), []byte("dado"))
	}

	esperarFlush(t, engine, 3)

	fmt.Println("=== Aguardando o Worker de Compactação fazer a faxina... ===")

	//time.Sleep(3 * time.Second)
	time.Sleep(300 * time.Millisecond)

	_, err := engine.Get([]byte("heroi2"))
	if err != ErrKeyNotFound {
		t.Errorf("Falha: O heroi2 (deletado) ainda foi encontrado! Erro: %v", err)
	}

	val, err := engine.Get([]byte("heroi1"))
	if err != nil || string(val) != "o_cavaleiro_das_trevas" {
		t.Errorf("Falha ao recuperar a versão atualizada do heroi1. Retornou: %s", string(val))
	}
}
