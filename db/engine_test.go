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
	// Limpa qualquer sstable residual gerada por execuções anteriores
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()

	// O limite do motor (MemTableLimit) é 4096.
	// Vamos inserir 4100 chaves para forçar EXATAMENTE 1 flush para o disco.
	for i := 0; i < 4100; i++ {
		key := []byte(fmt.Sprintf("chave_%04d", i))
		val := []byte(fmt.Sprintf("valor_%04d", i))
		err := engine.Put(key, val)
		if err != nil {
			t.Fatalf("Erro no Put da iteração %d: %v", i, err)
		}
	}

	// Aguarda 1 segundo para a goroutine do worker assíncrono terminar de gravar no HD
	time.Sleep(1 * time.Second)

	// --- FASE DE LEITURA ---

	// 1. Lendo da MemTable Ativa (As chaves mais recentes ficaram na RAM: 4096 a 4099)
	valMem, err := engine.Get([]byte("chave_4098"))
	if err != nil || !bytes.Equal(valMem, []byte("valor_4098")) {
		t.Errorf("Falha ao ler chave que deveria estar na MemTable Ativa: %v", err)
	}

	// 2. Lendo do Disco (As chaves mais antigas foram pro arquivo sstable_0001.data)
	valDisco, err := engine.Get([]byte("chave_0500"))
	if err != nil || !bytes.Equal(valDisco, []byte("valor_0500")) {
		t.Errorf("Falha ao ler chave que deveria estar no Disco: %v", err)
	}

	// 3. Lendo chave que não existe em nenhum lugar
	_, err = engine.Get([]byte("chave_9999"))
	if err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound, recebeu: %v", err)
	}
}

// limparSSTables é uma função auxiliar para não sujar o seu repositório com lixo de testes
func limparSSTables() {
	files, _ := filepath.Glob("sstable_*.data")
	for _, f := range files {
		os.Remove(f)
	}
}

func TestEngine_ConcurrentStress_WithLogs(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	// 1. Configurando o arquivo de Log para o teste
	logFile, err := os.OpenFile("stress_test.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		t.Fatalf("Falha ao criar arquivo de log: %v", err)
	}
	defer logFile.Close()

	// Cria um logger customizado apontando para o arquivo
	logger := log.New(logFile, "[STRESS TEST] ", log.LstdFlags|log.Lmicroseconds)
	logger.Println("Iniciando bateria de testes de concorrência massiva...")

	engine := NewEngine()
	var wg sync.WaitGroup

	// 2. Disparando 500 Goroutines escrevendo simultaneamente
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

	// Espera todas as escritas terminarem
	wg.Wait()
	logger.Println("Escritas finalizadas. Iniciando validação de leitura...")

	// 3. Validando se nenhuma goroutine atropelou a outra
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
