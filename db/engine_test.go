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

func TestEngine_Tombstone_Lifecycle(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()

	t.Run("Cenario 1: Delecao pura na MemTable", func(t *testing.T) {
		key := []byte("heroi1")
		engine.Put(key, []byte("batman"))

		engine.Delete(key) // Deleta imediatamente

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
		// AQUI É 1: É o primeiro flush do ciclo de vida
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
		// AQUI É 2: É o segundo flush deste teste
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

	// 1. GERAR ARQUIVO 1 (Dados Originais)
	engine.Put([]byte("heroi1"), []byte("batman"))
	engine.Put([]byte("heroi2"), []byte("superman"))
	// Enche a MemTable para forçar o flush pro disco
	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo1_%d", i)), []byte("dado"))
	}
	//time.Sleep(1 * time.Second) // Dá tempo do Worker de Disco salvar sstable_X.data
	esperarFlush(t, engine, 1)
	// 2. GERAR ARQUIVO 2 (Atualização e Deleção)
	engine.Put([]byte("heroi1"), []byte("o_cavaleiro_das_trevas")) // Atualiza a chave
	engine.Delete([]byte("heroi2"))                                // Marca a Lápide
	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo2_%d", i)), []byte("dado"))
	}
	//time.Sleep(1 * time.Second)
	esperarFlush(t, engine, 2)
	// 3. GERAR ARQUIVO 3 (Gatilho da Compactação)
	// O worker só roda se tiver len(sstables) >= 3
	engine.Put([]byte("vilao1"), []byte("coringa"))
	for i := 0; i < MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("lixo3_%d", i)), []byte("dado"))
	}
	//time.Sleep(1 * time.Second)
	esperarFlush(t, engine, 3)
	// Neste exato momento, temos 3 arquivos separados no disco, com dados duplicados e lápides.
	fmt.Println("=== Aguardando o Worker de Compactação fazer a faxina... ===")
	// Esperamos 3 segundos (tempo suficiente para o Ticker de 2s disparar)
	//time.Sleep(3 * time.Second)
	time.Sleep(300 * time.Millisecond)
	// Validação 1: O heroi2 TEM que estar apagado permanentemente
	_, err := engine.Get([]byte("heroi2"))
	if err != ErrKeyNotFound {
		t.Errorf("Falha: O heroi2 (deletado) ainda foi encontrado! Erro: %v", err)
	}

	// Validação 2: O heroi1 tem que retornar a versão ATUALIZADA (do arquivo 2), e não a original
	val, err := engine.Get([]byte("heroi1"))
	if err != nil || string(val) != "o_cavaleiro_das_trevas" {
		t.Errorf("Falha ao recuperar a versão atualizada do heroi1. Retornou: %s", string(val))
	}
}
