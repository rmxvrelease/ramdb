package db

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestBloomFilter_NuncaDaFalsoNegativo(t *testing.T) {
	filtro := NewBloomFilter(10000, BloomFalsePositiveRate)
	for i := 0; i < 10000; i++ {
		filtro.Add([]byte(fmt.Sprintf("chave_%06d", i)))
	}

	// A regra de ouro: tudo que entrou tem que responder "talvez".
	for i := 0; i < 10000; i++ {
		if !filtro.MightContain([]byte(fmt.Sprintf("chave_%06d", i))) {
			t.Fatalf("falso negativo em chave_%06d", i)
		}
	}
}

func TestBloomFilter_TaxaDeFalsoPositivo(t *testing.T) {
	const chaves, consultas = MemTableLimit, 100000

	filtro := NewBloomFilter(chaves, BloomFalsePositiveRate)
	for i := 0; i < chaves; i++ {
		filtro.Add([]byte(fmt.Sprintf("chave_%06d", i)))
	}

	falsosPositivos := 0
	for i := 0; i < consultas; i++ {
		if filtro.MightContain([]byte(fmt.Sprintf("ausente_%06d", i))) {
			falsosPositivos++
		}
	}

	taxa := float64(falsosPositivos) / consultas
	t.Logf("%d chaves em %d bits (%d bytes), k=%d: falso positivo medido %.2f%%",
		chaves, filtro.numBits, len(filtro.bits)*8, filtro.numHashes, taxa*100)
	if taxa > 2*BloomFalsePositiveRate {
		t.Errorf("taxa de falso positivo %.2f%% muito acima do alvo de %.0f%%", taxa*100, BloomFalsePositiveRate*100)
	}
}

func TestBloomFilter_NilSempreDizTalvez(t *testing.T) {
	var filtro *BloomFilter
	if !filtro.MightContain([]byte("qualquer")) {
		t.Error("filtro nil não sabe nada, então precisa responder \"talvez\"")
	}
}

func TestEngine_BloomEvitaLerODisco(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()
	encherUmaMemTable(engine, "chave")
	esperarFlush(t, engine, 1)

	for i := 0; i < 1000; i++ {
		_, err := engine.Get([]byte(fmt.Sprintf("fantasma_%04d", i)))
		if err != ErrKeyNotFound {
			t.Fatalf("Esperava ErrKeyNotFound, recebeu: %v", err)
		}
	}
	leituras := engine.diskLookups.Load()
	t.Logf("1000 GETs de chaves inexistentes abriram o disco %d vezes", leituras)
	if leituras > 50 {
		t.Errorf("o filtro deveria barrar quase todas as leituras, mas o disco foi aberto %d vezes", leituras)
	}

	// Uma chave que está no disco continua sendo encontrada.
	val, err := engine.Get([]byte("chave_0500"))
	if err != nil || !bytes.Equal(val, []byte("valor_0500")) {
		t.Errorf("Falha ao ler chave do disco: %v", err)
	}
}

func TestEngine_RemontaFiltrosAoReiniciar(t *testing.T) {
	limparSSTables()
	defer limparSSTables()

	primeiro := NewEngine()
	encherUmaMemTable(primeiro, "chave")
	esperarFlush(t, primeiro, 1)

	// "Reinicia" o banco: um Engine novo só conhece o que está no disco.
	engine := NewEngine()
	if len(engine.sstables) != 1 || engine.sstables[0].filter == nil {
		t.Fatalf("esperava 1 SSTable com filtro remontado, tem %d", len(engine.sstables))
	}
	val, err := engine.Get([]byte("chave_0500"))
	if err != nil || !bytes.Equal(val, []byte("valor_0500")) {
		t.Errorf("Falha ao ler chave gravada antes de reiniciar: %v", err)
	}

	// O próximo flush precisa ir para um arquivo novo, sem sobrescrever o sstable_0001.
	encherUmaMemTable(engine, "nova")
	esperarFlush(t, engine, 2)
	if engine.sstables[1].path != "sstable_0002.data" {
		t.Errorf("esperava sstable_0002.data, gravou %s", engine.sstables[1].path)
	}
	if _, err := engine.Get([]byte("chave_0500")); err != nil {
		t.Errorf("chave antiga sumiu depois do segundo flush: %v", err)
	}
}

// Mede o caso que o filtro existe para resolver: GET de chave que não está em lugar nenhum.
// Rode com: go test ./db -run '^$' -bench GetChaveInexistente -benchmem
func BenchmarkEngine_GetChaveInexistente(b *testing.B) {
	limparSSTables()
	defer limparSSTables()

	engine := NewEngine()
	for arquivo := 0; arquivo < 4; arquivo++ {
		encherUmaMemTable(engine, fmt.Sprintf("arq%d", arquivo))
		esperarFlush(b, engine, arquivo+1)
	}

	chave := []byte("chave_que_nao_existe")
	for b.Loop() {
		if _, err := engine.Get(chave); err != ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

// encherUmaMemTable insere MemTableLimit+1 chaves: a última dispara o congelamento
// das MemTableLimit anteriores, que vão para o disco num SSTable só.
func encherUmaMemTable(engine *Engine, prefixo string) {
	for i := 0; i <= MemTableLimit; i++ {
		engine.Put([]byte(fmt.Sprintf("%s_%04d", prefixo, i)), []byte(fmt.Sprintf("valor_%04d", i)))
	}
}

// esperarFlush espera o worker publicar n SSTables, em vez de um Sleep com tempo chutado.
func esperarFlush(tb testing.TB, engine *Engine, n int) {
	tb.Helper()
	prazo := time.Now().Add(10 * time.Second)
	for time.Now().Before(prazo) {
		engine.mu.RLock()
		pronto := len(engine.sstables) == n && engine.immutableTree == nil
		engine.mu.RUnlock()
		if pronto {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Fatalf("o worker não publicou %d SSTables a tempo", n)
}
