package aof

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestAOF_AppendAndClose(t *testing.T) {
	filename := "test_database_basic.aof"
	os.Remove(filename)
	defer os.Remove(filename)

	a, err := NewAOF(filename)
	if err != nil {
		t.Fatalf("Falha ao criar AOF: %v", err)
	}

	// Agora testamos com arrays de bytes no formato real que o handler envia
	cmds := [][]byte{
		[]byte("SET key1 val1"),
		[]byte("DEL key1"),
		[]byte("SET key2 val2"),
	}

	for _, cmd := range cmds {
		a.Append(cmd)
	}
	a.Close() // Fecha o canal e aguarda a gravação no disco

	content, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Falha ao ler o arquivo AOF: %v", err)
	}

	for _, cmd := range cmds {
		// O Append() no aof.go adiciona um '\n' no final, então nossa validação deve esperar isso
		expected := append(cmd, '\n')
		if !bytes.Contains(content, expected) {
			t.Errorf("Esperava encontrar o comando %q no arquivo, mas não encontrou", cmd)
		}
	}
}

func TestAOF_Concurrency(t *testing.T) {
	filename := "test_database_concurrent.aof"
	os.Remove(filename)
	defer os.Remove(filename)

	a, err := NewAOF(filename)
	if err != nil {
		t.Fatalf("Falha ao criar AOF: %v", err)
	}

	var wg sync.WaitGroup
	goroutines := 50
	writesPerRoutine := 100

	// Dispara 50 goroutines metralhando o AOF ao mesmo tempo
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for j := 0; j < writesPerRoutine; j++ {
				cmd := []byte(fmt.Sprintf("SET key_%d_%d valor", routineID, j))
				a.Append(cmd)
			}
		}(i)
	}

	wg.Wait()
	a.Close()

	// Validação: Abre o arquivo e conta se as 5000 linhas foram gravadas perfeitamente
	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Falha ao ler AOF: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lines := 0
	for scanner.Scan() {
		lines++
	}

	expectedLines := goroutines * writesPerRoutine
	if lines != expectedLines {
		t.Errorf("Concorrência falhou. Esperava %d comandos, encontrou %d", expectedLines, lines)
	}
}

func TestAOF_ReadFileSeek(t *testing.T) {
	filename := "test_database_seek.aof"
	os.Remove(filename)
	defer os.Remove(filename)

	a, err := NewAOF(filename)
	if err != nil {
		t.Fatalf("Falha ao criar AOF: %v", err)
	}

	a.Append([]byte("SET ponteiro inicio"))
	a.Close()

	// Simula a reabertura do servidor carregando o AOF existente
	a2, _ := NewAOF(filename)
	defer a2.Close()

	// Pega o ponteiro na posição 0
	filePtr := a2.ReadFile()

	scanner := bufio.NewScanner(filePtr)
	if !scanner.Scan() {
		t.Fatal("Esperava ler a linha do arquivo retornado pelo ReadFile")
	}

	// O scanner retira o '\n' automaticamente
	if scanner.Text() != "SET ponteiro inicio" {
		t.Errorf("Conteúdo lido incorreto após o Seek: %s", scanner.Text())
	}
}
