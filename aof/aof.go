package aof

import (
	"log"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file *os.File
	ch   chan []byte // Adeus strings! Tráfego nativo de bytes
	wg   sync.WaitGroup
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0667)
	if err != nil {
		return nil, err
	}

	aof := &AOF{
		file: f,
		ch:   make(chan []byte, 1024), // Buffer de 1024 slices de bytes
	}

	aof.wg.Add(1)
	go aof.worker() // inicia goroutine de gravação em background

	return aof, nil
}

func (a *AOF) worker() {
	defer a.wg.Done()

	// sincroniza arquivo com o disco a cada segundo
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case cmd, ok := <-a.ch:
			if !ok {
				a.file.Sync()
				return
			}
			// Gravamos os bytes diretamente no arquivo
			_, err := a.file.Write(cmd)
			if err != nil {
				log.Println("Erro ao gravar no AOF:", err)
			}
		case <-ticker.C:
			a.file.Sync()
		}
	}
}

// Append agora recebe bytes diretamente
func (a *AOF) Append(cmd []byte) {
	// Fazemos uma cópia rápida e segura adicionando o '\n'.
	// Isso impede que, se o servidor TCP reutilizar o buffer de leitura subjacente,
	// nós gravemos "lixo" acidentalmente no disco.
	buf := make([]byte, len(cmd)+1)
	copy(buf, cmd)
	buf[len(cmd)] = '\n'

	a.ch <- buf
}

func (a *AOF) Close() {
	close(a.ch)
	a.wg.Wait()
	a.file.Close()
}

func (a *AOF) ReadFile() *os.File {
	// manda o ponteiro do arquivo pra posição 0
	a.file.Seek(0, 0)
	return a.file
}
