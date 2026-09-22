package aof

import (
	"log"
	"os"
	"sync"
	"time"
)

type AOF struct {
	file *os.File
	ch   chan string
	wg   sync.WaitGroup
}

func NewAOF(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	// (PESSOA2) HÁ DE SER MUDADO PARA ACEITAR OS BYTES
	aof := &AOF{
		file: f,
		ch:   make(chan string, 1024), //buffer de 1024 comandos para evitar bloqueio do handler
	}

	aof.wg.Add(1)
	go aof.worker() //inicia goroutine de gravação em background

	return aof, nil
}

func (a *AOF) worker() {
	defer a.wg.Done()

	//sincroniza arquivo com o disco a cada segundo
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case cmd, ok := <-a.ch:
			if !ok {
				a.file.Sync()
				return
			}
			// (PESSOA2) HÁ DE SER MUDADO PARA ACEITAR OS BYTES
			_, err := a.file.WriteString(cmd)
			if err != nil {
				log.Println("Erro ao gravar no AOF:", err)
			}
		case <-ticker.C:
			a.file.Sync()
		}
	}
}

func (a *AOF) Append(cmd string) {
	a.ch <- cmd
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
