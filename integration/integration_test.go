package integration_test

import (
	"fmt"
	"log"
	"net"
	"ramdb/aof"
	"ramdb/db"
	"ramdb/handler"
	"ramdb/server"
	"testing"
	"time"
)

func runServer(handler *handler.CommandHandler) {
	serv := server.Server{
		ListenAddress:  ":8081", // Trocamos para 8081 para fugir da porta travada
		MaxConnections: 100,     // ESSENCIAL: Cria a pool de workers
		MessageMaxSize: 4096,    // ESSENCIAL: Permite que o parser aceite payloads reais
		RequestHandleCallback: func(r server.Request) server.Response {
			return handler.Handle(r)
		},
	}
	
	// Se o servidor falhar ao subir, o log nos avisa na hora em vez de morrer em silêncio
	if err := serv.StartServing(); err != nil {
		log.Printf("[FALHA FATAL] Erro ao iniciar o servidor TCP: %v\n", err)
	}
}

func TestMessageExchange(t *testing.T) {
	persistencia, err := aof.NewAOF("database.aof")
	if err != nil {
		log.Fatal("Erro ao iniciar AOF:", err)
	}
	defer persistencia.Close()
	
	database := db.NewEngine()
	cmdHandler := handler.New(database, persistencia)

	go runServer(cmdHandler)

	comandosSimulados := []string{
		"SET linguagem go",
		"GET linguagem",
		"DEL linguagem",
		"GET linguagem", //deve retornar erro
	}
	
	time.Sleep(time.Second * 2)

	for _, payload := range comandosSimulados {
		// Protocolo: 4 SOH (1,1,1,1) + msg_length + request_id
		request := []byte{1, 1, 1, 1, byte(len(payload)), 0, 0, 0, 0, 1, 0, 0}
		request = append(request, []byte(payload)...)

		// Lembra de trocar a porta no cliente também!
		conn, err := net.Dial("tcp", ":8081")
		if err != nil {
			log.Fatalf("Falha ao discar: %s", err.Error())
		}

		_, err = conn.Write(request)
		if err != nil {
			log.Fatalf("Falha ao escrever: %s", err.Error())
		}

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("\nErro de leitura: %s", err.Error())
		}
		
		fmt.Printf("Comando: %s | Resposta: %s\n", payload, buffer[:n])
	}
}