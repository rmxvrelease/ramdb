package integration_test

import (
	"fmt"
	"log"
	"ramdb/aof"
	"ramdb/benchmarks"
	"ramdb/client"
	"ramdb/db"
	"ramdb/handler"
	"ramdb/server"
	"testing"
	"time"
)

func runServer(handler *handler.CommandHandler, bench *benchmarks.BenchmarkClient) {
	serv := server.Server{
		ListenAddress:   ":8081", // Trocamos para 8081 para fugir da porta travada
		MaxConnections:  100,     // ESSENCIAL: Cria a pool de workers
		MessageMaxSize:  4096,    // ESSENCIAL: Permite que o parser aceite payloads reais
		BenchmarkClient: bench,
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

	database := db.NewInMemEngine()
	cmdHandler := handler.New(database, persistencia)
	bench, err := benchmarks.NewBenchmarkService()
	if err != nil {
		log.Fatalf("Erro na criação de NewBenchmarkService: %v", err)
	}

	go runServer(cmdHandler, bench.GetBenchmarkClient())
	go bench.Run()

	c := client.New(":8081")

	comandosSimulados := []func() ([]byte, error){
		func() ([]byte, error) {
			return []byte{}, c.Put("linguagem", []byte("go"))
		},
		func() ([]byte, error) {
			return c.Get("linguagem")
		},
		func() ([]byte, error) {
			return []byte{}, c.Del("linguagem")
		},
		func() ([]byte, error) {
			return c.Get("linguagem")
		},
	}

	time.Sleep(time.Second * 2)

	for _, request := range comandosSimulados {
		result, err := request()
		if err != nil {
			fmt.Printf("Erro: %s", err.Error())
		}
		fmt.Printf("Resposta: %s\n", result)
		time.Sleep(time.Second)
	}

}
