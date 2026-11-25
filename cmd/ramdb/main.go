package main

import (
	//"fmt"
	"flag"
	"log"
	"os"
	"ramdb/aof"
	"ramdb/benchmarks"
	"ramdb/db"
	"ramdb/handler"
	"ramdb/server"
)

type CustomResponse struct{}

func (CustomResponse) Fail() error {
	return nil
}

func run_server(addr string, benchmark_output string) {
	persistencia, err := aof.NewAOF("database.aof")
	if err != nil {
		log.Fatal("Erro ao iniciar AOF:", err)
	}
	defer persistencia.Close()

	db := db.NewInMemEngine()
	cmdHandler := handler.New(db, persistencia)

	bench, err := benchmarks.NewBenchmarkService(benchmark_output)
	if err != nil {
		log.Fatalf("Erro na criação de NewBenchmarkService: %v", err)
	}
	go bench.Run()

	serv := server.Server{
		ListenAddress:         addr,
		MaxConnections:        100,
		MessageMaxSize:        4096,
		RequestHandleCallback: cmdHandler.Handle,
		BenchmarkClient:       bench.GetBenchmarkClient(),
	}
	err = serv.StartServing()
	if err != nil {
		log.Printf("Erro não tratado no servidor: %v", err)
		os.Exit(1)
	}
}

func main() {
	addr := flag.String("addr", ":8000", "endereço em que o servidor escuta")
	benchmark_output := flag.String("benchmark-output", "benchmark.log", "arquivo onde as métricas de benchmark são escritas")
	flag.Parse()
	run_server(*addr, *benchmark_output)
}

/*
	fmt.Println("teste")

	// 2. Simulamos uma requisição que acabou de sair do TCP
	req1 := server.Request{RequestBody: []byte("SET chave secreta")}
	resp1 := cmdHandler.Handle(req1)
	fmt.Printf("Comando SET: %+v\n", resp1)

	// 3. Simulamos a busca dessa mesma chave
	req2 := server.Request{RequestBody: []byte("GET chave")}
	resp2 := cmdHandler.Handle(req2)
	fmt.Printf("Comando GET: %+v\n", resp2)

	// 4. Simulamos um erro
	req3 := server.Request{RequestBody: []byte("GET nao_existe")}
	resp3 := cmdHandler.Handle(req3)
	fmt.Printf("Comando GET (Erro): %+v\n", resp3)
*/

/*
	comandosSimulados := []string{
		"SET linguagem go",
		"GET linguagem",
		"DEL linguagem",
		"GET linguagem", //deve retornar erro
	}

	fmt.Println("teste AOF")
	for _, payload := range comandosSimulados {
		req := server.Request{
			RequestBody: []byte(payload),
		}

		resp := cmdHandler.Handle(req)

		//valida o retorno do handler
		if err := resp.Fail(); err != nil {
			fmt.Printf("Comando: %-20s -> ERRO: %v\n", payload, err)
		} else {
			if strResp, ok := resp.(handler.StringResponse); ok {
				fmt.Printf("Comando: %-20s -> RESPOSTA: %s\n", payload, strResp.Data)
			}
		}
	}
*/
