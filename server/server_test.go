package server_test

import (
	"fmt"
	"net"
	"ramdb/server"
	"testing"
	"time"
)

type ResponseCompatible struct{}

func (ResponseCompatible) Fail() error {
	return nil
}
func (ResponseCompatible) RawData() []byte {
	return []byte{}
}

func TestSandbox(t *testing.T) {
	go func() {
		srv := server.Server{
			ListenAddress:  "0.0.0.0:8000",
			MaxConnections: 50,
			MessageMaxSize: 1024 * 10,
			RequestHandleCallback: func(server.Request) server.Response {
				return ResponseCompatible{}
			},
		}
		err := srv.StartServing()
		if err != nil {
			fmt.Printf("Erro ao servir: %s", err.Error())
		}
	}()

	time.Sleep(time.Second)
	conn, err := net.Dial("tcp", "0.0.0.0:8000")
	if err != nil {
		println(err.Error())
	}
	conn.Write(append([]byte{32, 8, 5, 1, 1, 1, 1, 0, 2, 0, 0, 0, 1, 0, 0}, "Eu gosto de batata"...))
	time.Sleep(time.Second * 1)
	conn.Close()
	time.Sleep(time.Second * 1)
}
