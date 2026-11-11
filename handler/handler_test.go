package handler

import (
	"ramdb/db"
	"ramdb/server"
	"testing"
)

func TestCommandHandler(t *testing.T) {

	database := db.NewEngine()
	cmdHandler := New(database, nil)

	cenarios := []struct {
		nome           string
		comando        string
		esperaErro     bool
		respostaAguard string
	}{
		{"Comando SET válido", "SET heroi batman", false, "OK"},
		{"Comando GET válido", "GET heroi", false, "batman"},
		{"GET em chave inexistente", "GET vilao", true, ""},
		{"Falta de argumentos no SET", "SET vazio", true, ""},
		{"Comando DEL válido", "DEL heroi", false, "OK"},
	}

	for _, tc := range cenarios {
		t.Run(tc.nome, func(t *testing.T) {

			req := server.Request{
				RequestBody: []byte(tc.comando),
			}

			resp := cmdHandler.Handle(req)

			byteResp, ok := resp.(ByteResponse)
			if !ok {
				t.Fatalf("O handler não retornou uma ByteResponse")
			}

			if tc.esperaErro && byteResp.Error == nil {
				t.Errorf("Esperava falha, mas o comando funcionou")
			}
			if !tc.esperaErro && byteResp.Error != nil {
				t.Errorf("Comando falhou inesperadamente: %v", byteResp.Error)
			}

			if string(byteResp.Data) != tc.respostaAguard {
				t.Errorf("Esperava '%s', recebeu '%s'", tc.respostaAguard, string(byteResp.Data))
			}
		})
	}
}
