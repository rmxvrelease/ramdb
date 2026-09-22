package handler

import (
	"bytes"
	"fmt"

	"ramdb/aof"
	"ramdb/db"
	"ramdb/server"
)

// ByteResponse substitui a StringResponse, operando nativamente com bytes.
type ByteResponse struct {
	Data  []byte
	Error error
}

func (r ByteResponse) Fail() error {
	return r.Error
}

func (r ByteResponse) RawData() []byte {
	return r.Data
}

type CommandHandler struct {
	db  *db.Engine
	aof *aof.AOF
}

func New(db *db.Engine, a *aof.AOF) *CommandHandler {
	return &CommandHandler{
		db:  db,
		aof: a,
	}
}

// Handle atende as requisições TCP operando diretamente na memória com bytes.
func (h *CommandHandler) Handle(r server.Request) server.Response {
	payload := bytes.TrimSpace(r.RequestBody)
	args := bytes.Split(payload, []byte(" "))

	if len(args) == 0 || len(args[0]) == 0 {
		return ByteResponse{Error: fmt.Errorf("comando vazio")}
	}

	comando := bytes.ToUpper(args[0])

	// Switch em Go não suporta []byte nativamente, mas converter apenas os ~3 bytes do comando é inofensivo
	switch string(comando) {
	case "SET":
		if len(args) < 3 {
			return ByteResponse{Error: fmt.Errorf("uso correto: SET <chave> <valor>")}
		}
		chave := args[1]
		valor := bytes.Join(args[2:], []byte(" ")) // Junta o resto dos bytes com espaços

		err := h.db.Put(chave, valor)
		if err != nil {
			return ByteResponse{Error: err}
		}

		if h.aof != nil {
			// Cast para string mantido temporariamente apenas para não quebrar a tipagem do canal do AOF
			h.aof.Append(string(payload) + "\n")
		}
		return ByteResponse{Data: []byte("OK")}

	case "GET":
		if len(args) < 2 {
			return ByteResponse{Error: fmt.Errorf("uso correto: GET <chave>")}
		}
		chave := args[1]
		valor, err := h.db.Get(chave)
		if err != nil {
			return ByteResponse{Error: err}
		}
		return ByteResponse{Data: valor}

	case "DEL":
		if len(args) < 2 {
			return ByteResponse{Error: fmt.Errorf("uso correto: DEL <chave>")}
		}
		chave := args[1]
		h.db.Delete(chave)

		if h.aof != nil {
			h.aof.Append(string(payload) + "\n")
		}
		return ByteResponse{Data: []byte("OK")}

	default:
		return ByteResponse{Error: fmt.Errorf("comando desconhecido: %s", string(comando))}
	}
}
