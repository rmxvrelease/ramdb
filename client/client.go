package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Client struct {
	id_counter    uint
	serverAddress string
}

func New(server_address string) Client {
	return Client{
		serverAddress: server_address,
	}
}

func (client *Client) Get(key string) ([]byte, error) {
	return client.send([]byte("GET " + key))
}

func (client *Client) Del(key string) error {
	return expectOK(client.send([]byte("DEL " + key)))
}

func (client *Client) Put(key string, val []byte) error {
	body := append([]byte("SET "+key+" "), val...)
	return expectOK(client.send(body))
}

// Monta a mensagem no formato do protocolo (padding + tamanho + id + corpo),
// envia ao servidor e retorna o corpo da resposta.
func (client *Client) send(body []byte) ([]byte, error) {
	client.id_counter += 1
	request := []byte{1, 1, 1, 1}
	request = binary.LittleEndian.AppendUint32(request, uint32(len(body)))
	request = binary.LittleEndian.AppendUint32(request, uint32(client.id_counter))
	request = append(request, body...)

	conn, err := net.Dial("tcp", client.serverAddress)
	if err != nil {
		return nil, fmt.Errorf("Erro de conexão: %s", err.Error())
	}
	defer conn.Close()

	_, err = conn.Write(request)
	if err != nil {
		return nil, fmt.Errorf("Erro de escrita: %s", err.Error())
	}

	// Fechar o lado de escrita faz o servidor encerrar a conexão após responder,
	// então podemos ler a resposta completa até o EOF.
	if tcp_conn, ok := conn.(*net.TCPConn); ok {
		tcp_conn.CloseWrite()
	}

	response, err := io.ReadAll(conn)
	if err != nil {
		return nil, fmt.Errorf("Erro de leitura: %s", err.Error())
	}
	if bytes.HasPrefix(response, []byte("ERRO: ")) {
		return nil, fmt.Errorf("Falha na requisição: %s", response[len("ERRO: "):])
	}
	if bytes_eq(response, []byte("full")) {
		return nil, fmt.Errorf("Falha na requisição: servidor sem conexões disponíveis")
	}

	return response, nil
}

func expectOK(response []byte, err error) error {
	if err != nil {
		return err
	}
	if !bytes_eq(response, []byte("OK")) {
		return fmt.Errorf("Falha na requisição: %s", response)
	}
	return nil
}

func bytes_eq(a []byte, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
