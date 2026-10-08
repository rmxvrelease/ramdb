package server

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"ramdb/constants"
	bfreader "ramdb/utils/buffered_reader"
	bufferedreader "ramdb/utils/buffered_reader"
	"time"
)

type Server struct {
	ListenAddress         string
	MaxConnections        int
	MessageMaxSize        int
	RequestHandleCallback func(Request) Response
}

type Request struct {
	RequestId   uint32
	RequestBody []byte
}

type Response interface {
	Fail() error     // Indicates if  the request could be resolved correctly
	RawData() []byte // Eu gosto de batata
}

func encodeResponse(r Response) []byte {
	// Se a resposta falhou (ex: chave não encontrada), retornamos a string do erro
	if r.Fail() != nil {
		return []byte("ERRO: " + r.Fail().Error())
	}

	// Caso contrário, retornamos o dado bruto (que já é []byte graças ao handler.go)
	return r.RawData()
}

// lê de byte a byte do buffer até encontrar a sequência [b01, b01, b01, b01]
func gotoMessageStart(reader *bfreader.BufferedReader[byte]) error {
	padding_count := 0
	char_buffer := []byte{0}
	for {
		_, err := reader.Read(char_buffer[:1])
		if err != nil {
			return err
		}
		if char_buffer[0] == constants.SOH {
			if padding_count == 3 {
				return nil
			}
			padding_count += 1
		} else {
			padding_count = 0
		}
	}
}

func parseRequest(reader *bfreader.BufferedReader[byte], max_msg_length int) (Request, error) {
	err := gotoMessageStart(reader)
	if err != nil {
		return Request{}, err
	}
	char_buffer := make([]byte, 8)
	n, err := reader.ReadExact(char_buffer)
	if err != nil {
		return Request{}, err
	} else if n != 8 {
		return Request{}, fmt.Errorf("Não foi possível ler a header completa da mensagem.")
	}
	msg_length := binary.LittleEndian.Uint32(char_buffer[:4])
	if int(msg_length) > max_msg_length {
		return Request{}, fmt.Errorf("Message size limit exceeded.")
	}
	request_id := binary.LittleEndian.Uint32(char_buffer[4:])
	msg_body_buffer := make([]byte, msg_length)
	n, err = reader.ReadExact(msg_body_buffer)
	fmt.Printf("Eu gosto de batata. len %d,  id %d content %s\n", msg_length, request_id, string(msg_body_buffer))
	if err != nil {
		return Request{}, err
	} else {
		return Request{
			RequestId:   request_id,
			RequestBody: msg_body_buffer[:n],
		}, nil
	}
}

func (serv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufferedreader.New[byte](conn, 1024)
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 3))
		decoded_request, err := parseRequest(&reader, serv.MessageMaxSize)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				break
			}
			log.Printf("Error decoding request: %s", err.Error())
			continue
		}
		response := encodeResponse(serv.RequestHandleCallback(decoded_request))
		n, err := conn.Write(response)
		if err != nil || n != len(response) {
			log.Printf("Error in sending response: %v.", err)
		}
	}
}

func (serv *Server) initializeConnectionPool() chan net.Conn {
	connection_channel := make(chan net.Conn)
	for _ = range serv.MaxConnections {
		go func() {
			for {
				serv.handleConnection(<-connection_channel)
			}
		}()
	}
	return connection_channel
}

func (serv *Server) StartServing() error {
	listener, err := net.Listen("tcp", serv.ListenAddress)
	if err != nil {
		return err
	}
	defer listener.Close()
	conn_chan := serv.initializeConnectionPool()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting to connection: %v.", err)
			continue
		}
		select {
		case conn_chan <- conn:
		default:
			conn.Write([]byte("full"))
			conn.Close()
		}

	}
}
