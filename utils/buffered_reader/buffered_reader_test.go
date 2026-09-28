package bufferedreader_test

import (
	"errors"
	"io"
	"testing"
	"time"

	bufferedreader "ramdb/utils/buffered_reader"
)

// sliceReader entrega os itens de data, no maximo chunk por chamada.
// Quando data acaba, devolve err (io.EOF por padrao).
type sliceReader[T any] struct {
	data  []T
	chunk int
	err   error
	calls int
}

func (r *sliceReader[T]) Read(p []T) (int, error) {
	r.calls++
	if len(r.data) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		return 0, io.EOF
	}
	n := min(len(p), len(r.data))
	if r.chunk > 0 {
		n = min(n, r.chunk)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

// errReader falha em toda chamada de Read.
type errReader[T any] struct{ err error }

func (r *errReader[T]) Read(p []T) (int, error) { return 0, r.err }

func bytesOf(s string) []byte { return []byte(s) }

func TestReadMenorQueOBuffer(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello world")}
	reader := bufferedreader.New[byte](src, 8)
	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read: erro inesperado: %v", err)
	}
	if n != 5 {
		t.Fatalf("Read: n = %d, esperado 5", n)
	}
	if got := string(buf); got != "hello" {
		t.Fatalf("Read: buf = %q, esperado %q", got, "hello")
	}
}

func TestReadSubsequenteVemDoBufferInterno(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello world")}
	reader := bufferedreader.New[byte](src, 16)

	buf := make([]byte, 5)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("primeiro Read: %v", err)
	}
	callsDepoisDoPrimeiro := src.calls

	buf2 := make([]byte, 6)
	n, err := reader.Read(buf2)
	if err != nil {
		t.Fatalf("segundo Read: %v", err)
	}
	if n != 6 || string(buf2) != " world" {
		t.Fatalf("segundo Read: n = %d, buf = %q, esperado 6 e %q", n, string(buf2), " world")
	}
	if src.calls != callsDepoisDoPrimeiro {
		t.Errorf("segundo Read chamou o reader base %d vez(es); deveria ser servido do buffer interno",
			src.calls-callsDepoisDoPrimeiro)
	}
}

func TestReadSequencialConsomeTudoNaOrdem(t *testing.T) {
	const conteudo = "abcdefghijklmnopqrstuvwxyz0123456789"

	casos := []struct {
		nome       string
		bufferSize int
		tamanhos   []int
	}{
		{"leituras uniformes menores que o buffer", 8, []int{4, 4, 4, 4}},
		{"leitura parcial seguida de leitura maior", 8, []int{3, 5, 6, 6}},
		{"leitura maior que o buffer interno", 4, []int{10, 10}},
		{"buffer interno de tamanho 1", 1, []int{2, 3, 4}},
		{"alterna pequeno e grande", 8, []int{1, 9, 2, 7}},
	}

	for _, caso := range casos {
		t.Run(caso.nome, func(t *testing.T) {
			src := &sliceReader[byte]{data: bytesOf(conteudo)}
			reader := bufferedreader.New[byte](src, caso.bufferSize)

			offset := 0
			for i, tamanho := range caso.tamanhos {
				buf := make([]byte, tamanho)
				n, err := reader.Read(buf)
				if err != nil {
					t.Fatalf("Read #%d (tamanho %d): erro inesperado: %v", i, tamanho, err)
				}
				if n != tamanho {
					t.Fatalf("Read #%d: n = %d, esperado %d", i, n, tamanho)
				}
				esperado := conteudo[offset : offset+tamanho]
				if got := string(buf[:n]); got != esperado {
					t.Fatalf("Read #%d: buf = %q, esperado %q", i, got, esperado)
				}
				offset += n
			}
		})
	}
}

func TestReadMaiorQueOBufferInterno(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("0123456789abcdef")}
	reader := bufferedreader.New[byte](src, 4)

	buf := make([]byte, 12)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read: erro inesperado: %v", err)
	}
	if n != 12 || string(buf) != "0123456789ab" {
		t.Fatalf("Read: n = %d, buf = %q, esperado 12 e %q", n, string(buf), "0123456789ab")
	}
}

func TestReadComReaderBaseEntregandoPoucoPorVez(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello world"), chunk: 2}
	reader := bufferedreader.New[byte](src, 8)

	buf := make([]byte, 6)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read: erro inesperado: %v", err)
	}
	if n <= 0 || n > 6 {
		t.Fatalf("Read: n = %d, fora do intervalo (0, 6]", n)
	}
	if got, esperado := string(buf[:n]), "hello "[:n]; got != esperado {
		t.Fatalf("Read: buf[:%d] = %q, esperado %q", n, got, esperado)
	}
}

func TestReadRetornaDadosPendentesQuandoAFonteAcaba(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello")}
	reader := bufferedreader.New[byte](src, 8)

	buf := make([]byte, 3)
	if _, err := reader.Read(buf); err != nil {
		t.Fatalf("primeiro Read: %v", err)
	}

	// Restam 2 itens no buffer interno e a fonte esta vazia.
	buf2 := make([]byte, 4)
	n, err := reader.Read(buf2)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("segundo Read: erro inesperado: %v", err)
	}
	if n != 2 {
		t.Fatalf("segundo Read: n = %d, esperado 2 (os itens ainda no buffer interno)", n)
	}
	if got := string(buf2[:n]); got != "lo" {
		t.Fatalf("segundo Read: buf = %q, esperado %q", got, "lo")
	}
}

func TestReadPropagaErroDoReaderBase(t *testing.T) {
	falha := errors.New("falha na fonte")
	reader := bufferedreader.New[byte](&errReader[byte]{err: falha}, 8)

	buf := make([]byte, 4)
	n, err := reader.Read(buf)
	if !errors.Is(err, falha) {
		t.Fatalf("Read: err = %v, esperado %v", err, falha)
	}
	if n != 0 {
		t.Fatalf("Read: n = %d, esperado 0", n)
	}
}

func TestReadNaoChamaAFonteQuandoOBufferJaAtende(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello world")}
	reader := bufferedreader.New[byte](src, 16)

	if _, err := reader.Read(make([]byte, 1)); err != nil {
		t.Fatalf("primeiro Read: %v", err)
	}
	if src.calls != 1 {
		t.Fatalf("primeiro Read fez %d chamadas a fonte, esperado 1", src.calls)
	}
	for i := range 5 {
		if _, err := reader.Read(make([]byte, 2)); err != nil {
			t.Fatalf("Read #%d: %v", i+1, err)
		}
	}
	if src.calls != 1 {
		t.Errorf("fonte chamada %d vezes; os dados ja estavam no buffer interno", src.calls)
	}
}

func TestReadBufferVazio(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello")}
	reader := bufferedreader.New[byte](src, 8)

	n, err := reader.Read([]byte{})
	if err != nil {
		t.Fatalf("Read com buf vazio: erro inesperado: %v", err)
	}
	if n != 0 {
		t.Fatalf("Read com buf vazio: n = %d, esperado 0", n)
	}
	if src.calls != 0 {
		t.Errorf("Read com buf vazio chamou a fonte %d vez(es), esperado 0", src.calls)
	}
}

func TestReadGenericoComTipoNaoByte(t *testing.T) {
	type ponto struct{ X, Y int }
	origem := []ponto{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}

	src := &sliceReader[ponto]{data: append([]ponto(nil), origem...)}
	reader := bufferedreader.New[ponto](src, 2)

	lidos := make([]ponto, 0, len(origem))
	for len(lidos) < len(origem) {
		buf := make([]ponto, 2)
		n, err := reader.Read(buf)
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("Read: erro inesperado: %v", err)
		}
		if n == 0 {
			break
		}
		lidos = append(lidos, buf[:n]...)
	}

	if len(lidos) != len(origem) {
		t.Fatalf("lidos %d pontos, esperado %d (%v)", len(lidos), len(origem), lidos)
	}
	for i := range origem {
		if lidos[i] != origem[i] {
			t.Fatalf("ponto %d = %v, esperado %v", i, lidos[i], origem[i])
		}
	}
}

// eofReader entrega os itens de data e devolve io.EOF junto com os ultimos
// bytes, na mesma chamada -- comportamento permitido pelo contrato io.Reader.
type eofReader[T any] struct {
	data  []T
	chunk int
	calls int
}

func (r *eofReader[T]) Read(p []T) (int, error) {
	r.calls++
	n := min(len(p), len(r.data))
	if r.chunk > 0 {
		n = min(n, r.chunk)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, io.EOF
	}
	return n, nil
}

// zeroReader nunca progride: devolve 0 itens sem erro.
type zeroReader[T any] struct{ calls int }

func (r *zeroReader[T]) Read(p []T) (int, error) {
	r.calls++
	return 0, nil
}

func TestReadExactPreencheOBufferCompleto(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello world"), chunk: 3}
	reader := bufferedreader.New[byte](src, 4)

	buf := make([]byte, 11)
	n, err := reader.ReadExact(buf)
	if err != nil {
		t.Fatalf("ReadExact: erro inesperado: %v", err)
	}
	if n != 11 {
		t.Fatalf("ReadExact: n = %d, esperado 11", n)
	}
	if got := string(buf); got != "hello world" {
		t.Fatalf("ReadExact: buf = %q, esperado %q", got, "hello world")
	}
}

func TestReadExactNaoReportaEOFQuandoPreencheTudo(t *testing.T) {
	// A fonte devolve os 5 bytes e io.EOF na mesma chamada. O pedido foi
	// atendido por inteiro, entao ReadExact deve reportar sucesso.
	src := &eofReader[byte]{data: bytesOf("hello")}
	reader := bufferedreader.New[byte](src, 4)

	buf := make([]byte, 5)
	n, err := reader.ReadExact(buf)
	if err != nil {
		t.Fatalf("ReadExact: erro inesperado: %v", err)
	}
	if n != 5 {
		t.Fatalf("ReadExact: n = %d, esperado 5", n)
	}
	if got := string(buf); got != "hello" {
		t.Fatalf("ReadExact: buf = %q, esperado %q", got, "hello")
	}
}

func TestReadExactRetornaTotalEscritoQuandoAFonteAcaba(t *testing.T) {
	// Pede 16 com apenas 10 disponiveis: o n retornado precisa ser 10 (o total
	// realmente escrito em buf), e nao o tamanho da ultima leitura.
	src := &sliceReader[byte]{data: bytesOf("0123456789")}
	reader := bufferedreader.New[byte](src, 4)

	buf := make([]byte, 16)
	n, err := reader.ReadExact(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ReadExact: err = %v, esperado io.EOF", err)
	}
	if n != 10 {
		t.Fatalf("ReadExact: n = %d, esperado 10", n)
	}
	if got := string(buf[:n]); got != "0123456789" {
		t.Fatalf("ReadExact: buf[:n] = %q, esperado %q", got, "0123456789")
	}
}

func TestReadExactLeituraSemProgressoViraErrUnexpectedEOF(t *testing.T) {
	// Uma fonte que devolve (0, nil) para sempre nao pode travar ReadExact.
	// O teste roda em outra goroutine para que uma regressao falhe por timeout
	// em vez de pendurar a suite inteira.
	type resultado struct {
		n   int
		err error
	}
	done := make(chan resultado, 1)
	go func() {
		reader := bufferedreader.New[byte](&zeroReader[byte]{}, 4)
		n, err := reader.ReadExact(make([]byte, 8))
		done <- resultado{n, err}
	}()

	select {
	case got := <-done:
		if !errors.Is(got.err, io.ErrUnexpectedEOF) {
			t.Fatalf("ReadExact: err = %v, esperado io.ErrUnexpectedEOF", got.err)
		}
		if got.n != 0 {
			t.Fatalf("ReadExact: n = %d, esperado 0", got.n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadExact: nao retornou -- laco infinito com fonte sem progresso")
	}
}

func TestReadExactPropagaErroDoReaderBase(t *testing.T) {
	falha := errors.New("falha de leitura")
	reader := bufferedreader.New[byte](&errReader[byte]{err: falha}, 4)

	buf := make([]byte, 4)
	n, err := reader.ReadExact(buf)
	if !errors.Is(err, falha) {
		t.Fatalf("ReadExact: err = %v, esperado %v", err, falha)
	}
	if n != 0 {
		t.Fatalf("ReadExact: n = %d, esperado 0", n)
	}
}

func TestReadExactAproveitaOBufferInterno(t *testing.T) {
	// Um Read inicial enche o buffer interno; o ReadExact seguinte deve ser
	// atendido sem tocar na fonte.
	src := &sliceReader[byte]{data: bytesOf("hello world")}
	reader := bufferedreader.New[byte](src, 8)

	if _, err := reader.Read(make([]byte, 2)); err != nil {
		t.Fatalf("Read: erro inesperado: %v", err)
	}
	chamadas := src.calls

	buf := make([]byte, 3)
	n, err := reader.ReadExact(buf)
	if err != nil {
		t.Fatalf("ReadExact: erro inesperado: %v", err)
	}
	if n != 3 {
		t.Fatalf("ReadExact: n = %d, esperado 3", n)
	}
	if got := string(buf); got != "llo" {
		t.Fatalf("ReadExact: buf = %q, esperado %q", got, "llo")
	}
	if src.calls != chamadas {
		t.Fatalf("ReadExact: chamou a fonte %d vez(es) a mais", src.calls-chamadas)
	}
}

func TestReadExactBufferVazio(t *testing.T) {
	src := &sliceReader[byte]{data: bytesOf("hello")}
	reader := bufferedreader.New[byte](src, 4)

	n, err := reader.ReadExact(nil)
	if err != nil {
		t.Fatalf("ReadExact: erro inesperado: %v", err)
	}
	if n != 0 {
		t.Fatalf("ReadExact: n = %d, esperado 0", n)
	}
	if src.calls != 0 {
		t.Fatalf("ReadExact: chamou a fonte %d vez(es) com buffer vazio", src.calls)
	}
}

func TestReadExactGenericoComTipoNaoByte(t *testing.T) {
	type ponto struct{ X, Y int }

	origem := []ponto{{1, 2}, {3, 4}, {5, 6}, {7, 8}}
	src := &sliceReader[ponto]{data: origem, chunk: 1}
	reader := bufferedreader.New[ponto](src, 2)

	buf := make([]ponto, 4)
	n, err := reader.ReadExact(buf)
	if err != nil {
		t.Fatalf("ReadExact: erro inesperado: %v", err)
	}
	if n != 4 {
		t.Fatalf("ReadExact: n = %d, esperado 4", n)
	}
	for i := range origem {
		if buf[i] != origem[i] {
			t.Fatalf("ponto %d = %v, esperado %v", i, buf[i], origem[i])
		}
	}
}
