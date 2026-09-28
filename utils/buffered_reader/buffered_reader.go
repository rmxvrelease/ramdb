package bufferedreader

import "io"

type Reader[T any] interface {
	Read([]T) (int, error)
}

type BufferedReader[T any] struct {
	readable Reader[T]
	buffer   []T
	pos      int
	data_end int
}

func New[T any](reader Reader[T], buffer_size int) BufferedReader[T] {
	return BufferedReader[T]{
		readable: reader,
		buffer:   make([]T, buffer_size),
		pos:      0,
		data_end: 0,
	}
}

func (reader *BufferedReader[T]) numOfBufferedBytes() int {
	return reader.data_end - reader.pos
}

// ReadExact preenche buf por completo, repetindo Read ate acabar.
// Se buf for preenchido por inteiro, não levanta erro (mesmo que a última leitura tenha acabado em EOF).
func (reader *BufferedReader[T]) ReadExact(buf []T) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := reader.Read(buf[total:])
		total += n
		if total == len(buf) {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

func (reader *BufferedReader[T]) Read(buf []T) (int, error) {
	remaining := reader.numOfBufferedBytes()
	if len(buf) <= remaining { // Número de bytes bufferizados já é suficiente para retornar
		copy(buf, reader.buffer[reader.pos:reader.data_end])
		reader.pos += len(buf)
		return len(buf), nil
	}

	missing := len(buf) - remaining
	copy(buf, reader.buffer[reader.pos:reader.data_end])
	tmp := make([]T, missing+len(reader.buffer)) // tenta ler os suficiente para completar o que está faltando + o bastante para encher o buffer
	nread, err := reader.readable.Read(tmp)
	if nread < missing {
		copy(buf[remaining:], tmp[:nread])
		reader.pos += remaining
		return remaining + nread, err
	}
	copy(buf[remaining:], tmp[:missing])
	copy(reader.buffer, tmp[missing:nread])
	reader.pos = 0
	reader.data_end = nread - missing
	return len(buf), err
}
