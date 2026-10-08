package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// WriteSSTable recebe os dados em ordem, serializa em binário e salva encriptado.
func WriteSSTable(data []KVPair, filepath string) error {
	var buf bytes.Buffer

	for _, kv := range data {
		// 1. Grava 1 byte indicando se é Tombstone (1) ou Não (0)
		var tombByte byte = 0
		if kv.Tombstone {
			tombByte = 1
		}
		buf.WriteByte(tombByte)

		// 2. Grava tamanho e bytes da chave
		err := binary.Write(&buf, binary.LittleEndian, uint32(len(kv.Key)))
		if err != nil {
			return err
		}
		buf.Write(kv.Key)

		// 3. Grava tamanho e bytes do valor
		err = binary.Write(&buf, binary.LittleEndian, uint32(len(kv.Value)))
		if err != nil {
			return err
		}
		buf.Write(kv.Value)
	}

	encryptedData, err := Encrypt(buf.Bytes())
	if err != nil {
		return fmt.Errorf("falha ao encriptar sstable: %v", err)
	}

	err = os.WriteFile(filepath, encryptedData, 0600)
	if err != nil {
		return fmt.Errorf("falha ao gravar arquivo sstable no disco: %v", err)
	}

	return nil
}

// FindInSSTable abre o arquivo, decripta a carga AES-256 e busca a chave sequencialmente.
func FindInSSTable(searchKey []byte, filepath string) ([]byte, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var found []byte
	var ok bool
	var wasTombstone bool // Guarda se achamos uma lápide

	err = forEachPair(decryptedData, func(key, value []byte, isTombstone bool) bool {
		if bytes.Equal(key, searchKey) {
			found, ok, wasTombstone = value, true, isTombstone
			return false // Achou a chave (mesmo que seja lápide), para a varredura
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	if ok {
		if wasTombstone {
			return nil, errTombstone // Sinaliza Lápide no disco
		}
		return found, nil
	}
	return nil, ErrKeyNotFound
}

// Leia apenas a atualização do callback nesta função, o resto se mantém:
func ReadSSTableKeys(filepath string) ([][]byte, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var keys [][]byte
	err = forEachPair(decryptedData, func(key, _ []byte, _ bool) bool {
		keys = append(keys, key)
		return true
	})
	return keys, err
}

// readSSTable lê os bytes criptografados do disco e decripta tudo para a memória.
func readSSTable(filepath string) ([]byte, error) {
	encryptedData, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	decryptedData, err := Decrypt(encryptedData)
	if err != nil {
		return nil, fmt.Errorf("falha ao decriptar %s: %v", filepath, err)
	}
	return decryptedData, nil
}

// forEachPair percorre o formato binário [tamChave][chave][tamValor][valor]...
// chamando visit para cada par. Se visit devolver false, a varredura para ali.
// forEachPair percorre o formato binário [Tombstone][tamChave][chave][tamValor][valor]...
func forEachPair(data []byte, visit func(key, value []byte, isTombstone bool) bool) error {
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Lê a flag Tombstone (1 byte)
		tombByte, err := buf.ReadByte()
		if err != nil {
			return err
		}
		isTombstone := tombByte == 1

		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, key); err != nil {
			return err
		}

		var valLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
			return err
		}

		value := make([]byte, valLen)
		if _, err := io.ReadFull(buf, value); err != nil {
			return err
		}

		if !visit(key, value, isTombstone) {
			return nil
		}
	}
	return nil
}

// ReadAllFromSSTable carrega todos os pares de um arquivo. Usado pelo worker de compactação.
func ReadAllFromSSTable(filepath string) ([]KVPair, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var pairs []KVPair
	err = forEachPair(decryptedData, func(key, value []byte, isTombstone bool) bool {
		// Fazemos cópias limpas dos slices para não prender o buffer grande na memória do GC
		k := make([]byte, len(key))
		copy(k, key)

		var v []byte
		if value != nil {
			v = make([]byte, len(value))
			copy(v, value)
		}

		pairs = append(pairs, KVPair{
			Key:       k,
			Value:     v,
			Tombstone: isTombstone,
		})
		return true // Continua lendo até o final
	})
	return pairs, err
}
