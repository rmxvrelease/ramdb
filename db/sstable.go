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
		// Grava tamanho e bytes da chave
		err := binary.Write(&buf, binary.LittleEndian, uint32(len(kv.Key)))
		if err != nil {
			return err
		}
		buf.Write(kv.Key)

		// Grava tamanho e bytes do valor
		err = binary.Write(&buf, binary.LittleEndian, uint32(len(kv.Value)))
		if err != nil {
			return err
		}
		buf.Write(kv.Value)
	}

	// Chama o Encrypt APENAS com os bytes, sem pedir a chave (já tá no crypto.go)
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
	err = forEachPair(decryptedData, func(key, value []byte) bool {
		if bytes.Equal(key, searchKey) {
			found, ok = value, true
			return false // achou: para a varredura
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		// Varreu o arquivo todo e não achou
		return nil, ErrKeyNotFound
	}
	return found, nil
}

// ReadSSTableKeys devolve todas as chaves de um SSTable. É usado para remontar o
// filtro de Bloom dos arquivos que já estavam no disco quando o banco iniciou.
func ReadSSTableKeys(filepath string) ([][]byte, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var keys [][]byte
	err = forEachPair(decryptedData, func(key, _ []byte) bool {
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
func forEachPair(data []byte, visit func(key, value []byte) bool) error {
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Lê o tamanho da chave (4 bytes)
		var keyLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return err
		}

		// Extrai a chave. io.ReadFull exige ler tudo; buf.Read poderia ler menos sem dar erro.
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(buf, key); err != nil {
			return err
		}

		// Lê o tamanho do valor (4 bytes)
		var valLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &valLen); err != nil {
			return err
		}

		// Extrai o valor
		value := make([]byte, valLen)
		if _, err := io.ReadFull(buf, value); err != nil {
			return err
		}

		if !visit(key, value) {
			return nil
		}
	}
	return nil
}
