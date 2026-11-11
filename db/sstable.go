package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func WriteSSTable(data []KVPair, filepath string) error {
	var buf bytes.Buffer

	for _, kv := range data {

		var tombByte byte = 0
		if kv.Tombstone {
			tombByte = 1
		}
		buf.WriteByte(tombByte)

		err := binary.Write(&buf, binary.LittleEndian, uint32(len(kv.Key)))
		if err != nil {
			return err
		}
		buf.Write(kv.Key)

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

func FindInSSTable(searchKey []byte, filepath string) ([]byte, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var found []byte
	var ok bool
	var wasTombstone bool

	err = forEachPair(decryptedData, func(key, value []byte, isTombstone bool) bool {
		if bytes.Equal(key, searchKey) {
			found, ok, wasTombstone = value, true, isTombstone
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	if ok {
		if wasTombstone {
			return nil, errTombstone
		}
		return found, nil
	}
	return nil, ErrKeyNotFound
}

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

func ReadAllFromSSTable(filepath string) ([]KVPair, error) {
	decryptedData, err := readSSTable(filepath)
	if err != nil {
		return nil, err
	}

	var pairs []KVPair
	err = forEachPair(decryptedData, func(key, value []byte, isTombstone bool) bool {

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
		return true
	})
	return pairs, err
}
