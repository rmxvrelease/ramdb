package db

import (
	"bytes"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	original := []byte("dados ultrassecretos da lsm-tree")

	// 1. Testa a Encriptação
	encrypted, err := Encrypt(original)
	if err != nil {
		t.Fatalf("Erro ao encriptar: %v", err)
	}

	if bytes.Equal(original, encrypted) {
		t.Fatalf("Falha crítica: O dado encriptado está idêntico ao original!")
	}

	// 2. Testa a Decriptação
	decrypted, err := Decrypt(encrypted)
	if err != nil {
		t.Fatalf("Erro ao decriptar: %v", err)
	}

	// 3. Valida se a informação sobreviveu intacta
	if !bytes.Equal(original, decrypted) {
		t.Fatalf("Esperava '%s', recebeu '%s'", string(original), string(decrypted))
	}
}
