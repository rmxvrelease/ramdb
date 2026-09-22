package db

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
)

// Chave hardcoded temporária (EXATAMENTE 32 bytes)
var devEncryptionKey = []byte("ramdb-super-secret-key-32-bytes!")

// Encrypt sela o payload usando AES-GCM-256. Recebe apenas o dado!
func Encrypt(payload []byte) ([]byte, error) {
	block, err := aes.NewCipher(devEncryptionKey)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := aesGCM.Seal(nonce, nonce, payload, nil)
	return ciphertext, nil
}

// Decrypt abre o payload encriptado usando AES-GCM-256 (Exige APENAS o ciphertext)
func Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(devEncryptionKey)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := aesGCM.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("ciphertext muito curto")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := aesGCM.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}
