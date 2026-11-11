package db

import (
	"bytes"
	"os"
	"testing"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	filename := "test_sstable_9999.data"

	os.Remove(filename)
	defer os.Remove(filename)

	dados := []KVPair{
		{Key: []byte("a_chave"), Value: []byte("valor_a")},
		{Key: []byte("b_chave"), Value: []byte("valor_b")},
		{Key: []byte("c_chave"), Value: []byte("valor_c")},
	}

	err := WriteSSTable(dados, filename)
	if err != nil {
		t.Fatalf("Falha ao escrever SSTable: %v", err)
	}

	val, err := FindInSSTable([]byte("b_chave"), filename)
	if err != nil {
		t.Fatalf("Erro ao buscar chave existente: %v", err)
	}

	if !bytes.Equal(val, []byte("valor_b")) {
		t.Errorf("Esperava 'valor_b', recebeu '%s'", string(val))
	}

	_, err = FindInSSTable([]byte("z_chave_fantasma"), filename)
	if err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound para chave inexistente, recebeu %v", err)
	}

	chaves, err := ReadSSTableKeys(filename)
	if err != nil || len(chaves) != len(dados) {
		t.Fatalf("Esperava %d chaves, recebeu %d (erro: %v)", len(dados), len(chaves), err)
	}
	for i, chave := range chaves {
		if !bytes.Equal(chave, dados[i].Key) {
			t.Errorf("Chave %d: esperava '%s', recebeu '%s'", i, dados[i].Key, chave)
		}
	}
}

func TestSSTable_ValorVazioNoFimDoArquivo(t *testing.T) {
	filename := "test_sstable_vazio.data"
	os.Remove(filename)
	defer os.Remove(filename)

	dados := []KVPair{
		{Key: []byte("a_chave"), Value: []byte("valor_a")},
		{Key: []byte("b_chave"), Value: []byte{}},
	}
	if err := WriteSSTable(dados, filename); err != nil {
		t.Fatalf("Falha ao escrever SSTable: %v", err)
	}

	val, err := FindInSSTable([]byte("b_chave"), filename)
	if err != nil || len(val) != 0 {
		t.Errorf("Esperava valor vazio sem erro, recebeu '%s' (erro: %v)", val, err)
	}
}

func TestSSTable_EdgeCases_ComLogsIntensos(t *testing.T) {
	filename := "edge_cases.data"
	os.Remove(filename)
	defer os.Remove(filename)

	cenarios := []struct {
		nome  string
		dados []KVPair
	}{
		{
			nome:  "Chaves e Valores Vazios (0 bytes)",
			dados: []KVPair{{Key: []byte(""), Value: []byte("")}},
		},
		{
			nome: "Valor Gigante (Payload pesado)",

			dados: []KVPair{{Key: []byte("chave_pesada"), Value: bytes.Repeat([]byte("A"), 1000000)}},
		},
		{
			nome:  "Caracteres de Controle e Nulos",
			dados: []KVPair{{Key: []byte{0x00, 0x01, 0xFF}, Value: []byte("\n\r\t\x00")}},
		},
	}

	for _, tc := range cenarios {
		t.Run(tc.nome, func(t *testing.T) {
			t.Logf("Rodando cenário crítico: %s", tc.nome)

			err := WriteSSTable(tc.dados, filename)
			if err != nil {
				t.Fatalf("Falha na gravação do cenário '%s': %v", tc.nome, err)
			}

			for _, kv := range tc.dados {
				recuperado, err := FindInSSTable(kv.Key, filename)
				if err != nil {
					t.Fatalf("Chave não encontrada no disco após gravar cenário '%s': %v", tc.nome, err)
				}
				if !bytes.Equal(recuperado, kv.Value) {
					t.Errorf("Dados corrompidos no cenário '%s'", tc.nome)
				}
			}
			t.Logf("✅ Cenário '%s' sobreviveu ao disco e criptografia!", tc.nome)
		})
	}
}
