package db

import (
	"bytes"
	"os"
	"testing"
)

func TestSSTable_WriteAndRead(t *testing.T) {
	filename := "test_sstable_9999.data"

	// Limpa o arquivo de teste antes e depois de rodar
	os.Remove(filename)
	defer os.Remove(filename)

	// 1. Prepara dados falsos (simulando a saída do inOrderTraversal)
	dados := []KVPair{
		{Key: []byte("a_chave"), Value: []byte("valor_a")},
		{Key: []byte("b_chave"), Value: []byte("valor_b")},
		{Key: []byte("c_chave"), Value: []byte("valor_c")},
	}

	// 2. Escreve no disco (encriptado e empacotado em binário)
	err := WriteSSTable(dados, filename)
	if err != nil {
		t.Fatalf("Falha ao escrever SSTable: %v", err)
	}

	// 3. Lê do disco buscando uma chave existente
	val, err := FindInSSTable([]byte("b_chave"), filename)
	if err != nil {
		t.Fatalf("Erro ao buscar chave existente: %v", err)
	}

	if !bytes.Equal(val, []byte("valor_b")) {
		t.Errorf("Esperava 'valor_b', recebeu '%s'", string(val))
	}

	// 4. Busca uma chave inexistente (deve retornar ErrKeyNotFound)
	_, err = FindInSSTable([]byte("z_chave_fantasma"), filename)
	if err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound para chave inexistente, recebeu %v", err)
	}

	// 5. ReadSSTableKeys devolve todas as chaves, na ordem gravada
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
			// Cria um valor com 1 milhão de bytes da letra 'A'
			dados: []KVPair{{Key: []byte("chave_pesada"), Value: bytes.Repeat([]byte("A"), 1000000)}},
		},
		{
			nome:  "Caracteres de Controle e Nulos",
			dados: []KVPair{{Key: []byte{0x00, 0x01, 0xFF}, Value: []byte("\n\r\t\x00")}},
		},
	}

	for _, tc := range cenarios {
		t.Run(tc.nome, func(t *testing.T) {
			t.Logf("Rodando cenário crítico: %s", tc.nome) // Log nativo do Go de testes

			err := WriteSSTable(tc.dados, filename)
			if err != nil {
				t.Fatalf("Falha na gravação do cenário '%s': %v", tc.nome, err)
			}

			// Tenta recuperar a chave imediatamente
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
