package db

import (
	"bytes"
	"testing"
)

func TestRBTree_PutAndGet(t *testing.T) {
	tree := NewRBTree()

	// 1. Teste de Inserção Básica
	err := tree.Put([]byte("linguagem"), []byte("go"))
	if err != nil {
		t.Fatalf("Falha inesperada ao inserir: %v", err)
	}

	// 2. Teste de Busca
	val, err := tree.Get([]byte("linguagem"))
	if err != nil {
		t.Fatalf("Chave não encontrada: %v", err)
	}
	if !bytes.Equal(val, []byte("go")) {
		t.Errorf("Esperava 'go', recebeu '%s'", string(val))
	}

	// 3. Teste de Sobrescrita (Atualização)
	tree.Put([]byte("linguagem"), []byte("c++"))
	val, _ = tree.Get([]byte("linguagem"))
	if !bytes.Equal(val, []byte("c++")) {
		t.Errorf("Esperava 'c++' após atualização, recebeu '%s'", string(val))
	}
	if tree.Size != 1 {
		t.Errorf("Tamanho da árvore deveria ser 1 após atualizar a mesma chave, mas é %d", tree.Size)
	}

	// Teste de Chave Inexistente
	_, err = tree.Get([]byte("banco_de_dados"))
	if err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound, recebeu: %v", err)
	}
}

func TestRBTree_MultipleInserts(t *testing.T) {
	tree := NewRBTree()

	// Inserindo chaves fora de ordem para forçar as rotações da árvore
	chaves := [][]byte{[]byte("d"), []byte("b"), []byte("a"), []byte("c"), []byte("e")}

	for _, k := range chaves {
		tree.Put(k, []byte("valor"))
	}

	if tree.Size != 5 {
		t.Errorf("Tamanho da árvore deveria ser 5, recebeu %d", tree.Size)
	}

	// Verifica se a raiz é preta (Regra fundamental da RB-Tree)
	if tree.Root.Color != Black {
		t.Errorf("A raiz da árvore deve ser sempre Preta")
	}
}
