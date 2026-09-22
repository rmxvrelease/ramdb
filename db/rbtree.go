package db

import (
	"bytes"
	"sync"
)

// Definindo as cores usando constantes tipadas para consumir o mínimo de memória
type Color bool

const (
	Red   Color = true
	Black Color = false
)

// Node representa um elemento da Árvore Rubro-Negra
type Node struct {
	Key   []byte
	Value []byte
	Color Color
	Left  *Node
	Right *Node
	// Na implementação padrão, ter um ponteiro para o pai (Parent)
	// facilita muito as rotações e o rebalanceamento.
	Parent *Node
}

// RBTree é a nossa MemTable
type RBTree struct {
	Root *Node
	mu   sync.RWMutex // Protege contra race conditions, igual vocês fizeram no map
	Size int          // Controla o tamanho (quantidade de chaves)
}

// NewRBTree inicializa uma árvore vazia
func NewRBTree() *RBTree {
	return &RBTree{}
}

// Get busca um valor na árvore em O(log n)
func (t *RBTree) Get(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.Root
	for current != nil {
		cmp := bytes.Compare(key, current.Key)
		if cmp == 0 { // key == current.Key
			return current.Value, nil
		} else if cmp < 0 { // key < current.Key
			current = current.Left
		} else { // key > current.Key
			current = current.Right
		}
	}

	return nil, ErrKeyNotFound
}

// Put insere ou atualiza um valor na árvore.
func (t *RBTree) Put(key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	newNode := &Node{
		Key:   key,
		Value: value,
		Color: Red, // Novos nós sempre nascem Vermelhos
	}

	var parent *Node
	current := t.Root

	// 1. Inserção normal de Árvore de Busca Binária
	for current != nil {
		parent = current
		cmp := bytes.Compare(newNode.Key, current.Key)

		if cmp == 0 {
			// A chave já existe, apenas atualizamos o valor
			current.Value = value
			return nil
		} else if cmp < 0 {
			current = current.Left
		} else {
			current = current.Right
		}
	}

	newNode.Parent = parent

	if parent == nil {
		t.Root = newNode
	} else if bytes.Compare(newNode.Key, parent.Key) < 0 {
		parent.Left = newNode
	} else {
		parent.Right = newNode
	}

	t.Size++

	// 2. Consertar as regras da Árvore Rubro-Negra
	t.insertFixup(newNode)
	return nil
}

// insertFixup garante que as propriedades rubro-negras não sejam violadas
func (t *RBTree) insertFixup(z *Node) {
	for z.Parent != nil && z.Parent.Color == Red {
		// Se o pai de Z for o filho esquerdo do avô
		if z.Parent == z.Parent.Parent.Left {
			y := z.Parent.Parent.Right // y é o tio de z
			if y != nil && y.Color == Red {
				// Caso 1: O tio é Vermelho. Recolore pai, tio e avô.
				z.Parent.Color = Black
				y.Color = Black
				z.Parent.Parent.Color = Red
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Right {
					// Caso 2: Z é filho direito. Rotação à esquerda no pai.
					z = z.Parent
					t.leftRotate(z)
				}
				// Caso 3: Z é filho esquerdo. Rotação à direita no avô.
				z.Parent.Color = Black
				z.Parent.Parent.Color = Red
				t.rightRotate(z.Parent.Parent)
			}
		} else {
			// Simétrico: o pai de Z é o filho direito do avô
			y := z.Parent.Parent.Left // tio
			if y != nil && y.Color == Red {
				z.Parent.Color = Black
				y.Color = Black
				z.Parent.Parent.Color = Red
				z = z.Parent.Parent
			} else {
				if z == z.Parent.Left {
					z = z.Parent
					t.rightRotate(z)
				}
				z.Parent.Color = Black
				z.Parent.Parent.Color = Red
				t.leftRotate(z.Parent.Parent)
			}
		}
	}
	t.Root.Color = Black // A raiz sempre termina Preta
}

// leftRotate gira os ponteiros para a esquerda
func (t *RBTree) leftRotate(x *Node) {
	y := x.Right
	x.Right = y.Left
	if y.Left != nil {
		y.Left.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Left {
		x.Parent.Left = y
	} else {
		x.Parent.Right = y
	}
	y.Left = x
	x.Parent = y
}

// rightRotate gira os ponteiros para a direita
func (t *RBTree) rightRotate(x *Node) {
	y := x.Left
	x.Left = y.Right
	if y.Right != nil {
		y.Right.Parent = x
	}
	y.Parent = x.Parent
	if x.Parent == nil {
		t.Root = y
	} else if x == x.Parent.Right {
		x.Parent.Right = y
	} else {
		x.Parent.Left = y
	}
	y.Right = x
	x.Parent = y
}

// GetAllInOrder varre a árvore inteira e retorna todos os dados ordenados lexicograficamente.
// Este método é utilizado para gerar o payload que será gravado no disco (SSTable).
func (t *RBTree) GetAllInOrder() []KVPair {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Pré-aloca o slice com o tamanho exato da árvore.
	// Isso evita realocações dinâmicas custosas sob o capô do Go.
	result := make([]KVPair, 0, t.Size)

	// Inicia a travessia a partir da raiz
	t.inOrderTraversal(t.Root, &result)

	return result
}

// inOrderTraversal é a função recursiva que caminha pela árvore: Esquerda -> Nó -> Direita.
func (t *RBTree) inOrderTraversal(node *Node, result *[]KVPair) {
	if node == nil {
		return
	}

	// 1. Desce tudo para a esquerda (menores valores)
	t.inOrderTraversal(node.Left, result)

	// 2. Processa o nó atual
	// Criamos cópias dos bytes para garantir que o worker de disco não
	// interfira acidentalmente na memória da árvore ativa
	keyCopy := make([]byte, len(node.Key))
	copy(keyCopy, node.Key)

	valueCopy := make([]byte, len(node.Value))
	copy(valueCopy, node.Value)

	*result = append(*result, KVPair{
		Key:   keyCopy,
		Value: valueCopy,
	})

	// 3. Desce para a direita (maiores valores)
	t.inOrderTraversal(node.Right, result)
}
