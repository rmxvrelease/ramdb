package db

import (
	"bytes"
	"sync"
)

type Color bool

const (
	Red   Color = true
	Black Color = false
)

type Node struct {
	Key       []byte
	Value     []byte
	Color     Color
	Left      *Node
	Right     *Node
	Parent    *Node
	Tombstone bool
}

type RBTree struct {
	Root *Node
	mu   sync.RWMutex
	Size int
}

func NewRBTree() *RBTree {
	return &RBTree{}
}

func (t *RBTree) Get(key []byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.Root
	for current != nil {
		cmp := bytes.Compare(key, current.Key)
		if cmp == 0 {
			if current.Tombstone {
				return nil, errTombstone
			}
			return current.Value, nil
		} else if cmp < 0 {
			current = current.Left
		} else {
			current = current.Right
		}
	}

	return nil, ErrKeyNotFound
}

func (t *RBTree) Put(key []byte, value []byte) error {
	return t.putInternal(key, value, false)
}

func (t *RBTree) PutTombstone(key []byte) error {
	return t.putInternal(key, nil, true)
}

func (t *RBTree) putInternal(key, value []byte, isTombstone bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	newNode := &Node{
		Key:       key,
		Value:     value,
		Color:     Red,
		Tombstone: isTombstone,
	}

	var parent *Node
	current := t.Root

	for current != nil {
		parent = current
		cmp := bytes.Compare(newNode.Key, current.Key)

		if cmp == 0 {
			current.Value = value
			current.Tombstone = isTombstone
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
	t.insertFixup(newNode)
	return nil
}

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
	t.Root.Color = Black
}

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

func (t *RBTree) GetAllInOrder() []KVPair {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]KVPair, 0, t.Size)

	t.inOrderTraversal(t.Root, &result)

	return result
}

func (t *RBTree) inOrderTraversal(node *Node, result *[]KVPair) {
	if node == nil {
		return
	}

	t.inOrderTraversal(node.Left, result)

	keyCopy := make([]byte, len(node.Key))
	copy(keyCopy, node.Key)

	valueCopy := make([]byte, len(node.Value))
	copy(valueCopy, node.Value)

	*result = append(*result, KVPair{
		Key:       keyCopy,
		Value:     valueCopy,
		Tombstone: node.Tombstone,
	})

	t.inOrderTraversal(node.Right, result)
}
