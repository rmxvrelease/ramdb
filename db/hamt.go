package db

import (
	"bytes"
	"hash/maphash"
	"math/bits"
	"slices"
	"sync"
)

// HAMT (Hash Array Mapped Trie) é um mapa sem ordem de chaves. Cada nível usa 5
// bits do hash da chave para escolher um de 32 filhos, então a profundidade fica
// em ~log32(n): 4 níveis para 1 milhão de chaves. Comparamos a chave inteira só
// uma vez, na folha.
//
// A raiz é dividida em 32 shards pelos 5 primeiros bits do hash. Uma chave nunca
// muda de shard, e nenhuma operação (divisão ou colapso de nós) cruza a fronteira
// entre eles, então cada shard é uma sub-árvore independente com o próprio
// RWMutex: escritas em shards diferentes rodam em paralelo, e uma escrita só
// bloqueia os Get do próprio shard.
type HAMT struct {
	shards [hamtShards]hamtShard
	seed   maphash.Seed
}

type hamtShard struct {
	mu   sync.RWMutex
	root hamtNode
	size int
}

const (
	hamtBitsPerLevel = 5
	hamtLevelMask    = 1<<hamtBitsPerLevel - 1
	hamtShards       = 1 << hamtBitsPerLevel
)

// hamtNode guarda só os filhos que existem. O bit i do bitmap diz se o filho de
// índice i existe; a posição dele em children é a quantidade de bits ligados
// abaixo de i (popcount).
type hamtNode struct {
	bitmap   uint32
	children []hamtChild
}

// hamtChild é uma subárvore (node) ou uma folha (leaf), nunca os dois.
type hamtChild struct {
	node *hamtNode
	leaf *hamtLeaf
}

// hamtLeaf guarda as chaves com exatamente o mesmo hash de 64 bits. Quase sempre
// é uma só; mais de uma só acontece em colisão total, quando não sobram bits para
// separá-las.
type hamtLeaf struct {
	hash  uint64
	pairs []hamtPair
}

type hamtPair struct {
	key   []byte
	value []byte
}

// NewHAMT cria um HAMT vazio. A semente do hash é aleatória por processo, para que
// ninguém consiga escolher chaves que caiam todas no mesmo caminho.
func NewHAMT() *HAMT {
	return &HAMT{seed: maphash.MakeSeed()}
}

func (t *HAMT) hash(key []byte) uint64 {
	return maphash.Bytes(t.seed, key)
}

// shard devolve o shard da chave. Os nós dentro dele começam a usar o hash a
// partir do bit hamtBitsPerLevel.
func (t *HAMT) shard(h uint64) *hamtShard {
	return &t.shards[h&hamtLevelMask]
}

// Len devolve a quantidade de chaves guardadas. Os shards são lidos um por vez,
// então com escritas concorrentes o total é aproximado.
func (t *HAMT) Len() int {
	total := 0
	for i := range t.shards {
		s := &t.shards[i]
		s.mu.RLock()
		total += s.size
		s.mu.RUnlock()
	}
	return total
}

func (t *HAMT) Get(key []byte) ([]byte, error) {
	return t.getHashed(t.hash(key), key)
}

func (t *HAMT) Put(key []byte, value []byte) {
	t.putHashed(t.hash(key), key, value)
}

// Delete remove a chave e devolve se ela existia.
func (t *HAMT) Delete(key []byte) bool {
	return t.deleteHashed(t.hash(key), key)
}

// As versões *Hashed recebem o hash pronto para que os testes consigam forçar
// colisões e prefixos compartilhados.

func (t *HAMT) getHashed(h uint64, key []byte) ([]byte, error) {
	s := t.shard(h)
	s.mu.RLock()
	defer s.mu.RUnlock()

	node := &s.root
	for shift := uint(hamtBitsPerLevel); ; shift += hamtBitsPerLevel {
		bit, pos := node.slot(h, shift)
		if node.bitmap&bit == 0 {
			return nil, ErrKeyNotFound
		}

		child := node.children[pos]
		if child.node != nil {
			node = child.node
			continue
		}

		if child.leaf.hash == h {
			for _, p := range child.leaf.pairs {
				if bytes.Equal(p.key, key) {
					return p.value, nil
				}
			}
		}
		return nil, ErrKeyNotFound
	}
}

func (t *HAMT) putHashed(h uint64, key []byte, value []byte) {
	s := t.shard(h)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.root.put(h, hamtBitsPerLevel, key, value) {
		s.size++
	}
}

func (t *HAMT) deleteHashed(h uint64, key []byte) bool {
	s := t.shard(h)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.root.delete(h, hamtBitsPerLevel, key) {
		s.size--
		return true
	}
	return false
}

// slot calcula o bit do filho neste nível e a posição dele em children.
func (n *hamtNode) slot(h uint64, shift uint) (bit uint32, pos int) {
	bit = 1 << ((h >> shift) & hamtLevelMask)
	pos = bits.OnesCount32(n.bitmap & (bit - 1))
	return bit, pos
}

// put devolve true se a chave é nova e false se só atualizou o valor.
func (n *hamtNode) put(h uint64, shift uint, key, value []byte) bool {
	bit, pos := n.slot(h, shift)

	if n.bitmap&bit == 0 {
		leaf := &hamtLeaf{hash: h, pairs: []hamtPair{{key, value}}}
		n.children = slices.Insert(n.children, pos, hamtChild{leaf: leaf})
		n.bitmap |= bit
		return true
	}

	child := &n.children[pos]
	if child.node != nil {
		return child.node.put(h, shift+hamtBitsPerLevel, key, value)
	}

	leaf := child.leaf
	if leaf.hash == h {
		for i := range leaf.pairs {
			if bytes.Equal(leaf.pairs[i].key, key) {
				leaf.pairs[i].value = value
				return false
			}
		}
		leaf.pairs = append(leaf.pairs, hamtPair{key, value})
		return true
	}

	// Hashes diferentes no mesmo slot: a folha desce para um nó novo e as duas
	// chaves são separadas pelos próximos bits. Como os hashes diferem em algum
	// bit, isso termina antes de acabarem os 64 bits.
	sub := &hamtNode{}
	subBit, _ := sub.slot(leaf.hash, shift+hamtBitsPerLevel)
	sub.bitmap = subBit
	sub.children = []hamtChild{{leaf: leaf}}
	sub.put(h, shift+hamtBitsPerLevel, key, value)

	*child = hamtChild{node: sub}
	return true
}

// delete devolve true se a chave existia. Na volta da recursão, um nó que ficou
// com uma única folha é trocado por ela, para que a árvore não guarde caminhos
// que não separam mais nada.
func (n *hamtNode) delete(h uint64, shift uint, key []byte) bool {
	bit, pos := n.slot(h, shift)
	if n.bitmap&bit == 0 {
		return false
	}

	child := &n.children[pos]
	if child.node != nil {
		if !child.node.delete(h, shift+hamtBitsPerLevel, key) {
			return false
		}
		sub := child.node
		switch {
		case len(sub.children) == 0:
			n.removeSlot(bit, pos)
		case len(sub.children) == 1 && sub.children[0].leaf != nil:
			*child = sub.children[0]
		}
		return true
	}

	leaf := child.leaf
	if leaf.hash != h {
		return false
	}
	i := slices.IndexFunc(leaf.pairs, func(p hamtPair) bool { return bytes.Equal(p.key, key) })
	if i < 0 {
		return false
	}
	leaf.pairs = slices.Delete(leaf.pairs, i, i+1)
	if len(leaf.pairs) == 0 {
		n.removeSlot(bit, pos)
	}
	return true
}

func (n *hamtNode) removeSlot(bit uint32, pos int) {
	n.children = slices.Delete(n.children, pos, pos+1)
	n.bitmap &^= bit
}
