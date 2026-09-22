package db

import (
	"hash/fnv"
	"math"
)

// BloomFilter responde "essa chave pode estar neste SSTable?" usando só bits na RAM.
// Quando diz "não", é garantido. Quando diz "sim", pode ser falso positivo.
// Depois de montado ele só é lido, então vários Gets podem consultá-lo ao mesmo tempo sem lock.
type BloomFilter struct {
	bits      []uint64 // cada uint64 guarda 64 bits do filtro
	numBits   uint64   // m: total de bits (sempre múltiplo de 64)
	numHashes uint64   // k: quantos bits cada chave liga
}

// NewBloomFilter dimensiona o filtro para expectedKeys chaves com a taxa de
// falso positivo pedida (0.01 = 1%).
func NewBloomFilter(expectedKeys int, falsePositiveRate float64) *BloomFilter {
	n := float64(max(expectedKeys, 1))

	// Fórmulas clássicas do filtro de Bloom:
	//   m = -n·ln(p) / (ln 2)²   (quantos bits)
	//   k = (m/n)·ln 2           (quantos hashes por chave)
	m := math.Ceil(-n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	k := max(math.Round(m/n*math.Ln2), 1)

	words := (uint64(m) + 63) / 64 // divide arredondando para cima
	return &BloomFilter{
		bits:      make([]uint64, words),
		numBits:   words * 64,
		numHashes: uint64(k),
	}
}

// Add liga os k bits da chave.
func (b *BloomFilter) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < b.numHashes; i++ {
		pos := (h1 + i*h2) % b.numBits
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

// MightContain devolve false só quando a chave com certeza nunca foi adicionada.
// Um filtro nil não sabe nada sobre o arquivo, então responde "talvez".
func (b *BloomFilter) MightContain(key []byte) bool {
	if b == nil {
		return true
	}
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < b.numHashes; i++ {
		pos := (h1 + i*h2) % b.numBits
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}

// bloomHashes gera as duas bases do double hashing (Kirsch-Mitzenmacher):
// a i-ésima posição é h1 + i·h2, o que dispensa ter k funções de hash diferentes.
func bloomHashes(key []byte) (h1, h2 uint64) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return sum & 0xffffffff, sum >> 32
}
