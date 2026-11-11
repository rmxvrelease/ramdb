package db

import (
	"hash/fnv"
	"math"
)

type BloomFilter struct {
	bits      []uint64
	numBits   uint64
	numHashes uint64
}

func NewBloomFilter(expectedKeys int, falsePositiveRate float64) *BloomFilter {
	n := float64(max(expectedKeys, 1))

	m := math.Ceil(-n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2))
	k := max(math.Round(m/n*math.Ln2), 1)

	words := (uint64(m) + 63) / 64
	return &BloomFilter{
		bits:      make([]uint64, words),
		numBits:   words * 64,
		numHashes: uint64(k),
	}
}

func (b *BloomFilter) Add(key []byte) {
	h1, h2 := bloomHashes(key)
	for i := uint64(0); i < b.numHashes; i++ {
		pos := (h1 + i*h2) % b.numBits
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

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

func bloomHashes(key []byte) (h1, h2 uint64) {
	h := fnv.New64a()
	h.Write(key)
	sum := h.Sum64()
	return sum & 0xffffffff, sum >> 32
}
