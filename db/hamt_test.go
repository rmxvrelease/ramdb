package db

import (
	"bytes"
	"fmt"
	"math/bits"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

var _ DBEngine = (*PureMemEngine)(nil)

// checarInvariantes percorre cada shard e falha se o bitmap não bater com os
// filhos, se sobrou algum nó que não separa nada (vazio ou com uma única folha) ou
// se alguma chave está no shard errado.
func checarInvariantes(t *testing.T, tree *HAMT) {
	t.Helper()
	var shardAtual uint64
	var contar func(n *hamtNode, raiz bool) int
	contar = func(n *hamtNode, raiz bool) int {
		if bits.OnesCount32(n.bitmap) != len(n.children) {
			t.Fatalf("bitmap com %d bits, mas %d filhos", bits.OnesCount32(n.bitmap), len(n.children))
		}
		if !raiz && len(n.children) == 0 {
			t.Fatalf("nó interno vazio")
		}
		if !raiz && len(n.children) == 1 && n.children[0].leaf != nil {
			t.Fatalf("nó interno com uma única folha não foi colapsado")
		}
		total := 0
		for _, c := range n.children {
			if (c.node == nil) == (c.leaf == nil) {
				t.Fatalf("filho precisa ser nó ou folha, exatamente um dos dois")
			}
			if c.node != nil {
				total += contar(c.node, false)
			} else {
				if c.leaf.hash&hamtLevelMask != shardAtual {
					t.Fatalf("folha com hash %x está no shard %d", c.leaf.hash, shardAtual)
				}
				total += len(c.leaf.pairs)
			}
		}
		return total
	}
	for i := range tree.shards {
		s := &tree.shards[i]
		shardAtual = uint64(i)
		if total := contar(&s.root, true); total != s.size {
			t.Fatalf("shard %d tem %d chaves, mas size diz %d", i, total, s.size)
		}
	}
}

func TestHAMT_PutGetDelete(t *testing.T) {
	tree := NewHAMT()

	tree.Put([]byte("linguagem"), []byte("go"))
	val, err := tree.Get([]byte("linguagem"))
	if err != nil || !bytes.Equal(val, []byte("go")) {
		t.Fatalf("Esperava 'go', recebeu '%s' (%v)", val, err)
	}

	tree.Put([]byte("linguagem"), []byte("c++"))
	val, _ = tree.Get([]byte("linguagem"))
	if !bytes.Equal(val, []byte("c++")) {
		t.Errorf("Esperava 'c++' após atualização, recebeu '%s'", val)
	}
	if tree.Len() != 1 {
		t.Errorf("Tamanho deveria ser 1 após atualizar a mesma chave, mas é %d", tree.Len())
	}

	if !tree.Delete([]byte("linguagem")) {
		t.Errorf("Delete deveria encontrar a chave")
	}
	if tree.Delete([]byte("linguagem")) {
		t.Errorf("Segundo Delete não deveria encontrar a chave")
	}
	if _, err := tree.Get([]byte("linguagem")); err != ErrKeyNotFound {
		t.Errorf("Esperava ErrKeyNotFound, recebeu: %v", err)
	}
	checarInvariantes(t, tree)
}

func TestHAMT_ColisaoTotal(t *testing.T) {
	tree := NewHAMT()
	const h = 0xDEADBEEF

	tree.putHashed(h, []byte("a"), []byte("1"))
	tree.putHashed(h, []byte("b"), []byte("2"))
	tree.putHashed(h, []byte("c"), []byte("3"))
	checarInvariantes(t, tree)

	for k, v := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		got, err := tree.getHashed(h, []byte(k))
		if err != nil || string(got) != v {
			t.Errorf("chave %s: esperava %s, recebeu '%s' (%v)", k, v, got, err)
		}
	}
	if _, err := tree.getHashed(h, []byte("d")); err != ErrKeyNotFound {
		t.Errorf("chave ausente com o mesmo hash deveria dar ErrKeyNotFound, recebeu %v", err)
	}

	tree.deleteHashed(h, []byte("b"))
	if _, err := tree.getHashed(h, []byte("b")); err != ErrKeyNotFound {
		t.Errorf("b deveria ter sido apagada")
	}
	if got, _ := tree.getHashed(h, []byte("c")); string(got) != "3" {
		t.Errorf("c deveria continuar lá, recebeu '%s'", got)
	}
	tree.deleteHashed(h, []byte("a"))
	tree.deleteHashed(h, []byte("c"))
	if raiz := tree.shard(h).root; len(raiz.children) != 0 {
		t.Errorf("raiz do shard deveria ficar vazia, tem %d filhos", len(raiz.children))
	}
	checarInvariantes(t, tree)
}

func TestHAMT_PrefixoCompartilhado_Colapsa(t *testing.T) {
	tree := NewHAMT()
	// Os dois hashes só diferem no bit 62, então caem no mesmo shard, dividem os
	// níveis seguintes e a folha precisa descer até o último.
	h1 := uint64(0x0123456789ABCDEF) &^ (1 << 62)
	h2 := h1 | (1 << 62)

	tree.putHashed(h1, []byte("x"), []byte("1"))
	tree.putHashed(h2, []byte("y"), []byte("2"))
	checarInvariantes(t, tree)

	if got, _ := tree.getHashed(h1, []byte("x")); string(got) != "1" {
		t.Errorf("x: esperava 1, recebeu '%s'", got)
	}
	if got, _ := tree.getHashed(h2, []byte("y")); string(got) != "2" {
		t.Errorf("y: esperava 2, recebeu '%s'", got)
	}

	tree.deleteHashed(h1, []byte("x"))
	checarInvariantes(t, tree)
	if c := tree.shard(h2).root.children; len(c) != 1 || c[0].leaf == nil {
		t.Fatalf("a cadeia de nós deveria ter colapsado em uma folha na raiz do shard")
	}
	if got, _ := tree.getHashed(h2, []byte("y")); string(got) != "2" {
		t.Errorf("y deveria sobreviver ao colapso, recebeu '%s'", got)
	}
}

// Compara o HAMT com um map comum numa sequência aleatória de operações. Os
// hashes só usam 10 bits para forçar muitas colisões e divisões de nós.
func TestHAMT_ComparadoComMap(t *testing.T) {
	tree := NewHAMT()
	esperado := map[string]string{}
	rng := rand.New(rand.NewPCG(1, 2))
	hashFraco := func(k []byte) uint64 { return tree.hash(k) & 0x3FF }

	for i := range 50000 {
		k := fmt.Appendf(nil, "k%d", rng.IntN(3000))
		h := hashFraco(k)
		switch rng.IntN(3) {
		case 0, 1:
			v := fmt.Sprintf("v%d", i)
			tree.putHashed(h, k, []byte(v))
			esperado[string(k)] = v
		case 2:
			_, existia := esperado[string(k)]
			if tree.deleteHashed(h, k) != existia {
				t.Fatalf("op %d: Delete(%s) discordou do map", i, k)
			}
			delete(esperado, string(k))
		}
	}

	checarInvariantes(t, tree)
	if tree.Len() != len(esperado) {
		t.Fatalf("HAMT tem %d chaves, map tem %d", tree.Len(), len(esperado))
	}
	for k, v := range esperado {
		got, err := tree.getHashed(hashFraco([]byte(k)), []byte(k))
		if err != nil || string(got) != v {
			t.Fatalf("chave %s: esperava %s, recebeu '%s' (%v)", k, v, got, err)
		}
	}
}

// Com o lock de escrita de um shard preso, operações em outro shard precisam
// terminar, e as do shard preso precisam esperar.
func TestHAMT_ShardsIndependentes(t *testing.T) {
	tree := NewHAMT()
	const hPreso, hLivre = 0, 1

	tree.shard(hPreso).mu.Lock()

	livre := make(chan struct{})
	go func() {
		tree.putHashed(hLivre, []byte("livre"), []byte("1"))
		tree.getHashed(hLivre, []byte("livre"))
		tree.deleteHashed(hLivre, []byte("livre"))
		close(livre)
	}()
	select {
	case <-livre:
	case <-time.After(time.Second):
		t.Fatal("operação em outro shard ficou bloqueada pelo lock do shard 0")
	}

	preso := make(chan struct{})
	go func() {
		tree.putHashed(hPreso, []byte("preso"), []byte("1"))
		close(preso)
	}()
	select {
	case <-preso:
		t.Fatal("Put no shard travado deveria ter esperado o lock")
	case <-time.After(50 * time.Millisecond):
	}

	tree.shard(hPreso).mu.Unlock()
	<-preso
	if got, err := tree.getHashed(hPreso, []byte("preso")); err != nil || string(got) != "1" {
		t.Errorf("Put no shard 0 deveria ter concluído após o unlock: '%s' (%v)", got, err)
	}
}

func TestPureMemEngine_Concorrencia(t *testing.T) {
	engine := NewPureMemEngine()
	var wg sync.WaitGroup

	for w := range 50 {
		wg.Go(func() {
			for i := range 1000 {
				k := fmt.Appendf(nil, "w%d_k%d", w, i)
				engine.Put(k, k)
				if got, err := engine.Get(k); err != nil || !bytes.Equal(got, k) {
					t.Errorf("leitura logo após escrita falhou para %s", k)
				}
				if i%2 == 0 {
					engine.Delete(k)
				}
			}
		})
	}
	wg.Wait()

	if engine.table.Len() != 50*500 {
		t.Errorf("Esperava %d chaves, encontrou %d", 50*500, engine.table.Len())
	}
	checarInvariantes(t, engine.table)
}

func TestPureMemEngine_NaoCompartilhaBuffers(t *testing.T) {
	engine := NewPureMemEngine()
	chave, valor := []byte("k"), []byte("v1")
	engine.Put(chave, valor)
	chave[0], valor[0] = 'Z', 'X'

	got, err := engine.Get([]byte("k"))
	if err != nil || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("buffers do chamador alteraram o banco: '%s' (%v)", got, err)
	}
	got[0] = 'Y'
	if again, _ := engine.Get([]byte("k")); !bytes.Equal(again, []byte("v1")) {
		t.Errorf("alterar o retorno do Get alterou o banco: '%s'", again)
	}
}
