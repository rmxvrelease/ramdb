// ramdb-spam bombardeia o servidor com requests aleatórios (SET/GET/DEL) e
// mostra, a cada segundo, quantos foram enviados e a latência vista pelo
// cliente. Ctrl+C encerra e imprime o total.
//
// Uso: go run ./cmd/ramdb-spam [-addr :8000] [-c 10] [-d 10s] [-rate 0]
//
//	[-keys 1000] [-value-size 32] [-set 30] [-get 60] [-del 10]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"ramdb/client"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Contadores compartilhados pelos workers. Cada segundo o loop principal faz
// Swap(0) para imprimir só o que aconteceu naquele segundo.
type counters struct {
	ok           atomic.Int64 // resposta de sucesso
	app_err      atomic.Int64 // o servidor respondeu com erro (ex: chave não existe)
	full         atomic.Int64 // servidor sem workers livres, conexão recusada
	net_err      atomic.Int64 // falha de conexão, escrita ou leitura
	acc_rt_micro atomic.Int64 // soma do round-trip de todos os requests
}

type totals struct {
	ok, app_err, full, net_err, acc_rt_micro int64
}

func (self *counters) swap() totals {
	return totals{
		ok:           self.ok.Swap(0),
		app_err:      self.app_err.Swap(0),
		full:         self.full.Swap(0),
		net_err:      self.net_err.Swap(0),
		acc_rt_micro: self.acc_rt_micro.Swap(0),
	}
}

func (self *totals) add(other totals) {
	self.ok += other.ok
	self.app_err += other.app_err
	self.full += other.full
	self.net_err += other.net_err
	self.acc_rt_micro += other.acc_rt_micro
}

func (self totals) sent() int64 {
	return self.ok + self.app_err + self.full + self.net_err
}

func (self totals) String() string {
	avg := int64(0)
	if sent := self.sent(); sent > 0 {
		avg = self.acc_rt_micro / sent
	}
	return fmt.Sprintf("enviados %7d | ok %7d | erro app %6d | full %6d | erro rede %6d | rt médio %6d µs",
		self.sent(), self.ok, self.app_err, self.full, self.net_err, avg)
}

type config struct {
	addr       string
	keys       int
	value_size int
	set_weight int
	get_weight int
	del_weight int
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":8000", "endereço do servidor")
	concurrency := flag.Int("c", 10, "número de workers enviando requests em paralelo")
	duration := flag.Duration("d", 10*time.Second, "duração do teste (0 = até Ctrl+C)")
	rate := flag.Int("rate", 0, "limite total de requests por segundo (0 = sem limite)")
	flag.IntVar(&cfg.keys, "keys", 1000, "quantidade de chaves distintas usadas")
	flag.IntVar(&cfg.value_size, "value-size", 32, "tamanho em bytes dos valores do SET")
	flag.IntVar(&cfg.set_weight, "set", 30, "peso relativo de SET")
	flag.IntVar(&cfg.get_weight, "get", 60, "peso relativo de GET")
	flag.IntVar(&cfg.del_weight, "del", 10, "peso relativo de DEL")
	flag.Parse()
	if *concurrency < 1 || cfg.keys < 1 || cfg.value_size < 0 || *rate < 0 {
		log.Fatal("-c e -keys precisam ser >= 1; -value-size e -rate precisam ser >= 0")
	}
	if cfg.set_weight < 0 || cfg.get_weight < 0 || cfg.del_weight < 0 || cfg.set_weight+cfg.get_weight+cfg.del_weight == 0 {
		log.Fatal("os pesos -set, -get e -del precisam ser >= 0 e somar mais que 0")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		ctx, stop = context.WithTimeout(ctx, *duration)
		defer stop()
	}

	// Com -rate, um ticker distribui permissões e cada request consome uma.
	// Sem -rate, o canal fica nil e os workers não esperam.
	var permits chan struct{}
	if *rate > 0 {
		permits = make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Second / time.Duration(*rate))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					select {
					case permits <- struct{}{}:
					default: // todos os workers ocupados: o request é perdido
					}
				}
			}
		}()
	}

	var stats counters
	var wg sync.WaitGroup
	for i := range *concurrency {
		wg.Go(func() { worker(ctx, cfg, uint64(i), permits, &stats) })
	}

	fmt.Printf("ramdb-spam → %s | %d workers | rate %s | duração %s\n",
		cfg.addr, *concurrency, rateLabel(*rate), durationLabel(*duration))

	var total totals
	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			sec := stats.swap()
			total.add(sec)
			fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), sec)
		}
	}

	wg.Wait()
	total.add(stats.swap())
	elapsed := time.Since(start)
	fmt.Printf("\ntotal em %s: %s | %.0f req/s\n",
		elapsed.Round(time.Millisecond), total, float64(total.sent())/elapsed.Seconds())
}

func worker(ctx context.Context, cfg config, id uint64, permits chan struct{}, stats *counters) {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), id))
	c := client.New(cfg.addr) // o client não é seguro para uso concorrente: um por worker
	value := make([]byte, cfg.value_size)
	total_weight := cfg.set_weight + cfg.get_weight + cfg.del_weight

	for {
		if permits != nil {
			select {
			case <-ctx.Done():
				return
			case <-permits:
			}
		} else if ctx.Err() != nil {
			return
		}

		key := "key-" + strconv.Itoa(rng.IntN(cfg.keys))
		var err error
		begin := time.Now()
		switch pick := rng.IntN(total_weight); {
		case pick < cfg.set_weight:
			for i := range value {
				value[i] = 'a' + byte(rng.IntN(26))
			}
			err = c.Put(key, value)
		case pick < cfg.set_weight+cfg.get_weight:
			_, err = c.Get(key)
		default:
			err = c.Del(key)
		}
		stats.acc_rt_micro.Add(time.Since(begin).Microseconds())

		// O client só devolve erros como texto, então a classificação é pelo
		// prefixo da mensagem (ver client/client.go).
		switch {
		case err == nil:
			stats.ok.Add(1)
		case strings.Contains(err.Error(), "sem conexões disponíveis"):
			stats.full.Add(1)
		case strings.HasPrefix(err.Error(), "Falha na requisição"):
			stats.app_err.Add(1)
		default:
			stats.net_err.Add(1)
		}
	}
}

func rateLabel(rate int) string {
	if rate == 0 {
		return "sem limite"
	}
	return strconv.Itoa(rate) + "/s"
}

func durationLabel(d time.Duration) string {
	if d == 0 {
		return "até Ctrl+C"
	}
	return d.String()
}
