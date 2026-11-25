// ramdb-top acompanha o benchmark.log escrito pelo servidor e mostra as
// métricas por segundo em tempo real. Roda como um processo separado: a única
// ligação com o servidor é o formato das linhas SEC_AVG e DROPPED do log.
//
// Uso: go run ./cmd/ramdb-top [-i benchmark.log] [-window 60] [-from-start]
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type secondStats struct {
	requests   int64
	avg_micros int64
	dropped    int64
}

const table_rows = 15

// Sequências ANSI. A tela alternativa é a mesma que top e less usam: o que é
// desenhado nela não vai para o scrollback e some quando o programa sai.
const (
	enter_alt_screen = "\033[?1049h\033[?25l" // entra na tela alternativa e esconde o cursor
	leave_alt_screen = "\033[?25h\033[?1049l" // mostra o cursor e volta à tela normal
	cursor_home      = "\033[H"
	clear_line_end   = "\033[K" // limpa do cursor até o fim da linha
	clear_screen_end = "\033[J" // limpa do cursor até o fim da tela
)

// fatalf restaura o terminal antes de sair, senão ele fica na tela
// alternativa e sem cursor.
func fatalf(format string, args ...any) {
	os.Stdout.WriteString(leave_alt_screen)
	log.Fatalf(format, args...)
}

func main() {
	path_flag := flag.String("i", "benchmark.log", "caminho do log escrito pelo servidor")
	window := flag.Int("window", 60, "quantos segundos manter na janela")
	from_start := flag.Bool("from-start", false, "lê o log desde o início em vez de só as linhas novas")
	flag.Parse()
	if *window < 1 {
		log.Fatal("-window precisa ser >= 1")
	}
	path := *path_flag

	os.Stdout.WriteString(enter_alt_screen)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		os.Stdout.WriteString(leave_alt_screen)
		os.Exit(0)
	}()

	file, was_created := openWhenExists(path)
	defer file.Close()

	// Se o arquivo ainda não existia, tudo nele é novo: lê desde o início.
	var offset int64
	if !*from_start && !was_created {
		var err error
		if offset, err = file.Seek(0, io.SeekEnd); err != nil {
			fatalf("falha ao ir para o fim do log: %v", err)
		}
	}
	reader := bufio.NewReader(file)

	stats := map[int64]*secondStats{}
	var last_sec int64
	pending := "" // pedaço de linha lido antes do servidor terminar de escrevê-la
	dirty := true

	for {
		chunk, err := reader.ReadString('\n')
		offset += int64(len(chunk))
		pending += chunk
		if err == nil {
			if sec, ok := parseLine(pending, stats); ok {
				last_sec = max(last_sec, sec)
				dirty = true
			}
			pending = ""
			continue
		}
		if err != io.EOF {
			fatalf("falha ao ler o log: %v", err)
		}

		// Chegou ao fim do que já foi escrito: desenha e espera mais linhas.
		if dirty {
			for sec := range stats {
				if sec <= last_sec-int64(*window) {
					delete(stats, sec)
				}
			}
			render(path, stats, last_sec, *window)
			dirty = false
		}
		time.Sleep(200 * time.Millisecond)

		// Se o log foi truncado, recomeça do início.
		if info, err := file.Stat(); err == nil && info.Size() < offset {
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				fatalf("falha ao voltar ao início do log: %v", err)
			}
			reader.Reset(file)
			offset, pending, last_sec, dirty = 0, "", 0, true
			clear(stats)
		}
	}
}

// openWhenExists espera o arquivo existir. O bool indica se foi preciso esperar.
func openWhenExists(path string) (*os.File, bool) {
	waiting := false
	for {
		file, err := os.Open(path)
		if err == nil {
			return file, waiting
		}
		if !errors.Is(err, fs.ErrNotExist) {
			fatalf("falha ao abrir %s: %v", path, err)
		}
		if !waiting {
			fmt.Printf("aguardando %s ser criado...\n", path)
			waiting = true
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// parseLine lê linhas no formato:
//
//	SEC_AVG | <segundo> | <requests> | <latência média µs>
//	DROPPED | <segundo> | <reports descartados>
//
// e devolve o segundo da linha. Outras linhas (REQ, lixo) são ignoradas.
func parseLine(line string, stats map[int64]*secondStats) (int64, bool) {
	fields := strings.Split(strings.TrimSpace(line), " | ")
	nums := make([]int64, len(fields)-1)
	for i, f := range fields[1:] {
		n, err := strconv.ParseInt(f, 10, 64)
		if err != nil {
			return 0, false
		}
		nums[i] = n
	}

	get := func(sec int64) *secondStats {
		s, ok := stats[sec]
		if !ok {
			s = &secondStats{}
			stats[sec] = s
		}
		return s
	}

	switch {
	case fields[0] == "SEC_AVG" && len(nums) == 3:
		s := get(nums[0])
		s.requests = nums[1]
		s.avg_micros = nums[2]
		return nums[0], true
	case fields[0] == "DROPPED" && len(nums) == 2:
		get(nums[0]).dropped += nums[1]
		return nums[0], true
	default:
		return 0, false
	}
}

func render(path string, stats map[int64]*secondStats, last_sec int64, window int) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "ramdb-top — %s\n\n", path)
	if last_sec == 0 {
		sb.WriteString("aguardando dados...\n")
		draw(sb.String())
		return
	}

	first_sec := last_sec - int64(window) + 1
	reqs := make([]int64, window)
	avgs := make([]int64, window)
	var total_reqs, total_delay, total_dropped, worst_avg, active_secs int64
	for i := range window {
		s, ok := stats[first_sec+int64(i)]
		if !ok {
			continue
		}
		reqs[i] = s.requests
		avgs[i] = s.avg_micros
		total_reqs += s.requests
		total_delay += s.requests * s.avg_micros
		total_dropped += s.dropped
		worst_avg = max(worst_avg, s.avg_micros)
		if s.requests > 0 {
			active_secs++
		}
	}

	fmt.Fprintf(&sb, "últimos %ds   req/s médio: %d   latência média: %d µs   pior segundo: %d µs   dropped: %d\n\n",
		window, safeDiv(total_reqs, active_secs), safeDiv(total_delay, total_reqs), worst_avg, total_dropped)
	fmt.Fprintf(&sb, "req/s   %s\n", sparkline(reqs))
	fmt.Fprintf(&sb, "avg µs  %s\n\n", sparkline(avgs))

	fmt.Fprintf(&sb, "%-10s %10s %10s %10s\n", "hora", "req/s", "avg µs", "dropped")
	for sec := last_sec; sec > last_sec-table_rows && sec >= first_sec; sec-- {
		s, ok := stats[sec]
		if !ok {
			s = &secondStats{}
		}
		fmt.Fprintf(&sb, "%-10s %10d %10d %10d\n",
			time.Unix(sec, 0).Format("15:04:05"), s.requests, s.avg_micros, s.dropped)
	}
	draw(sb.String())
}

// draw sobrescreve o frame anterior em vez de apagar a tela inteira: volta o
// cursor ao topo, limpa o resto de cada linha escrita e depois tudo o que
// sobrou abaixo do frame novo. Tudo vai numa única escrita, sem piscar.
func draw(frame string) {
	os.Stdout.WriteString(cursor_home + strings.ReplaceAll(frame, "\n", clear_line_end+"\n") + clear_screen_end)
}

func safeDiv(a, b int64) int64 {
	if b == 0 {
		return 0
	}
	return a / b
}

// sparkline desenha um caractere por valor, com altura proporcional ao maior
// valor da série. Zero vira espaço, para segundos sem dados ficarem visíveis.
func sparkline(values []int64) string {
	levels := []rune("▁▂▃▄▅▆▇█")
	var max_v int64
	for _, v := range values {
		max_v = max(max_v, v)
	}
	var sb strings.Builder
	for _, v := range values {
		if v <= 0 {
			sb.WriteRune(' ')
			continue
		}
		sb.WriteRune(levels[v*int64(len(levels)-1)/max_v])
	}
	return sb.String()
}
