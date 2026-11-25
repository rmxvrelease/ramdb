package benchmarks

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// Códigos dos comandos. Só o código vai para o canal, não o corpo do request,
// para que cada report ocupe um tamanho fixo e pequeno.
const (
	CmdUnknown uint8 = iota
	CmdSet
	CmdGet
	CmdDel
)

var commandNames = [...]string{
	CmdUnknown: "UNKNOWN",
	CmdSet:     "SET",
	CmdGet:     "GET",
	CmdDel:     "DEL",
}

func commandCode(body []byte) uint8 {
	name, _, _ := bytes.Cut(bytes.TrimSpace(body), []byte(" "))
	switch {
	case bytes.EqualFold(name, []byte("SET")):
		return CmdSet
	case bytes.EqualFold(name, []byte("GET")):
		return CmdGet
	case bytes.EqualFold(name, []byte("DEL")):
		return CmdDel
	default:
		return CmdUnknown
	}
}

type commandReport struct {
	command   uint8
	received  time.Time
	responded time.Time
}

type BenchmarkClient struct {
	reqReportChan chan commandReport
	dropped       *atomic.Int64
}

func (self *BenchmarkClient) ReportRequest(body []byte, received, responded time.Time) {
	if self == nil {
		return
	}
	report := commandReport{
		command:   commandCode(body),
		received:  received,
		responded: responded,
	}
	select {
	case self.reqReportChan <- report:
	default:
		self.dropped.Add(1)
	}
}

type BenchmarkService struct {
	reqReportChan chan commandReport
	dropped       *atomic.Int64
	output_file   *os.File
}

func NewBenchmarkService() (BenchmarkService, error) {
	file, err := os.OpenFile("benchmark.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	return BenchmarkService{
		reqReportChan: make(chan commandReport, 100),
		dropped:       &atomic.Int64{},
		output_file:   file,
	}, err
}

func (self BenchmarkService) GetBenchmarkClient() *BenchmarkClient {
	return &BenchmarkClient{
		reqReportChan: self.reqReportChan,
		dropped:       self.dropped,
	}
}

func (self *BenchmarkService) Run() {

	type secondBucket struct {
		sec                       int64
		request_count             int64
		acc_response_delay_micros int64
	}

	// Escritas vão para um buffer em memória e só chegam ao arquivo quando ele
	// enche ou a cada tick, evitando uma syscall por request.
	out := bufio.NewWriterSize(self.output_file, 64*1024)
	write_line := func(format string, args ...any) {
		if _, err := fmt.Fprintf(out, format, args...); err != nil {
			log.Printf("falha ao escrever nos logs: %v", err)
		}
	}

	// Só existe o bucket do segundo atual. Reports de um segundo que já passou
	// entram no bucket atual mesmo assim: a imprecisão é aceitável aqui.
	curr := secondBucket{}
	flush := func() {
		if curr.request_count > 0 {
			write_line("SEC_AVG | %d | %d | %d\n", curr.sec, curr.request_count, curr.acc_response_delay_micros/curr.request_count)
		}
		curr = secondBucket{}
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			if curr.sec < now.Unix() {
				flush()
			}
			if dropped := self.dropped.Swap(0); dropped > 0 {
				write_line("DROPPED | %d | %d\n", now.Unix(), dropped)
			}
			if err := out.Flush(); err != nil {
				log.Printf("falha ao escrever nos logs: %v", err)
			}
		case report := <-self.reqReportChan:
			// Sub usa o relógio monotônico, imune a ajustes do relógio de parede.
			delay_micros := report.responded.Sub(report.received).Microseconds()
			write_line("REQ | %d | %d | %d | %s\n",
				report.received.UnixMicro(), report.responded.UnixMicro(), delay_micros, commandNames[report.command])

			if sec := report.received.Unix(); sec > curr.sec {
				flush()
				curr.sec = sec
			}
			curr.request_count += 1
			curr.acc_response_delay_micros += delay_micros
		}
	}
}
