package main

import (
	"bufio"
	"bytes"
	"flag"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"
)

var (
	filename = flag.String("f", "", "input file name")
	brokers  = flag.String("b", "127.0.0.1:9092,127.0.0.1:9093", "kakfa brokers")
	topic    = flag.String("t", "test", "kafka topic")
)

type Ttail struct {
	filename string
	topic    string
	producer sarama.AsyncProducer
	size     int64
	where    int
}

func init() {
	flag.Parse()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.Println("start")

	if *filename != "" {
		t := newTtail()
		go t.run()
	}

	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)
	<-exitChan
	log.Println("stop")
}

func newTtail() *Ttail {
	config := sarama.NewConfig()
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.Flush.Messages = 400
	config.Producer.RequiredAcks = 0
	producer, _ := sarama.NewAsyncProducer(strings.Split(*brokers, ","), config)

	return &Ttail{
		filename: *filename,
		topic:    *topic,
		producer: producer,
		where:    os.SEEK_END,
	}
}

func (t *Ttail) run() {
	go func() {
		for err := range t.producer.Errors() {
			log.Println(err)
		}
	}()

	host, _ := os.Hostname()
	h := []byte(host)
	fb := []byte(t.filename)

	for {
		f, err := os.Open(t.filename)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		ret, _ := f.Seek(0, t.where)
		t.size = ret
		br := bufio.NewReaderSize(f, 1024*128)

		for {
			line, err := br.ReadBytes('\n')
			lsize := len(line)

			if err == io.EOF {
				fi, err := os.Stat(t.filename)
				if os.IsNotExist(err) {
					log.Println(err)
					f.Close()
					t.where = os.SEEK_SET
					break
				}

				if t.size > fi.Size() {
					log.Println("runcate or reopened")
					f.Close()
					t.where = os.SEEK_SET
					break
				}
				time.Sleep(time.Second)
				continue
			}

			if err != nil {
				f.Close()
				time.Sleep(5 * time.Second)
				break
			}

			msg := &sarama.ProducerMessage{
				Topic: t.topic,
				Value: sarama.ByteEncoder(
					bytes.Join([][]byte{h, fb, line[:(lsize - 1)]},
						[]byte{32})),
			}
			t.producer.Input() <- msg

			t.size = t.size + int64(lsize)
		}
	}
}
