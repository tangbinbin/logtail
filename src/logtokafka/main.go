package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
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
	filename          = flag.String("f", "", "input file name")
	mode              = flag.String("m", "polling", "polling or notify")
	brokers           = flag.String("b", "10.121.91.6:9092,10.121.92.4:9092", "kakfa brokers")
	topic             = flag.String("t", "test", "kafka topic")
	prefix            = flag.String("prefix", "", "only line begins with prefix")
	containStrings    = flag.String("contains", "", "Split by ','")
	containRelation   = flag.String("containRelation", "or", "one of (and,or)")
	notContainStrings = flag.String("notcontains", "", "Split by ',' and")
	flush             = flag.Int("n", 200, "number message flush to kafka")
	buffer            = flag.Int("s", 128, "bufio size kbyte")
	addmetadata       = flag.Bool("addmetadata", false, "add hostname and filename")
	empty             = []byte("")
)

type Ttail struct {
	filename        string
	topic           string
	bufferSize      int
	producer        sarama.AsyncProducer
	size            int64
	where           int
	containRelation string
	filenameB       []byte
	hostname        []byte
	hasprefix       []byte
	contains        [][]byte
	notcontains     [][]byte
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
	} else {
		log.Fatal("no file input")
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
	config.Producer.Flush.Messages = *flush
	config.Producer.RequiredAcks = 0
	producer, _ := sarama.NewAsyncProducer(strings.Split(*brokers, ","), config)

	t := &Ttail{
		topic:      *topic,
		bufferSize: *buffer,
		producer:   producer,
		where:      os.SEEK_END,
	}
	if *mode == "polling" {
		t.filename = *filename
	} else {
		t.filename = parseFilename(*filename)
	}
	t.filenameB = []byte(t.filename)
	host, _ := os.Hostname()
	t.hostname = []byte(host)
	t.hasprefix = []byte(*prefix)

	for _, i := range strings.Split(*containStrings, ",") {
		t.contains = append(t.contains, []byte(i))
	}

	for _, j := range strings.Split(*notContainStrings, ",") {
		t.notcontains = append(t.notcontains, []byte(j))
	}

	t.containRelation = *containRelation
	return t
}

func (t *Ttail) run() {
	go func() {
		for err := range t.producer.Errors() {
			log.Println(err)
		}
	}()

	if *mode == "polling" {
		t.runOnPolling()
	}
	t.runOnNotify()
}

func (t *Ttail) runOnPolling() {
	for {
		f, err := os.Open(t.filename)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}

		ret, _ := f.Seek(0, t.where)
		t.size = ret
		br := bufio.NewReaderSize(f, 1024*t.bufferSize)

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

			if !t.parseLine(line) {
				continue
			}

			msg := &sarama.ProducerMessage{Topic: t.topic}
			if *addmetadata {
				msg.Value = sarama.ByteEncoder(
					bytes.Join([][]byte{
						t.hostname,
						t.filenameB,
						line[:(lsize - 1)]}, []byte{32},
					),
				)
			} else {
				msg.Value = sarama.ByteEncoder(line[:(lsize - 1)])
			}

			t.producer.Input() <- msg
			t.size = t.size + int64(lsize)
		}
	}
}

func (t *Ttail) runOnNotify() {
	for {
		f, err := os.Open(t.filename)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}

		f.Seek(0, t.where)
		br := bufio.NewReaderSize(f, 1024*t.bufferSize)

		for {
			line, err := br.ReadBytes('\n')

			if err == io.EOF {
				time.Sleep(time.Second)
				newFilename := parseFilename(*filename)
				if newFilename != t.filename {
					log.Printf(
						"close old file %s and open newfile %s\n",
						t.filename,
						newFilename,
					)
					f.Close()
					t.filename = newFilename
					t.where = os.SEEK_SET
					break
				}
				continue
			}

			if err != nil {
				f.Close()
				time.Sleep(5 * time.Second)
				break
			}

			if !t.parseLine(line) {
				continue
			}

			msg := &sarama.ProducerMessage{Topic: t.topic}
			if *addmetadata {
				msg.Value = sarama.ByteEncoder(
					bytes.Join([][]byte{
						t.hostname,
						t.filenameB,
						line[:(len(line) - 1)]}, []byte{32},
					),
				)
			} else {
				msg.Value = sarama.ByteEncoder(line[:(len(line) - 1)])
			}

			t.producer.Input() <- msg
		}
	}
}

func parseFilename(filename string) (f string) {
	t := time.Now()
	year, month, day := t.Date()
	hour := t.Hour()

	f = filename

	if strings.Contains(f, "YYYY") {
		f = fmt.Sprintf(
			strings.Replace(f, "YYYY", "%d", -1),
			year,
		)
	}

	if strings.Contains(f, "MM") {
		f = fmt.Sprintf(
			strings.Replace(f, "MM", "%s", -1),
			parseInt(int(month)),
		)
	}

	if strings.Contains(f, "DD") {
		f = fmt.Sprintf(
			strings.Replace(f, "DD", "%s", -1),
			parseInt(day),
		)
	}

	if strings.Contains(f, "HH") {
		f = fmt.Sprintf(
			strings.Replace(f, "HH", "%s", -1),
			parseInt(hour),
		)
	}

	return
}

func parseInt(i int) string {
	if i < 10 {
		return fmt.Sprintf("0%d", i)
	}
	return fmt.Sprintf("%d", i)
}

func (t *Ttail) parseLine(line []byte) bool {
	if hasPrefix(line, t.hasprefix) &&
		contains(line, t.containRelation, t.contains) &&
		notcontains(line, t.notcontains) {
		return true
	}
	return false
}

func hasPrefix(line []byte, prefix []byte) bool {
	if bytes.Equal(prefix, empty) {
		return true
	}
	return bytes.HasPrefix(line, prefix)
}

func contains(line []byte, t string, c [][]byte) bool {
	if t == "or" {
		for _, s := range c {
			if bytes.Contains(line, s) {
				return true
			}
		}
		return false
	}
	if t == "and" {
		for _, s := range c {
			if !bytes.Contains(line, s) {
				return false
			}
		}
		return true
	}
	return false
}

func notcontains(line []byte, c [][]byte) bool {
	for _, s := range c {
		if bytes.Equal(s, empty) {
			continue
		}
		if bytes.Contains(line, s) {
			return false
		}
	}
	return true
}
