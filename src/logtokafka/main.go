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
	brokers           = flag.String("b", "127.0.0.1:9092,127.0.0.1:9093", "kafka brokers")
	topic             = flag.String("t", "test", "kafka topic")
	prefix            = flag.String("prefix", "", "only line begins with prefix")
	template          = flag.String("m", "LINE", "HOSTNAME FILENAME LINE")
	containStrings    = flag.String("contains", "", "Split by ','")
	containRelation   = flag.String("containRelation", "or", "one of (and,or)")
	notContainStrings = flag.String("notcontains", "", "Split by ',' and")
	flush             = flag.Int("n", 200, "number message flush to kafka")
	chsize            = flag.Int("cs", 1000, "size of buffer channel")
	buffer            = flag.Int("s", 128, "bufio size kbyte")
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
	mode            string
	template        string
	temCache        string
	addHost         bool
	addFilename     bool
	hostname        string
	ch              chan ([]byte)
	hasprefix       []byte
	contains        [][]byte
	notcontains     [][]byte
}

func init() {
	flag.Parse()
}

func main() {
	if runtime.NumCPU() > 2 {
		runtime.GOMAXPROCS(2)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
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
		topic:       *topic,
		bufferSize:  *buffer,
		producer:    producer,
		where:       os.SEEK_END,
		mode:        getMode(*filename),
		template:    *template,
		addHost:     strings.Contains(*template, "HOSTNAME"),
		addFilename: strings.Contains(*template, "FILENAME"),
	}

	if t.mode == "polling" {
		t.filename = *filename
	} else {
		t.filename = parseFilename(*filename)
	}
	host, _ := os.Hostname()
	t.hostname = host
	t.hasprefix = []byte(*prefix)

	for _, i := range strings.Split(*containStrings, ",") {
		t.contains = append(t.contains, []byte(i))
	}

	for _, j := range strings.Split(*notContainStrings, ",") {
		t.notcontains = append(t.notcontains, []byte(j))
	}

	t.containRelation = *containRelation
	t.ch = make(chan []byte, 1000)

	log.Println("tep", t.template)
	t.temCache = t.template
	if t.addHost {
		t.temCache = fmt.Sprintf(
			strings.Replace(t.temCache, "HOSTNAME", "%s", -1),
			t.hostname,
		)
	}

	if t.addFilename {
		t.temCache = strings.Replace(t.temCache, "FILENAME", "%s", -1)
		log.Println(t.temCache)
	} else {
		log.Println(t.temCache)
		t.temCache = strings.Replace(t.temCache, "LINE", "%s", -1)
		log.Println(t.temCache)
	}

	return t
}

func (t *Ttail) run() {
	go func() {
		for err := range t.producer.Errors() {
			log.Println(err)
		}
	}()

	go func() {
		for {
			line := <-t.ch
			msg := &sarama.ProducerMessage{
				Topic: t.topic,
				Value: sarama.ByteEncoder(t.parseLine(line)),
			}
			t.producer.Input() <- msg
		}
	}()

	if t.mode == "polling" {
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
				log.Println(err)
				f.Close()
				time.Sleep(5 * time.Second)
				break
			}

			if !t.rightful(line) {
				continue
			}

			t.ch <- line
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
				log.Println(err)
				f.Close()
				time.Sleep(5 * time.Second)
				break
			}

			if !t.rightful(line) {
				continue
			}

			t.ch <- line
		}
	}
}

func (t *Ttail) parseLine(line []byte) []byte {
	if t.addFilename {
		return []byte(fmt.Sprintf(
			strings.Replace(
				fmt.Sprintf(t.temCache, t.filename),
				"LINE", "%s", -1),
			line[:len(line)-1],
		))
	} else {
		return []byte(fmt.Sprintf(t.temCache, line[:len(line)-1]))
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

func (t *Ttail) rightful(line []byte) bool {
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

func getMode(f string) string {
	if strings.Contains(f, "YYYY") ||
		strings.Contains(f, "MM") ||
		strings.Contains(f, "DD") ||
		strings.Contains(f, "HH") {
		return "notify"
	}
	return "polling"
}
