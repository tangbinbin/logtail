package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"io"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	filename = flag.String("f", "", "input file name")
	addr     = flag.String("h", "127.0.0.1:3306", "mysql addr")
	user     = flag.String("u", "test", "mysql user")
	password = flag.String("p", "test", "mysql password")
	db       *sql.DB
)

type Ttail struct {
	filename string
	size     int64
	where    int
}

type Logger struct {
	hostname string
	filename string
	msg      string
}

func init() {
	flag.Parse()
	initDb()
}

func initDb() {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8&timeout=100ms",
		*user, *password, *addr, "log")
	var err error
	db, err = sql.Open("mysql", connStr)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(2)
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
	return &Ttail{
		filename: *filename,
		where:    os.SEEK_END,
	}
}

func (t *Ttail) run() {
	host, _ := os.Hostname()
	ch := make(chan Logger, 1000)

	go func() {
		for {
			l := <-ch
			exec(l)
		}
	}()

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
			line, err := br.ReadString('\n')
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

			l := Logger{
				hostname: host,
				filename: t.filename,
				msg:      line,
			}
			ch <- l

			t.size = t.size + int64(lsize)
		}
	}
}

func exec(l Logger) {
	db.Exec("insert into log (host, filename, msg) values (?, ?,?)",
		l.hostname, l.filename, l.msg)
}
