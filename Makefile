all: install

GOPATH:=$(CURDIR)
export GOPATH

dep:
	go get github.com/go-sql-driver/mysql
	go get github.com/Shopify/sarama

install:dep
	go install logtokafka
	go install logtomysql
