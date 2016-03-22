lottail

## 使用说明
- logtokafka 收集log写入mysql
- logtomysql 收集log写入kafka
- go1.5

## logtail
    ./bin/logtokafka -h
    Usage of ./bin/logtokafka:
        -addmetadata
            add hostname and filename (default true)
            如果true 则kafka格式为 $Host $filename $line
            如果false 则为 $line
        -b string
            kakfa brokers (default "10.121.91.6:9092,10.121.92.4:9092")
        -containRelation string
            one of (and,or) (default "or")
            包含字符串的关联关系
        -contains string
            Split by ','
            包含这些字符串的行才会被收集，多个之间用，分隔
        -f string
            input file name
            文件名，可以加上日期通配
            a.log
            a.log.YYYYMMDDHH
            a.YYYY-MM-DD.log
        -m string
            polling or notify (default "polling")
            打开日志文件的模式
            如果是精确的日志名选择 polling 模式
            如果含日期通配则选择 notify 模式
        -n int
            number message flush to kafka (default 200)
            每次往kafka写的msg条数
        -notcontains string
            Split by ',' and
            包含这些字符串的行将不被收集，多个用，分隔
        -prefix string
            only line begins with prefix
            只收集以 prefix 为开头的行
        -s int
            bufio size kbyte (default 128)
        -t string
            kafka topic (default "test")
