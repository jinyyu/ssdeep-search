package main

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"ssdeep_search"
)

var (
	db *sqlx.DB
)

type SsdeepRecord struct {
	MD5    string `db:"md5"`
	Ssdeep string `db:"ssdeep"`
}

func connectDB() *sqlx.DB {
	pgdb, err := sqlx.Connect("postgres", "user=ljy dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	return pgdb
}

func QueryRecords(md5 string) (results []SsdeepRecord) {
	err := db.Select(&results, "select * from all_changed WHERE MD5 > $1 order by md5 asc limit 100", md5)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln(err)
	}
	return
}

func ProcessRecord(record SsdeepRecord) (err error) {
	ssdeep, err := ssdeep_search.ParseSsdeep(record.Ssdeep)
	if err != nil {
		log.Fatalln("ParseSsdeep error ", err)
	}

	var md5s []string
	keys := ssdeep_search.GenerateKeys(ssdeep.HashBlockSize)
	if len(keys) > 0 {
		md5s = GetMD5S(ssdeep.BlockSize, keys)
	}

	keys = ssdeep_search.GenerateKeys(ssdeep.Hash2BlockSIze)
	if len(keys) > 0 {
		md5s = append(md5s, GetMD5S(2*ssdeep.BlockSize, keys)...)
	}

	if len(md5s) > 0 {
		records := getRecords(md5s)
		if len(records) > 0 {
			log.Println("_")
		}

	}
	return nil
}

func getRecords(md5s []string) (records []SsdeepRecord) {

	sqlStr, args, err := sqlx.In("SELECT * from all_ori when md5 in ($1)", md5s)
	if err != nil {
		log.Fatalln("in error ", err)
	}

	err = db.Select(&records, sqlStr, args...)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln("sql error ", err)
	}
	return
}

func GetMD5S(blocksize uint32, keywords []string) (md5 []string) {
	sqlStr, args, err := sqlx.In("select distinct(md5) from key_word_index where block_size=$1 and key in ($2)", blocksize, keywords)
	if err != nil {
		log.Fatalln("in error ", err)
	}
	err = db.Select(&md5, sqlStr, args...)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln("sql error ", err)
	}
	return
}

func main() {
	db = connectDB()
	startMD5 := ""
	total := 0

	for {
		records := QueryRecords(startMD5)
		if len(records) == 0 {
			break
		}

		total += len(records)

		for _, record := range records {
			err := ProcessRecord(record)
			if err != nil {
				log.Fatalf("ProcessRecord error %v", err)
			}
			startMD5 = record.MD5
		}
	}

	log.Println("ssdeep records ", total)
}
