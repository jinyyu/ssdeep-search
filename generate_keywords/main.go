package main

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"ssdeep_search"
)

var (
	db *sqlx.DB
)

func connectDB() *sqlx.DB {
	pgdb, err := sqlx.Connect("postgres", "user=ljy dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatalln(err)
	}
	return pgdb
}

type SsdeepRecord struct {
	MD5    string `db:"md5"`
	Ssdeep string `db:"ssdeep"`
}

func QueryRecords(md5 string) (results []SsdeepRecord) {
	err := db.Select(&results, "select * from all_ori WHERE MD5 > $1 order by md5 asc limit 100", md5)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln(err)
	}
	return
}

func GenerateKeyWords(record SsdeepRecord) (err error) {
	ssdeep, err := ssdeep_search.ParseSsdeep(record.Ssdeep)
	if err != nil {
		return err
	}

	err = insertBlockSizeKeyword(ssdeep.BlockSize, ssdeep.HashBlockSize, record.MD5)
	if err != nil {
		return err
	}

	err = insertBlockSizeKeyword(2*ssdeep.BlockSize, ssdeep.Hash2BlockSIze, record.MD5)
	if err != nil {
		return err
	}
	return err
}

func insertBlockSizeKeyword(blocksize uint32, hash string, md5 string) (err error) {
	hash = ssdeep_search.EliminateSequences(hash)
	keywords := ssdeep_search.GenerateKeys(hash)
	if len(keywords) == 0 {
		return nil
	}

	sql := "insert into key_word_index(block_size, key, md5) values "
	args := make([]interface{}, 0)
	seq := 1

	for i, keyword := range keywords {
		if i > 0 {
			sql += ","
		}

		sql += fmt.Sprintf("($%d, $%d, $%d)", seq, seq+1, seq+2)
		seq += 3
		args = append(args, blocksize, keyword, md5)
	}
	_, err = db.Exec(sql, args...)
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
			err := GenerateKeyWords(record)
			if err != nil {
				log.Fatalln("GenerateKeyWords error ", err)
			}
			startMD5 = record.MD5
		}

		println("total = ", total)
	}

	log.Println("ssdeep records ", total)
}
