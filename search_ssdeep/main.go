package main

/*
#cgo CFLAGS: -I /devel/include
#cgo LDFLAGS: -L /devel/lib -lfuzzy
#include "fuzzy.h"
#include <stdio.h>
#include <stdint.h>
*/
import "C"

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"log"
	"sort"
	"ssdeep_search"
	"strings"
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

func getSearchKeys(hash string) (keys []string) {
	hash = ssdeep_search.EliminateSequences(hash)
	return ssdeep_search.GenerateKeys(hash)
}

func ProcessRecord(record SsdeepRecord) (err error) {
	ssdeep, err := ssdeep_search.ParseSsdeep(record.Ssdeep)
	if err != nil {
		log.Fatalln("ParseSsdeep error ", err)
	}

	var md5s []string
	keys := getSearchKeys(ssdeep.HashBlockSize)
	if len(keys) > 0 {
		md5s = GetMD5S(ssdeep.BlockSize, keys)
	}

	keys = getSearchKeys(ssdeep.Hash2BlockSIze)
	if len(keys) > 0 {
		md5s = append(md5s, GetMD5S(2*ssdeep.BlockSize, keys)...)
	}

	if len(md5s) > 0 {
		records := getRecords(md5s)
		if len(records) > 0 {
			computeSimilarity(record, records)
		} else {
			saveResult(record.MD5, record.Ssdeep, "", "", 0)
		}

	}
	return nil
}

func computeSimilarity(input SsdeepRecord, records []SsdeepRecord) {
	type item struct {
		score  int
		MD5    string
		ssdeep string
	}

	items := make([]item, 0)

	for _, record := range records {
		score := Fuzzy_compare(input.Ssdeep, record.Ssdeep)
		items = append(items, item{
			score:  score,
			MD5:    record.MD5,
			ssdeep: record.Ssdeep,
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].score > items[j].score
	})

	saveItem := items[0]
	saveResult(input.MD5, input.Ssdeep, saveItem.MD5, saveItem.ssdeep, saveItem.score)
}

func saveResult(inputMD5 string, inputSSdeep string, outputMD5 string, outputSSdeep string, score int) {
	_, err := db.Exec("insert into results(changed_md5,changed_ssdeep,ori_md5,ori_ssdeep,score) values($1,$2,$3,$4,$5)", inputMD5, inputSSdeep, outputMD5, outputSSdeep, score)
	if err != nil {
		log.Fatalln("insert result error ", err)
	}
}

func getRecords(md5s []string) (records []SsdeepRecord) {

	sqlStr, args, err := sqlx.In("SELECT * from all_ori where md5 in (?)", md5s)
	if err != nil {
		log.Fatalln("in error ", err)
	}

	sqlStr = replaceInSql(sqlStr)
	err = db.Select(&records, sqlStr, args...)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln("sql error ", err)
	}
	return
}

func GetMD5S(blocksize uint32, keywords []string) (md5 []string) {
	sqlStr, args, err := sqlx.In("select distinct(md5) from key_word_index where block_size=? and key in (?)", blocksize, keywords)
	if err != nil {
		log.Fatalln("in error ", err)
	}
	sqlStr = replaceInSql(sqlStr)
	err = db.Select(&md5, sqlStr, args...)
	if err != nil && err != sql.ErrNoRows {
		log.Fatalln("sql error ", err)
	}
	return
}

func Fuzzy_compare(hash1 string, hash2 string) (result int) {

	ch1 := C.CString(hash1)
	ch2 := C.CString(hash2)

	hashSimilarity := C.fuzzy_compare(ch1, ch2)

	fmt.Println(hashSimilarity)

	return int(hashSimilarity)

}

func replaceInSql(sqlStr string) string {
	index := 1
	for {
		if strings.Contains(sqlStr, "?") {
			str := fmt.Sprintf("$%d", index)
			sqlStr = strings.Replace(sqlStr, "?", str, 1)
			index += 1
			continue
		}
		break
	}
	return sqlStr
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
