package ssdeep_search

import (
	"fmt"
	"strconv"
	"strings"
)

func GenerateKeys(hash string) (keys []string) {
	for index := 7; index <= len(hash); index++ {
		keys = append(keys, hash[index-7:index])
	}
	return
}

type SsdeepSign struct {
	BlockSize      uint32
	HashBlockSize  string
	Hash2BlockSIze string
}

func ParseSsdeep(ssdeep string) (sign *SsdeepSign, err error) {
	err = fmt.Errorf("invalid ssddep")

	strs := strings.Split(ssdeep, ":")
	if len(strs) != 3 {
		return
	}

	blockSize, err := strconv.ParseInt(strs[0], 10, 64)
	if err != nil {
		return
	}

	sign = &SsdeepSign{
		BlockSize:      uint32(blockSize),
		HashBlockSize:  strs[1],
		Hash2BlockSIze: strs[2],
	}
	return
}

//删除长度大于3的连续相同序列
func EliminateSequences(hash string) string {
	if len(hash) == 0 {
		return hash
	}

	ret := make([]byte, 0, len(hash))
	prev := hash[0]
	ret = append(ret, prev)
	seq := 1
	for i := 1; i < len(hash); i++ {
		current := hash[i]
		if current == prev {
			seq += 1
		} else {
			seq = 1
		}

		if seq <= 3 {
			ret = append(ret, current)
		}
		prev = current
	}
	return string(ret)
}
