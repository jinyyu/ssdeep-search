package ssdeep_search

import "testing"

func TestGenerateKeys(t *testing.T) {
	hash := "12345678"
	keys := GenerateKeys(hash)

	if len(keys) != 2 || keys[0] != "1234567" || keys[1] != "2345678" {
		t.Error("error")
	}

	ssdeep := "196608:V84H0CBigCD2lwAwh9kAzYou1KgervWxbGoH9Vv:V84H+BYwUAzYVRU1U"
	result, err := ParseSsdeep(ssdeep)
	if err != nil {
		t.Errorf("error %v", err)
	}
	if result.BlockSize != 196608 || result.HashBlockSize != "V84H0CBigCD2lwAwh9kAzYou1KgervWxbGoH9Vv" || result.Hash2BlockSIze != "V84H+BYwUAzYVRU1U" {
		t.Error("ERROR")
	}
}

func TestEliminateSequences(t *testing.T) {
	type Test struct {
		in  string
		out string
	}

	tests := []Test{
		{
			"12LLLL99999123566787877788",
			"12LLL999123566787877788",
		},
		{
			"LLLL123",
			"LLL123",
		},
		{
			"233333333333333333333333333333333333333333333333333333333",
			"2333",
		},
	}

	for i, test := range tests {
		output := EliminateSequences(test.in)
		if output != test.out {
			t.Errorf("i = %d, output=%s, woutput=%s", i, output, test.out)
		}
	}
}
