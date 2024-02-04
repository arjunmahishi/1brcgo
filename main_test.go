package main

import (
	"bytes"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"os"
	"testing"
)

func TestManualConv(t *testing.T) {
	tt := []struct {
		in  []byte
		out int
	}{
		{[]byte("12.9"), 129},
		{[]byte("0.0"), 0},
		{[]byte("-10.1"), -101},
		{[]byte("-1.1"), -11},
	}

	fmt.Println(int('0'))

	for _, tc := range tt {
		t.Run(string(tc.in), func(t *testing.T) {
			if parseTemp(tc.in) != tc.out {
				t.Errorf(
					"parseFloat(%s) = %d, want %d",
					tc.in, parseTemp(tc.in), tc.out,
				)
			}
		})
	}
}

func TestParse(t *testing.T) {
	tt := []struct {
		in      []byte
		station string
		temp    int
	}{
		{[]byte("Abc;12.0"), "Abc", 120},
		{[]byte("Efg;1.2"), "Efg", 12},
		{[]byte("Ijk;-1.0"), "Ijk", -10},
		{[]byte("Klm;-12.9"), "Klm", -129},
	}

	for _, tc := range tt {
		t.Run(string(tc.in), func(t *testing.T) {
			station, temp := parse(tc.in)
			if string(station) != tc.station {
				t.Errorf("Want = %s, got = %s", tc.station, station)
			}

			if temp != tc.temp {
				t.Errorf("Want = %d, got = %d", tc.temp, temp)
			}
		})
	}
}

var hasher maphash.Hash

func hashCityName(cityName []byte) uint64 {
	hasher.Reset()
	if _, err := hasher.Write(cityName); err != nil {
		panic(err)
	}

	return hasher.Sum64() % (10 << 15)
}

func TestHash(t *testing.T) {
	t.Skip()
	file, err := os.Open("testdata")
	if err != nil {
		t.Fatal(err)
	}

	cont, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	m := uint64(0)
	dedup := map[uint64]bool{}
	lines := bytes.Split(cont, []byte("\n"))
	for _, line := range lines {
		split := bytes.Split(line, []byte(";"))
		id := hashCityName(split[0])

		if dedup[id] {
			t.Errorf("Duplicate hash %d", id)
		}

		dedup[id] = true
		if id > m {
			m = id
		}
	}

	log.Println(m)
	log.Println(10 << 15)
}

func TestHandleChunk(t *testing.T) {
	// Banjul;38.9\nHamilton;9.5\nMoncton;10.3\nKarachi;20.9\nAssab;24.4\nNouakchott;17.3\nBeirut;16.0\nDolisie;23.6\nHoniara;25.7\nJos;3.9

	tt := []struct {
		in  []byte
		out map[string]*temprature
	}{
		{
			in: []byte("Banjul;38.9\nHamilton;9.5\nMoncton;10.3\nKarachi;20.9\nAssab;24.4\nNouakchott;17.3\nBeirut;16.0\nDolisie;23.6\nHoniara;25.7\nJos;3.9"),
			out: map[string]*temprature{
				"Banjul":     {count: 1, sum: 389, min: 389, max: 389},
				"Hamilton":   {count: 1, sum: 95, min: 95, max: 95},
				"Moncton":    {count: 1, sum: 103, min: 103, max: 103},
				"Karachi":    {count: 1, sum: 209, min: 209, max: 209},
				"Assab":      {count: 1, sum: 244, min: 244, max: 244},
				"Nouakchott": {count: 1, sum: 173, min: 173, max: 173},
				"Beirut":     {count: 1, sum: 160, min: 160, max: 160},
				"Dolisie":    {count: 1, sum: 236, min: 236, max: 236},
				"Honiara":    {count: 1, sum: 257, min: 257, max: 257},
				"Jos":        {count: 1, sum: 39, min: 39, max: 39},
			},
		},
		{
			// trailing \n
			in: []byte("Banjul;38.9\nHamilton;9.5\nMoncton;10.3\nKarachi;20.9\nAssab;24.4\nNouakchott;17.3\nBeirut;16.0\nDolisie;23.6\nHoniara;25.7\nJos;3.9\nBanjul;-38.9\n"),
			out: map[string]*temprature{
				"Banjul":     {count: 2, sum: 00, min: -389, max: 389},
				"Hamilton":   {count: 1, sum: 95, min: 95, max: 95},
				"Moncton":    {count: 1, sum: 103, min: 103, max: 103},
				"Karachi":    {count: 1, sum: 209, min: 209, max: 209},
				"Assab":      {count: 1, sum: 244, min: 244, max: 244},
				"Nouakchott": {count: 1, sum: 173, min: 173, max: 173},
				"Beirut":     {count: 1, sum: 160, min: 160, max: 160},
				"Dolisie":    {count: 1, sum: 236, min: 236, max: 236},
				"Honiara":    {count: 1, sum: 257, min: 257, max: 257},
				"Jos":        {count: 1, sum: 39, min: 39, max: 39},
			},
		},
	}

	for i, tc := range tt {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			res := handleChunkOld(tc.in)

			for station, temp := range tc.out {
				if _, ok := res[station]; !ok {
					t.Errorf("%s not found in result", station)
				}

				if temp.count != res[station].count {
					t.Errorf("Want = %d, got = %d", res[station].count, temp.count)
				}

				if temp.min != res[station].min {
					t.Errorf("Want = %d, got = %d", res[station].min, temp.min)
				}

				if temp.max != res[station].max {
					t.Errorf("Want = %d, got = %d", res[station].max, temp.max)
				}

				if temp.sum != res[station].sum {
					t.Errorf("Want = %d, got = %d", res[station].sum, temp.sum)
				}
			}
		})
	}
}
