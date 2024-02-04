package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"runtime/pprof"
	"testing"
	"unsafe"
)

func parse(line []byte) (string, int) {
	splitIdx := len(line) - 4 // 4 is the smallest possible temprature length

	for ; ; splitIdx-- {
		if line[splitIdx] == ';' {
			return unsafe.String(&line[0], splitIdx), parseTemp(line[splitIdx+1:])
		}
	}
}

func handleChunkOld(chunk []byte) map[string]*temprature {
	// s := time.Now()
	// defer func() {
	// 	log.Println(time.Since(s), len(chunk))
	// }()

	var (
		start, end, temp int
		station          string
		data             *temprature
		ok               bool

		// allocate a big size to avoid resizing
		localData = make(map[string]*temprature, 100000)
		chunkLen  = len(chunk)
	)

	for end < chunkLen {
		if chunk[end] == '\n' || end == chunkLen-1 {
			if end == chunkLen-1 && chunk[end] != '\n' {
				end = chunkLen
			}

			station, temp = parse(chunk[start:end])
			data, ok = localData[station]
			if !ok {
				data = &temprature{
					min:   temp,
					max:   temp,
					sum:   temp,
					count: 1,
				}

				localData[station] = data
			} else {
				data.min = min(data.min, temp)
				data.max = max(data.max, temp)
				data.sum += temp
				data.count++
			}

			start = end + 1
			end += 7 // smallest possible line
			continue
		}

		end++
	}

	return localData
}

func TestHashDups(t *testing.T) {
	file, err := os.Open("testdata/cities")
	if err != nil {
		panic(err)
	}

	cont, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	lines := bytes.Split(bytes.TrimSpace(cont), []byte("\n"))

	// for ; ; mod++ {
	small := uint64(9999999999)
	big := uint64(0)
	collisions := 0
	// qualified := true
	dedup := map[uint64][]string{}
	for _, city := range lines {
		h := hash(city)

		if _, ok := dedup[h]; ok {
			dedup[h] = append(dedup[h], string(city))
			continue
		}

		dedup[h] = []string{string(city)}

		if h < 0 {
			log.Println(string(city), h)
		}
	}

	for k, v := range dedup {
		small = min(small, k)
		big = max(big, k)

		// if len(v) > 2 {
		// 	qualified = false
		// }

		if len(v) > 1 {
			collisions++
			log.Println(k, len(v), v)
		}
	}

	// if qualified {
	// log.Println("-----")
	// log.Println("mod", mod)
	log.Println("small", small)
	log.Println("big", big)
	// }

	// if collisions == 0 {
	// 	log.Println("no collisions!!!!")
	// 	break
	// }

	// time.Sleep(200)
	// }
}

func BenchmarkHandleChunk(b *testing.B) {
	cpuProf, err := os.Create("cpu_custom_hash.prof")
	if err != nil {
		b.Fatal(err)
	}
	defer cpuProf.Close()

	if err := pprof.StartCPUProfile(cpuProf); err != nil {
		panic(err)
	}
	defer pprof.StopCPUProfile()

	file, err := os.Open("./testdata/sample_data.txt")
	if err != nil {
		b.Fatal(err)
	}

	conts, err := io.ReadAll(file)
	if err != nil {
		b.Fatal(err)
	}

	// 2 chunk because we are mutating the data
	chunk1 := bytes.TrimSpace(conts)
	chunk2 := bytes.TrimSpace(conts)

	b.Run("standard map", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			handleChunkOld(chunk1)
		}
	})

	b.Run("custom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			handleChunk(chunk2)
		}
	})
}

func TestHandleChunkNew(t *testing.T) {
	tt := []struct {
		in []byte
	}{
		{
			in: []byte("Banjul;38.9\nHamilton;9.5\nMoncton;10.3\nKarachi;20.9\nAssab;24.4\nNouakchott;17.3\nBeirut;16.0\nDolisie;23.6\nHoniara;25.7\nJos;3.9"),
		},
		// {
		// 	// trailing \n
		// 	in: []byte("Banjul;38.9\nHamilton;9.5\nMoncton;10.3\nKarachi;20.9\nAssab;24.4\nNouakchott;17.3\nBeirut;16.0\nDolisie;23.6\nHoniara;25.7\nJos;3.9\nBanjul;-38.9\n"),
		// },
	}

	for i, tc := range tt {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			res := handleChunk(tc.in)

			// res.iter(func(station []byte, temp *temprature) {
			// 	log.Println(string(station), temp)
			// })

			for _, entry := range res {
				if entry.count == 0 {
					continue
				}
				log.Println(string(entry.key), entry.min, entry.max, entry.sum, entry.count)
			}

			// for _, entry := range res {
			// 	if entry.a == nil {
			// 		continue
			// 	}
			// 	log.Println(string(entry.a.key), entry.a.temp)

			// 	if entry.b == nil {
			// 		continue
			// 	}
			// 	log.Println(string(entry.b.key), entry.b.temp)
			// }

			// if len(res) != 10 {
			// 	t.Errorf("Want = %d, got = %d", 10, len(res))
			// }

			// for _, entry := range res {
			// 	if entry.a == nil {
			// 		t.Errorf("Entry %d is nil", i)
			// 	}
			// }
		})
	}
}
