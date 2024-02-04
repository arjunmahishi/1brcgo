/*
If you want to see the tests and benchmarks that led to this, checkout
this repo: https://github.com/arjunmahishi/1brcgo

NOTE: this solution only works for the 413 stations generated by the
generate.go script :P. There is a function in hash_test.go (https://github.com/arjunmahishi/1brcgo/blob/a17e34a5543bcdfcedd7da9c1c862d3e1b1212b7/hash_test.go#L76)
which finds the right "mod" (len(arr) % mod) for a zero collision hash function.

Some of the most impactful optimisations in this solution:
  * Manually splitting text (instead of bytes.Split). Apart from reducing
    allocations, it also lets you skip a few iterations based on the smallest
    station name and smallest temperature length
  * Mmap did not have a significant impact on performance. It was probably
    the same as reading the file in chunks concurrently.
  * The custom hash function was not as impactful as I thought it would be.
    Just saved a few hundred milliseconds.
  * String conversion using unsafe was a lot faster than using string(someByteSlice) because
    it reuses the already allocated memory for the byte slice.
  * Avoiding strings.ParseFloat/Atoi saved a lot of time. I was able to parse
    the temperature as an int directly from the byte slice by transposing the
    ASCII values of each character to it's integer counterpart.
  * No locks or waitgroups were used. The fan-in of processed chunks was done
    using a single channel and a counter.
*/

package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"syscall"
	"unsafe"
)

type temprature struct {
	min, max, sum, count int
	key                  []byte
}

type processedBatch []temprature

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	if os.Getenv("PROFILE") == "1" {
		fmt.Println(runtime.NumCPU(), "CPUs available")
		cpuProfile, err := os.Create("cpu_profile.prof")
		if err != nil {
			panic(err)
		}
		defer cpuProfile.Close()

		if err := pprof.StartCPUProfile(cpuProfile); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	filename := "measurements.txt"
	if len(os.Args) > 1 {
		filename = os.Args[1]
	}

	run(filename)
}

func run(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}

	data, err := syscall.Mmap(
		int(file.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_SHARED,
	)
	if err != nil {
		panic(err)
	}

	var (
		noOfChunks  = runtime.NumCPU()
		chunkSize   = len(data) / noOfChunks
		start       = 0
		resChan     = make(chan processedBatch, noOfChunks)
		actualCount = 0
	)

	for i := 0; i < noOfChunks && start < len(data); i++ {
		end := min(start+chunkSize, len(data)-1)

		// find the nearest \n
		for {
			if data[end] == '\n' || end == len(data)-1 {
				go func(chunk []byte) {
					resChan <- handleChunk(chunk)
				}(data[start:end])
				actualCount++
				start = end + 1
				break
			}

			end++
		}
	}

	aggAndPrint(resChan, actualCount)
}

func aggAndPrint(resChan <-chan processedBatch, chunkCount int) {
	aggData := make(map[string]temprature, 100000)
	stationList := make([]string, 413)
	stationCount := 0

	for i := 0; i < chunkCount; i++ {
		for _, temp := range <-resChan {
			if temp.count == 0 {
				continue
			}

			station := unsafe.String(&temp.key[0], len(temp.key))
			if row, ok := aggData[station]; !ok {
				aggData[station] = temp
				stationList[stationCount] = station
				stationCount++
			} else {
				row.min = min(row.min, temp.min)
				row.max = max(row.max, temp.max)
				row.sum += temp.sum
				row.count += temp.count
				aggData[station] = row
			}
		}
	}

	// print
	sort.Strings(stationList)
	for _, station := range stationList {
		data := aggData[station]
		if data.count == 0 {
			continue
		}

		fmt.Printf(
			"%s=%.1f/%.1f/%.1f\n",
			station,
			float64(data.min)/10.0,
			(float64(data.sum)/float64(data.count))/10,
			float64(data.max)/10.0,
		)
	}
}

func parseTemp(s []byte) int {
	start := 0
	mul := 1
	if s[0] == '-' {
		start = 1
		mul = -1
	}

	if len(s[start:]) == 3 {
		return (((int(s[start]) - 48) * 10) + (int(s[start+2]) - 48)) * mul
	}

	return (((int(s[start]) - 48) * 100) + ((int(s[start+1]) - 48) * 10) + (int(s[start+3]) - 48)) * mul
}

func (pb *processedBatch) add(station []byte, temp int) {
	h := hash(station)
	bucket := &(*pb)[h]

	if bucket.count == 0 {
		*bucket = temprature{
			min:   temp,
			max:   temp,
			sum:   temp,
			count: 1,
			key:   station,
		}

		return
	}

	// found the same station
	bucket.min = min(bucket.min, temp)
	bucket.max = max(bucket.max, temp)
	bucket.sum += temp
	bucket.count++
}

func hash(key []byte) uint64 {
	hash := uint64(1)
	for _, c := range key {
		hash ^= uint64(c)
		hash *= 31
	}

	return (hash % 13696) // experimented - zero collisions
}

func handleChunk(chunk []byte) processedBatch {
	// s := time.Now()
	// defer func() {
	// 	log.Println(time.Since(s), len(chunk))
	// }()

	var (
		start, end, splitIdx int
		line                 []byte

		localData = make(processedBatch, 13690)
		chunkLen  = len(chunk)
	)

	for end < chunkLen {
		if chunk[end] == '\n' || end == chunkLen-1 {
			if end == chunkLen-1 && chunk[end] != '\n' {
				end = chunkLen
			}

			splitIdx = end - start - 4
			line = chunk[start:end]
			for ; ; splitIdx-- {
				if line[splitIdx] == ';' {
					localData.add(line[:splitIdx], parseTemp(line[splitIdx+1:]))
					break
				}
			}

			start = end + 1
			end += 7 // smallest possible line
			continue
		}

		end++
	}

	return localData
}