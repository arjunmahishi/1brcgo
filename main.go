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
