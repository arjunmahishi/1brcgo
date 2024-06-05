package main

import (
	"bytes"
	"strconv"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func BenchmarkSplit(b *testing.B) {
	s := []byte("foo,bar,baz")

	result1 := make([][]byte, 3)
	b.Run("stdlib", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			result1 = bytes.Split(s, []byte(","))
		}
	})

	result2 := make([][]byte, 3)
	b.Run("custom", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			startIdx := 0
			insertIdx := 0
			for i, c := range s {
				if c == ',' {
					result2[insertIdx] = s[startIdx:i]
					startIdx = i + 1
					insertIdx++
					continue
				}
			}

			result2[insertIdx] = s[startIdx:]
		}
	})

	assert.Equal(b, result1, result2)
}

func BenchmarkBytesToStr(b *testing.B) {
	byteData := []byte("hello world")

	b.Run("string()", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = string(byteData)
		}
	})

	b.Run("unsafe", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = unsafe.String(&byteData[0], len(byteData))
		}
	})
}

func BenchmarkParseTemprature(b *testing.B) {
	temp := []byte("123")

	b.Run("Using strconv.Atoi", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = strconv.Atoi(string(temp))
		}
	})

	b.Run("Transposing ASCII chars", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = parseTemp(temp)
		}
	})
}

func BenchmarkMapInit(b *testing.B) {
	var (
		m1 = make(map[string]int)
		m2 = make(map[string]int, 1000)
	)

	b.Run("With default initial size", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				m1[strconv.Itoa(j)] = j
			}
		}
	})

	b.Run("With big initial size", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for j := 0; j < 1000; j++ {
				m2[strconv.Itoa(j)] = j
			}
		}
	})
}
