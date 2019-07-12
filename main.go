package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"golang.org/x/crypto/sha3"
)

var valueSize = 128
var maxProcs = 64
var requests = 100000
var parallel = true

type State struct {
	sync.Map
}

type CommandJSON struct {
	ValuePlainText string `json:"value"`
	ValueHashed    string `json:"hash"`
}

func main() {
	fmt.Println("Parallel: ", parallel)
	fmt.Println("GOMAXPROCS: ", maxProcs)
	fmt.Println("Requests: ", requests)
	fmt.Println("Size of Commands: ", valueSize)

	state := &State{}

	runtime.GOMAXPROCS(maxProcs)

	beforeTotal := time.Now()
	for i := 0; i < requests; i++ {
		value := []byte(randomString(valueSize))
		if parallel {
			go state.Execute(i, value)
		} else {
			state.Execute(i, value)
		}
	}
	afterTotal := time.Now()

	testTook := afterTotal.Sub(beforeTotal).Seconds()
	fmt.Printf("Test took: %v\n", testTook)

	throughput := (float64(requests) / testTook)
	fmt.Printf("Throughput: %v ops/s\n", math.RoundToEven(throughput))

}

// Execute the command in the key-value store
func (st *State) Execute(key int, value []byte) bool {
	valuePlaintext := string(value)
	valueHashed := HashValue(valuePlaintext)
	valueArr := &CommandJSON{
		ValuePlainText: valuePlaintext,
		ValueHashed:    valueHashed}
	valueJSON, _ := json.Marshal(valueArr)
	_, loaded := st.Map.LoadOrStore(key, valueJSON)
	return !loaded
}

// Returns an int >= min, < max
func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

// Generate a random string of A-Z chars with len = l
func randomString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		bytes[i] = byte(randomInt(65, 90))
	}
	return string(bytes)
}

// HashValue hashes the value
func HashValue(value string) string {
	valueHashed := sha3.New512()
	valueHashed.Write([]byte(value))
	return hex.EncodeToString(valueHashed.Sum(nil))
}
