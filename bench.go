package main

// MIT License
//
// Copyright (c) 2023 DS2 Lab @ UVA
// Copyright (c) 2017 Josh Baker (https://github.com/tidwall/redbench)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ScottMansfield/nanolog"
	infinistore "github.com/ds2-lab/infinistore/client"
	"github.com/go-redis/redis/v8"

	//"github.com/pkg/profile"

	"github.com/ds2-lab/infinibench/benchclient"
	"github.com/dustin/go-humanize"
)

const (
	CLIENT_INFINICACHE = "infinistore"
	CLIENT_REDIS       = "redis"
	CLIENT_S3          = "s3"
	CLIENT_ELASTICACHE = "elasticache"
	CLIENT_FSX         = "fsx"
	CLIENT_EFS         = "efs"
)

// Options represents various options used by the Bench() function.
type Options struct {
	AddrList       string
	Bucket         string
	Requests       int
	Clients        int
	Pipeline       int
	Keymin         int
	Keymax         int
	Objsz          int
	Datashard      int
	Parityshard    int
	ECmaxgoroutine int
	Op             int
	Quiet          bool
	CSV            bool
	Stdout         io.Writer
	Stderr         io.Writer
	Printlog       bool
	File           string
	Interval       int64
	ClientLib      string
	ClientBase     string
}

// DefaultsOptions are the default options used by the Bench() function.
var DefaultOptions = &Options{
	AddrList:       "127.0.0.1:6378",
	Bucket:         "",
	Requests:       15,
	Clients:        1,
	Pipeline:       1,
	Keymin:         0,
	Keymax:         99,
	Objsz:          10485760 * 4,
	Datashard:      4,
	Parityshard:    2,
	ECmaxgoroutine: 32,
	Op:             0, // 0: SET; 1: GET
	Quiet:          false,
	CSV:            false,
	Stdout:         os.Stdout,
	Stderr:         os.Stderr,
	Printlog:       true,
	File:           "test.txt",
	Interval:       0,
	ClientLib:      CLIENT_INFINICACHE,
	ClientBase:     "",
}

func getRandomRange(min int, max int) int {
	var rn int
	rand.Seed(time.Now().UnixNano())
	rn = rand.Intn(max-min+1) + min
	return rn
}

func genKey(keymin int, keymax int, op int, i int) string {
	var ret string
	if op == 0 { // SET
		keyIdx := keymin + i%(keymax-keymin+1)
		ret = strings.Join([]string{"key_", strconv.Itoa(keyIdx)}, "")
	} else { // GET
		rn := getRandomRange(keymin, keymax)
		ret = strings.Join([]string{"key_", strconv.Itoa(rn)}, "")
	}
	log.Println("generated key: ", ret, "len: ", len(ret))
	return ret
}

// Bench performs a benchmark on the server at the specified address.
//func Bench(
//	name string,
//	addr string,
//	opts *Options,
//	prep func(conn net.Conn) bool,
//	fill func(buf []byte) []byte,
//) {
func Bench(
	opts *Options,
) {
	if !opts.Printlog {
		log.SetOutput(ioutil.Discard)
	}
	if opts.Stderr == nil {
		opts.Stderr = ioutil.Discard
	}
	if opts.Stdout == nil {
		opts.Stdout = ioutil.Discard
	}
	var totalPayload uint64
	var count uint64
	var duration int64
	//rpc := opts.Requests / opts.Clients
	//rpcex := opts.Requests % opts.Clients
	rpc := opts.Requests
	var tstop int64
	remaining := int64(opts.Clients)
	errs := make([]error, opts.Clients)
	durs := make([][]time.Duration, opts.Clients)
	clis := make([]benchclient.Client, opts.Clients)

	// create all clients
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		durs[i] = make([]time.Duration, crequests)
		for j := 0; j < len(durs[i]); j++ {
			durs[i][j] = -1
		}
		//conn, err := net.Dial("tcp", addr)

		var cli benchclient.Client
		switch opts.ClientLib {
		case CLIENT_REDIS:
			cli = benchclient.NewRedisWithBackend(redis.NewClient(&redis.Options{
				Addr:       opts.AddrList,
				Password:   "", // no password set
				DB:         0,  // use default DB
				PoolSize:   1,  // use 1 connection per concurrency.
				MaxRetries: 0,
			}))
		case CLIENT_S3:
			bucket := opts.ClientBase
			if bucket == "" {
				bucket = opts.Bucket
			}
			cli = benchclient.NewS3(bucket)
		case CLIENT_ELASTICACHE:
			cli = benchclient.NewRedisClusterByAddresses(strings.Split(opts.AddrList, ","), 0)
		case CLIENT_EFS:
			fallthrough
		case CLIENT_FSX:
			cli = benchclient.NewFile(opts.ClientLib, opts.ClientBase)
		default:
			addrArr := strings.Split(opts.AddrList, ",")
			log.Println("number of hosts: ", len(addrArr))
			ic := infinistore.NewClient(opts.Datashard, opts.Parityshard, opts.ECmaxgoroutine)
			ic.Dial(addrArr)
			cli = ic
		}
		defer cli.Close()
		clis[i] = cli
	}

	tstart := time.Now()
	for i := 0; i < opts.Clients; i++ {
		crequests := rpc
		//if i == opts.Clients-1 {
		//	crequests += rpcex
		//}
		//val := make([]byte, 10485760)
		val := make([]byte, opts.Objsz)
		rand.Read(val)

		go func(cli benchclient.Client, cid, crequests int) {
			defer func() {
				atomic.AddInt64(&remaining, -1)
			}()
			/*if conn == nil {
			if cli == nil {
				return
			}*/
			err := func() error {
				//var buf []byte
				//rd := bufio.NewReader(conn)
				for i := 0; i < crequests; i += opts.Pipeline {
					n := opts.Pipeline
					if i+n > crequests {
						n = crequests - i
					}
					key := genKey(opts.Keymin, opts.Keymax, opts.Op, i*opts.Clients+cid)
					/*
						buf = buf[:0]
						for i := 0; i < n; i++ {
							buf = fill(buf)
						}
						atomic.AddUint64(&totalPayload, uint64(len(buf)))
					*/
					start := time.Now()
					if opts.Op == 0 {
						atomic.AddUint64(&totalPayload, uint64(len(val)))
						cli.EcSet(key, val)
					} else {
						_, reader, _ := cli.EcGet(key)
						if reader != nil {
							atomic.AddUint64(&totalPayload, uint64(reader.Len()))
							reader.Close() // By closing the reader, we save memory.
						}
					}
					/*if err != nil {
						return err
					}
					if err := readResp(rd, n, opts); err != nil {
						return err
					}*/
					stop := time.Since(start)
					for j := 0; j < n; j++ {
						durs[cid][i+j] = stop / time.Duration(n)
					}
					atomic.AddInt64(&duration, int64(stop))
					atomic.AddUint64(&count, uint64(n))
					atomic.StoreInt64(&tstop, int64(time.Since(tstart)))
					if opts.Interval != 0 {
						time.Sleep(time.Duration(opts.Interval) * time.Millisecond)
					}
				}
				return nil
			}()
			if err != nil {
				errs[cid] = err
			}
			//}(conns[i], i, crequests)
		}(clis[i], i, crequests)
	}
	var die bool
	for {
		remaining := int(atomic.LoadInt64(&remaining))   // active clients
		count := int(atomic.LoadUint64(&count))          // completed requests
		real := time.Duration(atomic.LoadInt64(&tstop))  // real duration
		totalPayload := atomic.LoadUint64(&totalPayload) // size of all bytes sent
		more := remaining > 0
		var realrps float64
		if real > 0 {
			realrps = float64(count) / (float64(real) / float64(time.Second))
		}
		if !opts.CSV {
			//fmt.Fprintf(opts.Stdout, "\r%s: %.2f", name, realrps)
			fmt.Fprintf(opts.Stdout, "\r%.2f", realrps)
			if more {
				fmt.Fprintf(opts.Stdout, "\r")
			} else if opts.Quiet {
				fmt.Fprintf(opts.Stdout, " requests per second\n")
			} else {
				//fmt.Fprintf(opts.Stdout, "\r====== %s ======\n", name)
				fmt.Fprintf(opts.Stdout, "  %d requests completed in %.2f seconds\n", count, float64(real)/float64(time.Second))
				fmt.Fprintf(opts.Stdout, "  %d parallel clients\n", opts.Clients)
				fmt.Fprintf(opts.Stdout, "  %s bytes per second\n", humanize.Bytes(uint64(float64(totalPayload)/(float64(real)/float64(time.Second)))))
				fmt.Fprintf(opts.Stdout, "  keep alive: 1\n")
				fmt.Fprintf(opts.Stdout, "\n")
				var limit time.Duration
				var lastper float64
				for {
					limit += time.Millisecond
					var hits, count int
					for i := 0; i < len(durs); i++ {
						for j := 0; j < len(durs[i]); j++ {
							dur := durs[i][j]
							if dur == -1 {
								continue
							}
							if dur < limit {
								hits++
							}
							count++
						}
					}
					per := float64(hits) / float64(count)
					if math.Floor(per*10000) == math.Floor(lastper*10000) {
						continue
					}
					lastper = per
					fmt.Fprintf(opts.Stdout, "%.2f%% <= %d milliseconds\n", per*100, (limit-time.Millisecond)/time.Millisecond)
					if per == 1.0 {
						break
					}
				}
				fmt.Fprintf(opts.Stdout, "%.2f requests per second\n\n", realrps)
			}
		}
		if !more {
			if opts.CSV {
				//fmt.Fprintf(opts.Stdout, "\"%s\",\"%.2f\"\n", name, realrps)
				fmt.Fprintf(opts.Stdout, "\"%.2f\"\n", realrps)
			}
			for _, err := range errs {
				if err != nil {
					fmt.Fprintf(opts.Stderr, "%s\n", err)
					die = true
					if count == 0 {
						break
					}
				}
			}
			break
		}
		time.Sleep(time.Second / 5)
	}

	// close clients
	/*
		for i := 0; i < len(conns); i++ {
			if conns[i] != nil {
				conns[i].Close()
			}
		}*/
	if die {
		os.Exit(1)
	}
}

// AppendCommand will append a Redis command to the byte slice and
// returns a modified slice.
func AppendCommand(buf []byte, args ...string) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, arg...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func helpInfo() {
	fmt.Printf("Usage: %s [options]\n", os.Args[0])
	fmt.Println("Option list: ")
	flag.PrintDefaults()
	// fmt.Println("  -n [NUMBER]: number of requests")
	// fmt.Println("  -c [NUMBER]: number of concurrent clients")
	// fmt.Println("  -addrlist [ADDR:PORT,...]: server address:port")
	// fmt.Println("  -bucket: S3 bucket name")
	// fmt.Println("  -pipeline [NUMBER]: number of pipelined requests")
	// fmt.Println("  -keymin [NUMBER]: minimum key range")
	// fmt.Println("  -keymax [NUMBER]: maximum key range")
	// fmt.Println("  -sz [NUMBER]: object data size")
	// fmt.Println("  -d [NUMBER]: number of data shards for RS erasure coding")
	// fmt.Println("  -p [NUMBER]: number of parity shards for RS erasure coding")
	// fmt.Println("  -g [NUMBER]: max number of goroutines for RS erasure coding")
	// fmt.Println("  -dec: do decoding after Receive()?")
	// fmt.Println("  -op [0 or 1]: operation type (0: SET (load the data store); 1: GET)")
	// fmt.Println("  -log: print out debugging info?")
	// fmt.Println("  -file: print result to file")
	// fmt.Println("  -h: print out help info?")
	// fmt.Println("  -i: interval for every request (ms)")
	// fmt.Println("  -cli: client library used, try \"infinistore\" or \"redis\" or \"s3\" or \"elasticache\".")
}

func main() {
	//profile.Start(profile.CPUProfile)
	//defer profile.Start(profile.CPUProfile).Stop()
	//defer profile.Start().Stop()

	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	options := DefaultOptions

	flag.IntVar(&options.Requests, "n", 10, "Number of requests.")
	flag.IntVar(&options.Clients, "c", 1, "Number of clients.")
	flag.IntVar(&options.Keymin, "keymin", 1, "Start postfix of generated keys. Generated key will be in the form of key_[keymin] ~ key_[keymax].")
	flag.IntVar(&options.Keymax, "keymax", 10, "End postfix of generated keys.")
	flag.IntVar(&options.Objsz, "sz", 128, "Object size in bytes.")
	flag.IntVar(&options.Op, "op", 0, "Operation flag: 0 - SET (load the data store); 1 - GET.")
	flag.StringVar(&options.ClientLib, "cli", CLIENT_INFINICACHE, "Client library, support \"infinistore\", \"redis\", \"s3\", \"elasticache\", \"fsx\", and \"efs.\"")
	flag.StringVar(&options.AddrList, "addrlist", "127.0.0.1:6378", "Server addresses.")
	flag.IntVar(&options.Datashard, "d", 4, "Number of data shards for RS erasure coding. Ignore if cli is not \"infinistore.\"")
	flag.IntVar(&options.Parityshard, "p", 2, "Number of parity shards for RS erasure coding. Ignore if cli is not \"infinistore.\"")
	flag.IntVar(&options.ECmaxgoroutine, "g", 32, "Max number of goroutines for RS erasure coding. Ignore if cli is not \"infinistore.\"")
	flag.StringVar(&options.Bucket, "bucket", "", "S3 bucket name. Ignore if cli is not \"s3.\"")
	flag.StringVar(&options.ClientBase, "cli-base", "", "Base path for file based client.")
	flag.IntVar(&options.Pipeline, "pipeline", 1, "Number of pipelined requests")
	flag.BoolVar(&options.Printlog, "log", true, "Print debugging log.")
	flag.StringVar(&options.File, "file", "", "Print result to file.")
	flag.Int64Var(&options.Interval, "i", 0, "Interval for every req (ms)")

	flag.Parse()

	if printInfo {
		helpInfo()
		os.Exit(0)
	}

	if options.File != "" {
		logCreate(options)

		f := options.File + "_" + strconv.Itoa(options.Op) + "_summary.txt"
		file, err := os.Create(f)
		if err != nil {
			fmt.Printf("Failed to create file: %v\n", err)
		} else {
			options.Stdout = file
			defer file.Close()
		}
	}

	fmt.Println("Starting test...")
	Bench(options)

	if options.File != "" {
		if err := nanolog.Flush(); err != nil {
			fmt.Printf("Failed to collect data: %v\n", err)
		}
	}
}

//logCreate create the nanoLog
func logCreate(opts *Options) {
	// Set up nanoLog writer
	path := opts.File + "_" + strconv.Itoa(opts.Op) + "_bench.clog"
	nanoLogout, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		panic(err)
	}
}
