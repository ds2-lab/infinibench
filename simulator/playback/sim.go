package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	syslog "log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/dustin/go-humanize"
	"github.com/mason-leap-lab/infinicache/client"
	"github.com/mason-leap-lab/infinicache/common/logger"
	"github.com/mason-leap-lab/infinicache/proxy/global"
	"github.com/wangaoone/redbench/benchclient"

	"github.com/wangaoone/redbench/simulator/playback/proxy"
)

const (
	TIME_PATTERN  = "2006-01-02 15:04:05.000"
	TIME_PATTERN2 = "2006-01-02 15:04:05"
)

var (
	log = &logger.ColorLogger{
		Verbose: true,
		Level:   logger.LOG_LEVEL_ALL,
		Color:   true,
	}
	mu sync.Mutex
)

func init() {
	global.Log = log
}

type Options struct {
	AddrList       string
	Cluster        int
	Datashard      int
	Parityshard    int
	ECmaxgoroutine int
	CSV            bool
	Stdout         io.Writer
	Stderr         io.Writer
	NoDebug        bool
	SummaryOnly    bool
	File           string
	Compact        bool
	Interval       int64
	Dryrun         bool
	Lean           bool
	MaxSz          uint64
	ScaleFrom      uint64
	ScaleSz        float64
	Skip           int64
	S3             string
	Redis          string
	RedisCluster   bool
	Balance        bool
	Concurrency    int
}

type Member string

func (m Member) String() string {
	//return strconv.Atoi(m)
	return string(m)
}

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

func perform(opts *Options, cli benchclient.Client, p *proxy.Proxy, obj *proxy.Object) (string, string) {
	dryrun := 0
	if opts.Dryrun {
		dryrun = opts.Cluster
	}

	// log.Debug("Key:", obj.Key, "mapped to Proxy:", p.Id)
	if placements := p.Placements(obj.Key); placements != nil {
		log.Trace("Found placements of %v: %v", obj.Key, placements)

		reqId, reader, success := cli.EcGet(obj.Key, dryrun)
		if opts.Dryrun && opts.Balance {
			success = p.Validate(obj)
		}

		if !success {
			val := make([]byte, obj.Sz)
			rand.Read(val)
			resetPlacements := make([]int, opts.Datashard+opts.Parityshard)
			_, reset := cli.EcSet(obj.Key, val, dryrun, resetPlacements, "Reset")
			if reset {
				log.Trace("Reset %s.", obj.Key)

				displaced := false
				resetPlacements = p.Remap(resetPlacements, obj)
				for i, idx := range resetPlacements {
					p.ValidateLambda(idx)
					chk, _ := p.LambdaPool[placements[i]].GetChunk(fmt.Sprintf("%d@%s", i, obj.Key))
					if chk == nil {
						// Eviction tracked by simulator. Try find chunk from evicts.
						chk = p.GetEvicted(fmt.Sprintf("%d@%s", i, obj.Key))

						displaced = true
						p.LambdaPool[idx].AddChunk(chk)
						p.LambdaPool[idx].MemUsed += chk.Sz
					} else if idx != placements[i] {
						// Placement changed?
						displaced = true
						log.Warn("Placement changed on reset %s, %d -> %d", chk.Key, placements[i], idx)
						p.LambdaPool[placements[i]].MemUsed -= chk.Sz
						p.LambdaPool[placements[i]].DelChunk(chk.Key)
						p.LambdaPool[idx].AddChunk(chk)
						p.LambdaPool[idx].MemUsed += chk.Sz
					}
					chk.Reset++
					p.LambdaPool[idx].Activate(obj.Time)
				}
				if displaced {
					p.SetPlacements(obj.Key, resetPlacements)
				}
			}
			return "get", reqId
		} else if reader != nil {
			reader.Close()
		}
		log.Trace("Get %s.", obj.Key)

		for i, idx := range placements {
			chk, ok := p.LambdaPool[idx].GetChunk(fmt.Sprintf("%d@%s", i, obj.Key))
			if !ok {
				log.Error("Unexpected key %s not found in %d", chk.Key, idx)
			}
			chk.Freq++
			p.LambdaPool[idx].Activate(obj.Time)
		}
		return "get", reqId
	} else {
		log.Trace("No placements found: %v", obj.Key)

		// if key does not exist, generate the index array holding
		// indexes of the destination lambdas
		var val []byte
		if !opts.Lean {
			val = make([]byte, obj.Sz)
			rand.Read(val)
		}
		placements := make([]int, opts.Datashard+opts.Parityshard)
		dryrun := 0
		if opts.Dryrun {
			dryrun = opts.Cluster
		}
		reqId, success := cli.EcSet(obj.Key, val, dryrun, placements, "Normal")
		if !success {
			return "set", reqId
		}

		placements = p.Remap(placements, obj)
		for i, idx := range placements {
			chkKey := fmt.Sprintf("%d@%s", i, obj.Key)
			chk := p.GetEvicted(chkKey)
			if chk == nil {
				chk = &proxy.Chunk{
					Key:  chkKey,
					Sz:   obj.ChunkSz,
					Freq: 0,
				}
			}
			p.ValidateLambda(idx)
			p.LambdaPool[idx].AddChunk(chk)
			p.LambdaPool[idx].MemUsed += chk.Sz
			if opts.Dryrun && opts.Balance {
				p.Adapt(idx, chk)
			}
			p.LambdaPool[idx].Activate(obj.Time)
		}
		log.Trace("Set %s, placements: %v.", obj.Key, placements)
		p.SetPlacements(obj.Key, placements)
		return "set", reqId
	}
}

func initProxies(nProxies int, opts *Options) ([]*proxy.Proxy, *consistent.Consistent) {
	proxies := make([]*proxy.Proxy, nProxies)
	members := []consistent.Member{}
	for i, _ := range proxies {
		var balancer proxy.ProxyBalancer
		if opts.Balance {
			// Balancer optiosn:
			//balancer = &proxy.LRUPlacer{}
			balancer = &proxy.PriorityBalancer{}
			//balancer = &proxy.WeightedBalancer{}
		}
		proxies[i] = proxy.NewProxy(strconv.Itoa(i), opts.Cluster, balancer)

		member := Member(proxies[i].Id)
		members = append(members, member)
	}

	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	ring := consistent.New(members, cfg)

	return proxies, ring
}

func helpInfo() {
	fmt.Fprintf(os.Stderr, "Usage: ./playback [options] tracefile\n")
	fmt.Fprintf(os.Stderr, "Available options:\n")
	flag.PrintDefaults()
}

func main() {
	var printInfo bool
	flag.BoolVar(&printInfo, "h", false, "help info?")

	options := &Options{}
	flag.StringVar(&options.AddrList, "addrlist", "127.0.0.1:6378", "proxy address:port")
	flag.IntVar(&options.Cluster, "cluster", 300, "number of instance per proxy")
	flag.IntVar(&options.Datashard, "d", 4, "number of data shards for RS erasure coding")
	flag.IntVar(&options.Parityshard, "p", 2, "number of parity shards for RS erasure coding")
	flag.IntVar(&options.ECmaxgoroutine, "g", 32, "max number of goroutines for RS erasure coding")
	flag.BoolVar(&options.NoDebug, "disable-debug", false, "disable printing debugging log?")
	flag.BoolVar(&options.SummaryOnly, "summary-only", false, "show summary only")
	flag.StringVar(&options.File, "file", "playback", "print result to file")
	flag.BoolVar(&options.Compact, "compact", false, "playback in compact mode")
	flag.Int64Var(&options.Interval, "i", 2000, "interval for every req (ms), valid only if compact=true")
	flag.BoolVar(&options.Dryrun, "dryrun", false, "no actual invocation")
	flag.BoolVar(&options.Lean, "lean", false, "run with minimum memory consumtion, valid only if dryrun=true")
	flag.Uint64Var(&options.MaxSz, "maxsz", 2147483648, "max object size")
	flag.Uint64Var(&options.ScaleFrom, "scalefrom", 104857600, "objects larger than this size will be scaled")
	flag.Float64Var(&options.ScaleSz, "scalesz", 1, "scale object size")
	flag.Int64Var(&options.Skip, "skip", 0, "skip N records")
	flag.StringVar(&options.S3, "s3", "", "s3 bucket for enable s3 simulation")
	flag.StringVar(&options.Redis, "redis", "", "Redis for enable Redis simulation")
	flag.BoolVar(&options.RedisCluster, "redisCluster", false, "redisCluster for enable Redis simulation")
	flag.BoolVar(&options.Balance, "balance", false, "enable balancer on dryrun")
	flag.IntVar(&options.Concurrency, "c", 0, "max concurrency allowed, default to be unlimited.")

	flag.Parse()

	if printInfo || flag.NArg() < 1 {
		helpInfo()
		os.Exit(0)
	}

	if options.NoDebug {
		log.Verbose = false
		log.Level = logger.LOG_LEVEL_INFO
	}
	if options.SummaryOnly {
		log.Verbose = false
		log.Level = logger.LOG_LEVEL_WARN
	}

	traceFile, err := os.Open(flag.Arg(0))
	if err != nil {
		log.Error("Failed to open trace file: %s", flag.Arg(0))
		os.Exit(1)
	}
	defer traceFile.Close()

	addrArr := strings.Split(options.AddrList, ",")
	proxies, ring := initProxies(len(addrArr), options)
	var cli benchclient.Client
	if options.S3 != "" {
		cli = benchclient.NewS3(options.S3)
	} else if options.Redis != "" {
		cli = benchclient.NewRedis(options.Redis)
	} else if options.RedisCluster == true {
		cli = benchclient.NewElasticCache()
	} else {
		cli = client.NewClient(options.Datashard, options.Parityshard, options.ECmaxgoroutine)
		if !options.Dryrun {
			cli.(*client.Client).Dial(addrArr)
		}
	}

	reader := csv.NewReader(bufio.NewReader(traceFile))
	// Skip first line
	_, err = reader.Read()
	if err == io.EOF {
		panic(errors.New(fmt.Sprintf("Empty file: %s", flag.Arg(0))))
	} else if err != nil {
		panic(err)
	}

	timer := time.NewTimer(0)
	start := time.Now()
	read := int64(0)
	var skipedDuration time.Duration
	var startObject *proxy.Object
	var lastObject *proxy.Object
	var concurrency int32
	var maxConcurrency int32
	cond := sync.NewCond(&sync.Mutex{})
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		sz, szErr := strconv.ParseFloat(line[9], 64)
		t, tErr := time.Parse(TIME_PATTERN, line[11][:len(TIME_PATTERN)])
		if tErr != nil {
			t, tErr = time.Parse(TIME_PATTERN2, line[11][:len(TIME_PATTERN2)])
		}
		if szErr != nil || tErr != nil {
			log.Warn("Error on parse record, skip %v: %v, %v", line, szErr, tErr)
			continue
		}
		obj := &proxy.Object{
			Key:  line[6],
			Sz:   uint64(sz),
			Time: t,
		}
		if obj.Sz > options.MaxSz {
			obj.Sz = options.MaxSz
		}
		if obj.Sz > options.ScaleFrom {
			obj.Sz = uint64(float64(obj.Sz) * options.ScaleSz)
		}
		obj.ChunkSz = obj.Sz / uint64(options.Datashard)

		if lastObject != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timeout := time.Duration(options.Interval) * time.Millisecond
			if options.Compact {
				next := obj.Time.Sub(lastObject.Time)
				if next < timeout {
					timeout = next
				}
			} else {
				// Use absolute time span for accuracy
				timeout = obj.Time.Sub(startObject.Time) - skipedDuration - time.Since(start)
			}
			if timeout <= 0 || (options.Dryrun && options.Compact) {
				timeout = 0
			}

			// On skiping, use elapsed to record time.
			if read >= options.Skip {
				if timeout > 0 {
					log.Info("Playback %d in %v", read+1, timeout)
				}
				timer.Reset(timeout)
			} else {
				skipedDuration += timeout
				if timeout > 0 {
					log.Info("Skip %d: %v", read+1, timeout)
				}
			}
		} else {
			startObject = obj
		}

		if read >= options.Skip {
			<-timer.C
			log.Info("%d Playbacking %v(exp %v, act %v)...", read+1, obj.Key, obj.Time.Sub(startObject.Time), skipedDuration+time.Since(start))
			member := ring.LocateKey([]byte(obj.Key))
			hostId := member.String()
			id, _ := strconv.Atoi(hostId)

			// Concurrency control
			cond.L.Lock()
			for options.Concurrency > 0 && atomic.LoadInt32(&concurrency) >= int32(options.Concurrency) {
				cond.Wait()
			}
			maxConcurrency = MaxInt32(maxConcurrency, atomic.AddInt32(&concurrency, 1))

			// Start perform
			go func(p *proxy.Proxy, obj *proxy.Object, timeStartObject time.Time, start time.Time, skipped time.Duration) {
				_, reqId := perform(options, cli, p, obj)
				log.Debug("csv,%s,%s,%d,%d", reqId, obj.Key, int64(obj.Time.Sub(timeStartObject)), int64(skipped+time.Since(start)))
				atomic.AddInt32(&concurrency, -1)
				cond.Signal()
			}(proxies[id], obj, startObject.Time, start, skipedDuration)

			cond.L.Unlock()
		}

		lastObject = obj
		read++
	}

	totalMem := float64(0)
	maxMem := float64(0)
	minMem := float64(10000000000000)
	maxChunks := float64(0)
	minChunks := float64(1000)
	set := 0
	got := uint64(0)
	reset := uint64(0)
	activated := 0
	var balancerCost time.Duration
	for i := 0; i < len(proxies); i++ {
		prxy := proxies[i]
		for j := 0; j < len(prxy.LambdaPool); j++ {
			lambda := prxy.LambdaPool[j]
			totalMem += float64(lambda.MemUsed)
			minMem = math.Min(minMem, float64(lambda.MemUsed))
			maxMem = math.Max(maxMem, float64(lambda.MemUsed))
			minChunks = math.Min(minChunks, float64(lambda.NumChunks()))
			maxChunks = math.Max(maxChunks, float64(lambda.NumChunks()))
			set += lambda.NumChunks()
			for chk := range lambda.AllChunks() {
				got += chk.Value.(*proxy.Chunk).Freq
				reset += chk.Value.(*proxy.Chunk).Reset
			}
			activated += lambda.ActiveMinutes
		}
		for chk := range prxy.AllEvicts() {
			got += chk.Value.(*proxy.Chunk).Freq
			reset += chk.Value.(*proxy.Chunk).Reset
		}
		balancerCost += prxy.BalancerCost
		prxy.Close()
	}
	syslog.Printf("Total records: %d\n", read-options.Skip)
	syslog.Printf("Total memory consumed: %s\n", humanize.Bytes(uint64(totalMem)))
	syslog.Printf("Memory consumed per lambda: %s - %s\n", humanize.Bytes(uint64(minMem)), humanize.Bytes(uint64(maxMem)))
	syslog.Printf("Chunks per lambda: %d - %d\n", int(minChunks), int(maxChunks))
	syslog.Printf("Set %d, Got %d, Reset %d\n", set, got, reset)
	syslog.Printf("Active Minutes %d\n", activated)
	syslog.Printf("BalancerCost: %s(%s per request)", balancerCost, balancerCost/time.Duration(read-options.Skip))
	syslog.Printf("Max concurrency: %d\n", maxConcurrency)
}

func MaxInt32(a int32, b int32) int32 {
	if a < b {
		return b
	} else {
		return a
	}
}
