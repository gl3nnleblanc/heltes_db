package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	flagAddr       = flag.String("addr", "[::1]:50052", "coordinator gRPC address")
	flagWorkers    = flag.Int("workers", 100, "concurrent goroutines")
	flagDur        = flag.Duration("dur", 30*time.Second, "benchmark duration")
	flagKeyspace   = flag.Uint64("keyspace", 10_000, "distinct key space size")
	flagUpdatesPerTx = flag.Int("updates", 1, "updates per transaction")
	flagWarmup     = flag.Duration("warmup", 2*time.Second, "warmup period (excluded from stats)")
	flagProto      = flag.String("proto", "../proto/heltes_db.proto", "path to proto file")
)

var (
	nCommitted atomic.Int64
	nAborted   atomic.Int64
	nErrors    atomic.Int64
)

type sample struct {
	dur     time.Duration
	aborted bool
}

func main() {
	flag.Parse()

	// ── Connect ───────────────────────────────────────────────────────────────
	conn, err := grpc.NewClient(
		*flagAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	// ── Parse proto ───────────────────────────────────────────────────────────
	parser := protoparse.Parser{
		ImportPaths: []string{filepath.Dir(*flagProto)},
	}
	fds, err := parser.ParseFiles(filepath.Base(*flagProto))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse proto: %v\n", err)
		os.Exit(1)
	}
	svc := fds[0].FindService("heltes_db.CoordinatorService")
	if svc == nil {
		fmt.Fprintln(os.Stderr, "CoordinatorService not found")
		os.Exit(1)
	}
  fmt.Fprintln(os.Stderr, "Found CoordinatorService")

	stub := grpcdynamic.NewStub(conn)
	mBegin  := svc.FindMethodByName("Begin")
	mUpdate := svc.FindMethodByName("Update")
	mCommit := svc.FindMethodByName("Commit")

	run := func(ctx context.Context, out chan<- sample) {
		var wg sync.WaitGroup
		for i := 0; i < *flagWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
				newMsg := func(md *desc.MessageDescriptor) *dynamic.Message {
					return dynamic.NewMessage(md)
				}
				for ctx.Err() == nil {
					t0 := time.Now()

					// Begin
					beginRaw, err := stub.InvokeRpc(ctx, mBegin, newMsg(mBegin.GetInputType()))
					if err != nil {
            fmt.Fprintf(os.Stderr, "begin failed: %v\n", err)
						nErrors.Add(1)
						continue
					}
					txId := beginRaw.(*dynamic.Message).GetFieldByName("tx_id").(uint64)

					// Updates
					abrt := false
					for i := 0; i < *flagUpdatesPerTx; i++ {
						req := newMsg(mUpdate.GetInputType())
						req.SetFieldByName("tx_id", txId)
						req.SetFieldByName("key", rng.Uint64N(*flagKeyspace))
						req.SetFieldByName("value", rng.Uint64())
						resp, err := stub.InvokeRpc(ctx, mUpdate, req)
						if err != nil {
              fmt.Fprintf(os.Stderr, "update failed: %v\n", err)
							nErrors.Add(1)
							abrt = true
							break
						}
						ok, _ := resp.(*dynamic.Message).GetFieldByName("ok").(bool)
						if !ok {
							nAborted.Add(1)
							out <- sample{dur: time.Since(t0), aborted: true}
							abrt = true
							break
						}
					}
					if abrt {
						continue
					}

					// Commit
					commitReq := newMsg(mCommit.GetInputType())
					commitReq.SetFieldByName("tx_id", txId)
					commitResp, err := stub.InvokeRpc(ctx, mCommit, commitReq)
					if err != nil {
            fmt.Fprintf(os.Stderr, "commit failed: %v\n", err)
						nErrors.Add(1)
						continue
					}
					commitTs, _ := commitResp.(*dynamic.Message).GetFieldByName("commit_ts").(uint64)
					if commitTs == 0 {
						nAborted.Add(1)
						out <- sample{dur: time.Since(t0), aborted: true}
						continue
					}
					nCommitted.Add(1)
					out <- sample{dur: time.Since(t0)}
				}
			}()
		}
		wg.Wait()
	}

	// ── Warmup ────────────────────────────────────────────────────────────────
	fmt.Printf("warming up (%v, %d workers) ...\n", *flagWarmup, *flagWorkers)
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), *flagWarmup)
	warmupSamples := make(chan sample, 100_000)
	go func() { run(warmupCtx, warmupSamples); close(warmupSamples) }()
	for range warmupSamples {}
	warmupCancel()
	nCommitted.Store(0); nAborted.Store(0); nErrors.Store(0)

	// ── Benchmark ─────────────────────────────────────────────────────────────
	fmt.Printf("\nhammering %s  workers=%-4d  keyspace=%-6d  updates/tx=%d  dur=%v\n\n",
		*flagAddr, *flagWorkers, *flagKeyspace, *flagUpdatesPerTx, *flagDur)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	benchCtx, benchCancel := context.WithTimeout(ctx, *flagDur)
	defer benchCancel()

	samples := make(chan sample, 500_000)
	var collected []time.Duration
	var collDone sync.WaitGroup
	collDone.Add(1)
	go func() {
		defer collDone.Done()
		for s := range samples {
			if !s.aborted {
				collected = append(collected, s.dur)
			}
		}
	}()

	// Progress ticker
	ticker := time.NewTicker(time.Second)
	start := time.Now()
	var prevC, prevA, prevE int64
	go func() {
		for range ticker.C {
			c, a, e := nCommitted.Load(), nAborted.Load(), nErrors.Load()
			fmt.Printf("\r[%5.1fs]  committed %7d  (+%5d/s)  aborted %6d  (+%4d/s)  errors %4d",
				time.Since(start).Seconds(), c, c-prevC, a, a-prevA, e-prevE)
			prevC, prevA, prevE = c, a, e
		}
	}()

	run(benchCtx, samples)
	close(samples)
	ticker.Stop()
	collDone.Wait()
	elapsed := time.Since(start)

	// ── Results ───────────────────────────────────────────────────────────────
	c, a, e := nCommitted.Load(), nAborted.Load(), nErrors.Load()
	tps := float64(c) / elapsed.Seconds()
	abortPct := 100.0 * float64(a) / float64(max(c+a, 1))

	fmt.Printf("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Printf("duration     %v\n", elapsed.Round(time.Millisecond))
	fmt.Printf("committed    %d  (%.0f tx/s)\n", c, tps)
	fmt.Printf("aborted      %d  (%.1f%%)\n", a, abortPct)
	fmt.Printf("errors       %d\n", e)

	if len(collected) > 0 {
		sort.Slice(collected, func(i, j int) bool { return collected[i] < collected[j] })
		fmt.Printf("\nlatency (committed transactions only)\n")
		fmt.Printf("  p50   %v\n", pct(collected, 50))
		fmt.Printf("  p90   %v\n", pct(collected, 90))
		fmt.Printf("  p95   %v\n", pct(collected, 95))
		fmt.Printf("  p99   %v\n", pct(collected, 99))
		fmt.Printf("  p99.9 %v\n", pct(collected, 99.9))
		fmt.Printf("  max   %v\n", collected[len(collected)-1].Round(time.Microsecond))
		fmt.Printf("\n")
		printHistogram(collected)
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	idx := int(float64(len(sorted)-1) * p / 100.0)
	return sorted[idx].Round(time.Microsecond)
}

func printHistogram(sorted []time.Duration) {
	min, max := sorted[0], sorted[len(sorted)-1]
	buckets := 10
	width := (max - min) / time.Duration(buckets)
	if width == 0 {
		return
	}
	counts := make([]int, buckets)
	for _, d := range sorted {
		b := int((d - min) / width)
		if b >= buckets {
			b = buckets - 1
		}
		counts[b]++
	}
	peak := 0
	for _, c := range counts {
		if c > peak {
			peak = c
		}
	}
	barWidth := 40
	fmt.Println("latency distribution:")
	for i, c := range counts {
		lo := min + time.Duration(i)*width
		hi := lo + width
		bar := int(float64(c) / float64(peak) * float64(barWidth))
		fmt.Printf("  %7v – %-7v │%s %d\n",
			lo.Round(time.Microsecond),
			hi.Round(time.Microsecond),
			fill(bar, '█'),
			c,
		)
	}
}

func fill(n int, ch rune) string {
	s := make([]rune, n)
	for i := range s {
		s[i] = ch
	}
	return string(s)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
