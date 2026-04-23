package benchmarks

import (
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

const (
	defaultKeyspaceSize = 100_000
	defaultSeed         = 12345
	opsMultiplier       = 10 // ops = keyspaceSize * opsMultiplier
)

// BenchmarkThroughput_Uint64 runs all workload * distribution * resize mode combinations
// for uint64 keys across all map implementations.
func BenchmarkThroughput_Uint64(b *testing.B) {
	runThroughputSuite(b, Uint64MapFactories(), Uint64KeyGens())
}

// BenchmarkThroughput_String runs all workload * distribution * resize mode combinations
// for string keys across all map implementations.
func BenchmarkThroughput_String(b *testing.B) {
	runThroughputSuite(b, StringMapFactories(), StringKeyGens())
}

func runThroughputSuite[K comparable](
	b *testing.B,
	factories []NamedFactory[K, K],
	keygens []KeyGenConfig,
) {
	keyspaceSize := defaultKeyspaceSize
	numOps := keyspaceSize * opsMultiplier

	for _, w := range Scenarios {
		b.Run(w.Name, func(b *testing.B) {
			for _, kg := range keygens {
				b.Run(kg.Name, func(b *testing.B) {
					// Pre-generate keys
					rawKeys := kg.Generate(keyspaceSize, defaultSeed)
					keys := rawKeys.([]K)

					for _, rm := range []string{"PreSized", "Grow"} {
						b.Run(rm, func(b *testing.B) {
							// Pre-generate op stream (same for all maps in this combo)
							rng := rand.New(rand.NewPCG(defaultSeed, 0))
							ops := BuildOpStream(w, numOps, keyspaceSize, rng, kg.SampleIndex)
							prefillN := PrefillCount(w, keyspaceSize)

							for _, f := range factories {
								b.Run(f.Name, func(b *testing.B) {
									initCap := keyspaceSize
									if rm == "Grow" {
										initCap = 1
									}

									m := f.New(initCap)

									for i := 0; i < prefillN; i++ {
										m.Insert(keys[i], keys[i])
									}

									b.ReportAllocs()
									b.ResetTimer()

									// Distribute ops across goroutines using golden ratio offset
									var counter atomic.Uint64
									nOps := uint64(len(ops))

									b.RunParallel(func(pb *testing.PB) {
										// Choose a random offset to start operation sequence per goroutinue
										// uses a seeded rng to generate deterministic offsets
										id := counter.Add(1) - 1
										i := uint64(rand.New(rand.NewPCG(defaultSeed, id)).IntN(len(ops)))

										for pb.Next() {
											op := ops[i]
											key := keys[op.KeyIdx]
											switch op.Type {
											case OpGet:
												m.Get(key)
											case OpInsert:
												m.Insert(key, key)
											case OpDelete:
												m.Delete(key)
											case OpPut:
												m.Put(key, key)
											}
											i++
											if i >= nOps {
												i = 0
											}
										}
									})

									b.StopTimer()
									m.Close()
								})
							}
						})
					}
				})
			}
		})
	}
}
