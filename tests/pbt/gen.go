package pbt

import (
	"fmt"

	"pgregory.net/rapid"
)

type OpKind uint8

const (
	OpGet OpKind = iota
	OpInsert
	OpPut
	OpDelete
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "Get"
	case OpInsert:
		return "Insert"
	case OpPut:
		return "Put"
	case OpDelete:
		return "Delete"
	default:
		return fmt.Sprintf("Unknown(%d)", k)
	}
}

type Op[K comparable, V any] struct {
	Kind  OpKind
	Key   K
	Value V
}

type OpMix struct {
	Get    int
	Insert int
	Put    int
	Delete int
}

var (
	MixBalanced = OpMix{Get: 25, Insert: 25, Put: 25, Delete: 25}
	MixChurn    = OpMix{Get: 10, Insert: 30, Put: 30, Delete: 30}
)

func (m OpMix) total() int {
	return m.Get + m.Insert + m.Put + m.Delete
}

func (m OpMix) normalized() OpMix {
	if m.total() == 0 {
		return MixBalanced
	}
	return m
}

func DrawOpKind(t *rapid.T, mix OpMix) OpKind {
	m := mix.normalized()
	r := rapid.IntRange(0, m.total()-1).Draw(t, "opPick")
	if r < m.Get {
		return OpGet
	}
	r -= m.Get
	if r < m.Insert {
		return OpInsert
	}
	r -= m.Insert
	if r < m.Put {
		return OpPut
	}
	return OpDelete
}

func GenStringKey(keyspace int) *rapid.Generator[string] {
	return rapid.Map(rapid.IntRange(0, keyspace-1), func(idx int) string {
		return fmt.Sprintf("key_%d", idx)
	})
}

func genHotspotIndex(keyspace, hotspotSize, hotspotPercent int) *rapid.Generator[int] {
	if keyspace <= 1 {
		return rapid.Just(0)
	}
	if hotspotSize <= 0 || hotspotSize >= keyspace {
		hotspotSize = 1
	}
	if hotspotPercent < 0 {
		hotspotPercent = 0
	}
	if hotspotPercent > 100 {
		hotspotPercent = 100
	}
	return rapid.Custom(func(t *rapid.T) int {
		roll := rapid.IntRange(0, 99).Draw(t, "hotspotRoll")
		if roll < hotspotPercent {
			return rapid.IntRange(0, hotspotSize-1).Draw(t, "hotspotIndex")
		}
		return rapid.IntRange(hotspotSize, keyspace-1).Draw(t, "coldIndex")
	})
}

func GenHotspotStringKey(keyspace, hotspotSize, hotspotPercent int) *rapid.Generator[string] {
	return rapid.Map(genHotspotIndex(keyspace, hotspotSize, hotspotPercent), func(idx int) string {
		if idx < hotspotSize {
			return fmt.Sprintf("hot_%d", idx)
		}
		return fmt.Sprintf("cold_%d", idx)
	})
}

func GenHotspotUint64Key(keyspace, hotspotSize, hotspotPercent int) *rapid.Generator[uint64] {
	return rapid.Map(genHotspotIndex(keyspace, hotspotSize, hotspotPercent), func(idx int) uint64 {
		return uint64(idx)
	})
}

func AdversarialKeyCorpus() []string {
	return []string{
		"key", "key0", "key00", "key000", "key0000",
		"Key", "KEY", "kEy", "keY",
		"key_0", "key_1", "key_01", "key_001",
		"aaaaaaaa", "aaaaaaab", "aaaaaaba", "aaaabaaa",
		"zzzzzzzz", "zzzzzzzy", "zzzzzyzz", "zzzyzzzz",
		"01234567", "0123456x", "01234x67", "0x234567",
		"____", "___a", "__a_", "_a__", "a___",
		"long_key_with_prefix_aaaaaaaa", "long_key_with_prefix_aaaaaaab",
	}
}

func AdversarialUint64Corpus() []uint64 {
	return []uint64{
		0, 1, 2, 3, 4, 5, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128,
		255, 256, 511, 512, 1023, 1024, 2047, 2048,
		1<<32 - 1, 1 << 32, 1<<48 - 1, 1 << 48,
		1<<63 - 1, 1 << 63,
		0xAAAAAAAAAAAAAAAA, 0x5555555555555555,
		0x00000000FFFFFFFF, 0xFFFFFFFF00000000,
	}
}

func GenUint64Key(keyspace int) *rapid.Generator[uint64] {
	return rapid.Map(rapid.IntRange(0, keyspace-1), func(idx int) uint64 {
		return uint64(idx)
	})
}

func GenOp[K comparable, V any](keyGen *rapid.Generator[K], valGen *rapid.Generator[V], mix OpMix) *rapid.Generator[Op[K, V]] {
	return rapid.Custom(func(t *rapid.T) Op[K, V] {
		kind := DrawOpKind(t, mix)
		key := keyGen.Draw(t, "key")
		var value V
		if kind == OpInsert || kind == OpPut {
			value = valGen.Draw(t, "value")
		}
		return Op[K, V]{Kind: kind, Key: key, Value: value}
	})
}

func GenOpSequence[K comparable, V any](keyGen *rapid.Generator[K], valGen *rapid.Generator[V], mix OpMix, minLen, maxLen int) *rapid.Generator[[]Op[K, V]] {
	return rapid.SliceOfN(GenOp(keyGen, valGen, mix), minLen, maxLen)
}
