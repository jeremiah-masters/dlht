package benchmarks

import (
	_ "embed"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
)

//go:embed count_3l.txt
var rawTrigrams string

// trigramSampler provides weighted random sampling from English trigram frequencies.
// Source: Peter Norvig's count_3l.txt (Google Books Ngram Corpus, CC-BY 3.0).
// https://norvig.com/ngrams/count_3l.txt
// Contains all 17,576 (26^3) lowercase letter trigrams with occurrence counts.
type trigramSampler struct {
	trigrams   []string
	cumWeights []float64
	total      float64
}

func newTrigramSampler() *trigramSampler {
	s := &trigramSampler{}
	cum := 0.0
	for _, line := range strings.Split(strings.TrimSpace(rawTrigrams), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 2 {
			continue
		}
		w, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		cum += w
		s.trigrams = append(s.trigrams, parts[0])
		s.cumWeights = append(s.cumWeights, cum)
	}
	s.total = cum
	return s
}

func (s *trigramSampler) sample(rng *rand.Rand) string {
	r := rng.Float64() * s.total
	idx := sort.SearchFloat64s(s.cumWeights, r)
	if idx >= len(s.trigrams) {
		idx = len(s.trigrams) - 1
	}
	return s.trigrams[idx]
}
