package nlp

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

type MinHashLSH struct {
	numHashes  int
	numBands   int
	rowsPerBand int
	aCoeffs    []uint64
	bCoeffs    []uint64
	prime      uint64
	buckets    map[string][]uuid.UUID
	mu         sync.RWMutex
}

func NewMinHashLSH(numHashes, numBands int) *MinHashLSH {
	if numHashes <= 0 {
		numHashes = 64
	}
	if numBands <= 0 {
		numBands = 16
	}

	rows := numHashes / numBands
	p := uint64(4294967311)
	r := rand.New(rand.NewSource(1337))

	a := make([]uint64, numHashes)
	b := make([]uint64, numHashes)
	for i := 0; i < numHashes; i++ {
		a[i] = uint64(r.Int63n(int64(p-1)) + 1)
		b[i] = uint64(r.Int63n(int64(p-1)) + 1)
	}

	return &MinHashLSH{
		numHashes:   numHashes,
		numBands:    numBands,
		rowsPerBand: rows,
		aCoeffs:     a,
		bCoeffs:     b,
		prime:       p,
		buckets:     make(map[string][]uuid.UUID),
	}
}

func (l *MinHashLSH) ComputeSignature(text string) []uint64 {
	words := strings.Fields(strings.ToLower(text))
	if len(words) < 3 {
		shingleHash := hashString(text)
		sig := make([]uint64, l.numHashes)
		for i := 0; i < l.numHashes; i++ {
			sig[i] = (l.aCoeffs[i]*shingleHash + l.bCoeffs[i]) % l.prime
		}
		return sig
	}

	shingleHashes := make([]uint64, 0, len(words)-2)
	for i := 0; i <= len(words)-3; i++ {
		shingle := words[i] + " " + words[i+1] + " " + words[i+2]
		shingleHashes = append(shingleHashes, hashString(shingle))
	}

	sig := make([]uint64, l.numHashes)
	for i := 0; i < l.numHashes; i++ {
		minH := uint64(mathMaxUint64())
		a := l.aCoeffs[i]
		b := l.bCoeffs[i]
		for _, sh := range shingleHashes {
			val := (a*sh + b) % l.prime
			if val < minH {
				minH = val
			}
		}
		sig[i] = minH
	}

	return sig
}

func (l *MinHashLSH) FindNearDuplicate(ctx context.Context, sig []uint64) (bool, uuid.UUID) {
	_, span := otel.Tracer("nlp.lsh").Start(ctx, "MinHashLSH.FindNearDuplicate")
	defer span.End()

	l.mu.RLock()
	defer l.mu.RUnlock()

	for band := 0; band < l.numBands; band++ {
		startIdx := band * l.rowsPerBand
		endIdx := startIdx + l.rowsPerBand

		bandSig := sig[startIdx:endIdx]
		bucketKey := l.getBucketKey(band, bandSig)

		if candidates, exists := l.buckets[bucketKey]; exists && len(candidates) > 0 {
			return true, candidates[0]
		}
	}

	return false, uuid.Nil
}

func (l *MinHashLSH) IndexSignature(sig []uint64, vectorID uuid.UUID) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for band := 0; band < l.numBands; band++ {
		startIdx := band * l.rowsPerBand
		endIdx := startIdx + l.rowsPerBand

		bandSig := sig[startIdx:endIdx]
		bucketKey := l.getBucketKey(band, bandSig)

		l.buckets[bucketKey] = append(l.buckets[bucketKey], vectorID)
	}
}

func (l *MinHashLSH) getBucketKey(band int, bandSig []uint64) string {
	h := md5.New()
	for _, val := range bandSig {
		binary.Write(h, binary.LittleEndian, val)
	}
	return fmt.Sprintf("%d:%s", band, hex.EncodeToString(h.Sum(nil)))
}

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func mathMaxUint64() uint64 {
	return ^uint64(0)
}
