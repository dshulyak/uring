package uring

import (
	"testing"
)

func BenchmarkSQEntryReset(b *testing.B) {
	var sqe SQEntry
	for i := 0; i < b.N; i++ {
		sqe.Reset()
	}
	if sqe.userData != 0 {
		b.Error("dummy test to prevent optimization")
	}
}
