package artifact

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"os"
	"testing"
	"time"
)

func TestMeasureCommitBatch(t *testing.T) {
	if os.Getenv("SYNCKIT_MEASURE_COMMIT") == "" {
		t.Skip("set SYNCKIT_MEASURE_COMMIT=1 to time CommitBatch on this machine")
	}
	tests := []struct {
		name    string
		objects int
		size    int
	}{
		{"max objects", MaxBatchObjects, 64},
		{"max bytes", MaxBatchRaw/ChunkSize - 1, ChunkSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			src, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = src.Close() })
			dst, err := Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dst.Close() })
			entries := make([]ObjectEntry, tt.objects)
			for i := range entries {
				b := make([]byte, tt.size)
				if _, err := rand.Read(b); err != nil {
					t.Fatal(err)
				}
				binary.BigEndian.PutUint64(b, uint64(i))
				digest := Sum(b)
				if err := os.WriteFile(src.objectPath(digest), b, filePerm); err != nil {
					t.Fatal(err)
				}
				entries[i] = ObjectEntry{Digest: digest, Kind: KindBlob, Size: int64(len(b))}
			}
			batch, err := src.BuildBatch(ctx, entries)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := dst.BeginBatch(ctx, batch); err != nil {
				t.Fatal(err)
			}
			for index := range batch.Parts {
				data, err := src.ReadPart(ctx, batch.ID, index)
				if err != nil {
					t.Fatal(err)
				}
				if err := dst.PutPart(ctx, batch.ID, index, data); err != nil {
					t.Fatal(err)
				}
			}
			barriers := 0
			flushBarrier = func(dir string) error {
				barriers++
				return fullSyncDir(dir)
			}
			t.Cleanup(func() { flushBarrier = fullSyncDir })
			dirs := map[string]struct{}{}
			for _, entry := range entries {
				dirs[string(entry.Digest[:2])] = struct{}{}
			}
			start := time.Now()
			report, err := dst.CommitBatch(ctx, batch.ID)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("CommitBatch %d objects, %d raw bytes: stored %d in %s; %d F_FULLFSYNC barriers, %d plain object fsyncs, %d plain dir fsyncs",
				tt.objects, batch.RawSize, report.Stored, time.Since(start), barriers, report.Stored, len(dirs))
		})
	}
}
