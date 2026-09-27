package artifact

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/klauspost/compress/zstd"
	"github.com/yasyf/daemonkit/durable"
)

const (
	packEnd      byte = 0
	packBlob     byte = 1
	packManifest byte = 2
)

func newEncoder() (*zstd.Encoder, error) {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(WindowSize),
		zstd.WithEncoderCRC(true))
	if err != nil {
		return nil, fmt.Errorf("artifact: zstd encoder: %w", err)
	}
	return encoder, nil
}

func newDecoder(r io.Reader) (*zstd.Decoder, error) {
	decoder, err := zstd.NewReader(r,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(WindowSize),
		zstd.WithDecoderMaxMemory(uint64(maxPackBytes)))
	if err != nil {
		return nil, fmt.Errorf("artifact: zstd decoder: %w", err)
	}
	return decoder, nil
}

func packKind(kind Kind) byte {
	if kind == KindManifest {
		return packManifest
	}
	return packBlob
}

type packWriter struct {
	w io.Writer
	n int64
}

func (p *packWriter) write(b []byte) error {
	n, err := p.w.Write(b)
	p.n += int64(n)
	if err != nil {
		return fmt.Errorf("artifact: write pack: %w", err)
	}
	return nil
}

func (p *packWriter) begin() error {
	return p.write([]byte(packMagic))
}

func (p *packWriter) object(entry ObjectEntry, data []byte) error {
	digest, err := hex.DecodeString(string(entry.Digest))
	if err != nil {
		return fmt.Errorf("%w: object digest %s: %w", ErrInvalid, entry.Digest, err)
	}
	header := make([]byte, 0, 1+sha256.Size+binary.MaxVarintLen64)
	header = append(header, packKind(entry.Kind))
	header = append(header, digest...)
	header = binary.AppendUvarint(header, uint64(entry.Size)) //nolint:gosec // G115: a validated entry's size is positive.
	if err := p.write(header); err != nil {
		return err
	}
	return p.write(data)
}

func (p *packWriter) end() error {
	return p.write([]byte{packEnd})
}

type partWriter struct {
	dir   string
	buf   []byte
	parts []PartRef
}

func (w *partWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n := copy(w.buf[len(w.buf):PartSize], p)
		w.buf, p, written = w.buf[:len(w.buf)+n], p[n:], written+n
		if len(w.buf) == PartSize {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *partWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	if err := durable.WriteFile(filepath.Join(w.dir, partName(len(w.parts))), w.buf, filePerm); err != nil {
		return fmt.Errorf("artifact: write part %d: %w", len(w.parts), err)
	}
	w.parts = append(w.parts, PartRef{Digest: Sum(w.buf), Size: int64(len(w.buf))})
	w.buf = w.buf[:0]
	return nil
}

type packReader struct {
	r *bufio.Reader
	n int64
}

func (p *packReader) ReadByte() (byte, error) {
	b, err := p.r.ReadByte()
	if err == nil {
		p.n++
	}
	return b, err
}

func (p *packReader) full(b []byte) error {
	n, err := io.ReadFull(p.r, b)
	p.n += int64(n)
	return err
}

func scanPack(ctx context.Context, r io.Reader, d BatchDescriptor, each func(ObjectEntry, []byte) error) error {
	decoder, err := newDecoder(r)
	if err != nil {
		return err
	}
	defer decoder.Close()
	pack := &packReader{r: bufio.NewReader(io.LimitReader(decoder, d.RawSize+1))}
	magic := make([]byte, len(packMagic))
	if err := pack.full(magic); err != nil {
		return malformed(d, "read magic: %w", err)
	}
	if string(magic) != packMagic {
		return malformed(d, "pack magic %q", magic)
	}
	buf := make([]byte, max(ChunkSize, MaxManifestBytes))
	var digest [sha256.Size]byte
	for i, entry := range d.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		kind, err := pack.ReadByte()
		if err != nil {
			return malformed(d, "object %d kind: %w", i, err)
		}
		if kind != packKind(entry.Kind) {
			return malformed(d, "object %d kind byte %d, descriptor says %s", i, kind, entry.Kind)
		}
		if err := pack.full(digest[:]); err != nil {
			return malformed(d, "object %d digest: %w", i, err)
		}
		if Digest(hex.EncodeToString(digest[:])) != entry.Digest {
			return malformed(d, "object %d digest %x, descriptor says %s", i, digest, entry.Digest)
		}
		size, err := binary.ReadUvarint(pack)
		if err != nil {
			return malformed(d, "object %d size: %w", i, err)
		}
		if size != uint64(entry.Size) { //nolint:gosec // G115: a validated entry's size is positive.
			return malformed(d, "object %d size %d, descriptor says %d", i, size, entry.Size)
		}
		data := buf[:size]
		if err := pack.full(data); err != nil {
			return malformed(d, "object %d bytes: %w", i, err)
		}
		if Sum(data) != entry.Digest {
			return malformed(d, "object %d bytes do not hash to %s", i, entry.Digest)
		}
		if err := each(entry, data); err != nil {
			return err
		}
	}
	end, err := pack.ReadByte()
	if err != nil {
		return malformed(d, "terminator: %w", err)
	}
	if end != packEnd {
		return malformed(d, "terminator byte %d", end)
	}
	_, err = pack.ReadByte()
	if err == nil {
		return malformed(d, "pack stream runs past %d bytes", d.RawSize)
	}
	if !errors.Is(err, io.EOF) {
		return malformed(d, "pack stream end: %w", err)
	}
	if pack.n != d.RawSize {
		return malformed(d, "pack stream is %d bytes, descriptor says %d", pack.n, d.RawSize)
	}
	return nil
}

func malformed(d BatchDescriptor, format string, args ...any) error {
	return fmt.Errorf("%w: batch %s: "+format, append([]any{ErrInvalid, d.ID}, args...)...)
}
