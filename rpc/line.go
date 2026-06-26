package rpc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
)

// ReadLine reads one '\n'-terminated line from r, bounding the total at limit bytes.
// It accumulates the bufio chunks until the delimiter, returning the line without
// the trailing '\n'. If limit bytes arrive with no '\n', it returns an error rather
// than truncating or buffering further, so a peer cannot stream an unbounded line.
func ReadLine(r *bufio.Reader, limit int) ([]byte, error) {
	var buf bytes.Buffer
	for {
		chunk, err := r.ReadSlice('\n')
		if buf.Len()+len(chunk) > limit {
			return nil, fmt.Errorf("line exceeds %d bytes without a newline", limit)
		}
		buf.Write(chunk)
		switch {
		case err == nil:
			return bytes.TrimSuffix(buf.Bytes(), []byte{'\n'}), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		default:
			return nil, err
		}
	}
}
