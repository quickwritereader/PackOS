package access

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/BranchAndLink/packos/types"
)

type SeqGetAccess struct {
	buf           []byte     // full packed buffer
	count         int        // number of args (typeEnd included)
	base          int        // payload start offset
	pos           int        // current field index
	nextOffset    int        // absolute offset of next field start
	nextType      types.Type // decoded type tag of next field
	currentOffset int        // absolute offset of last field start
	currentType   types.Type // decoded type tag of last field
}

func NewSeqGetAccess(buf []byte) (*SeqGetAccess, error) {
	if len(buf) < 4 {
		return nil, errors.New("insufficient header")
	}

	base, ct := types.DecodeHeader(binary.LittleEndian.Uint16(buf[0:]))
	count := base / 2
	if len(buf) < base {
		return nil, errors.New("insufficient header")
	}

	h := binary.LittleEndian.Uint16(buf[2:])
	offset, nt := types.DecodeHeader(h)
	next := offset + base

	return &SeqGetAccess{
		buf:           buf,
		count:         count,
		base:          base,
		pos:           0,
		currentOffset: base,
		currentType:   ct,
		nextOffset:    next,
		nextType:      nt,
	}, nil
}

func (s *SeqGetAccess) ArgCount() int {
	return s.count - 1 //do not count TypeEnd
}

func (s *SeqGetAccess) UnderlineBuffer() []byte {
	return s.buf
}

func (s *SeqGetAccess) CurrentIndex() int {
	return s.pos
}

func (s *SeqGetAccess) PeekTypeWidth() (types.Type, int, error) {
	if s.pos >= s.count {
		return 0, 0, fmt.Errorf("PeekTypeWidth: out of bounds at pos %d", s.pos)
	}

	width := s.nextOffset - s.currentOffset
	if s.nextOffset > len(s.buf) {
		return s.currentType, -1, fmt.Errorf(
			"PeekTypeWidth: invalid range %d → %d exceeds buffer length %d",
			s.currentOffset, s.nextOffset, len(s.buf),
		)
	}

	return s.currentType, width, nil
}

func (s *SeqGetAccess) Advance() error {
	if s.pos+2 > s.count {
		return fmt.Errorf("Advance: out of bounds at pos %d", s.pos)
	}

	s.pos++
	s.currentOffset = s.nextOffset
	s.currentType = s.nextType
	//get next type if is exist
	h := binary.LittleEndian.Uint16(s.buf[(s.pos+1)*2:])
	end, nt := types.DecodeHeader(h)
	end += s.base
	s.nextOffset = end
	s.nextType = nt
	return nil
}

func (s *SeqGetAccess) PeekNestedSeq() (*SeqGetAccess, error) {
	if s.currentType != types.TypeMap && s.currentType != types.TypeTuple {
		return nil, fmt.Errorf("peekNestedSeq: current type is not Map or Tuple (got %v)", s.currentType)
	}

	width := s.nextOffset - s.currentOffset
	if width <= 0 || s.nextOffset > len(s.buf) {
		return nil, fmt.Errorf("peekNestedSeq: invalid range %d → %d", s.currentOffset, s.nextOffset)
	}

	nestedBuf := s.buf[s.currentOffset:s.nextOffset]
	nested, err := NewSeqGetAccess(nestedBuf)
	if err != nil {
		return nil, fmt.Errorf("peekNestedSeq: failed to initialize nested accessor %w", err)
	}
	return nested, nil
}

func (s *SeqGetAccess) Next() ([]byte, types.Type, error) {
	typ, width, err := s.PeekTypeWidth()
	if err != nil {
		return nil, 0, fmt.Errorf("next: peek failed at pos %d: %w", s.pos, err)
	}
	if width < 0 || s.currentOffset+width > len(s.buf) {
		return nil, 0, fmt.Errorf("next: invalid range %d → %d", s.currentOffset, s.currentOffset+width)
	}

	payload := s.buf[s.currentOffset : s.currentOffset+width]

	if err := s.Advance(); err != nil {
		return nil, 0, fmt.Errorf("next: advance failed at pos %d: %w", s.pos, err)
	}

	return payload, typ, nil
}
