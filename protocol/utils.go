package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

var errWrite = errors.New("incorrect number of bytes written")

// copyMessage copies reads & writes an entire message.
func CopyMessage(w io.Writer, r io.Reader) error {
	h, err := ReadMsgHeader(r)
	if err != nil {
		return err
	}
	if err := h.WriteTo(w); err != nil {
		return err
	}

	return err
}

func readDocument(r io.Reader, d *Document) error {
	var size int32
	if err := readInt32(r, &size); err != nil {
		return err
	}

	var w bytes.Buffer
	if err := writeInt32(&w, size); err != nil {
		return err
	}

	if _, err := io.CopyN(&w, r, int64(size-4)); err != nil {
		return err
	}

	*d = w.Bytes()
	return nil
}

const x00 = byte(0)

// ReadCString reads a null turminated string as defined by BSON from the
// reader. Note, the return value includes the trailing null byte.
func readCString(r io.Reader, s *CSString) error {
	var b []byte
	var n [1]byte
	for {
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return err
		}

		b = append(b, n[0])
		if n[0] == x00 {
			*s = b
			return nil
		}
	}
}

func readInt32(r io.Reader, i *int32) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func readInt64(r io.Reader, i *int64) error {
	return binary.Read(r, binary.LittleEndian, i)
}

func writeInt32(w io.Writer, i int32) error {
	return binary.Write(w, binary.LittleEndian, i)
}

func writeInt64(w io.Writer, i int64) error {
	return binary.Write(w, binary.LittleEndian, i)
}
