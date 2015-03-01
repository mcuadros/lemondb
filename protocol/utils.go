package protocol

import (
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

	_, err = io.CopyN(w, r, int64(h.MessageLength-HeaderLen))
	return err
}

// readDocument read an entire BSON document. This document can be used with
// bson.Unmarshal.
func readDocument(r io.Reader, d *Document) error {
	var sizeRaw [4]byte
	if _, err := io.ReadFull(r, sizeRaw[:]); err != nil {
		return err
	}
	size := getInt32(sizeRaw[:], 0)
	doc := make([]byte, size)
	SetInt32(doc, 0, size)
	if _, err := io.ReadFull(r, doc[4:]); err != nil {
		return err
	}

	*d = doc
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

// all data in the MongoDB wire protocol is little-endian.
// all the read/write functions below are little-endian.
func readInt32(r io.Reader, i *int32) error {
	b := make([]byte, 4)
	if _, err := r.Read(b); err != nil {
		return err
	}

	*i = (int32(b[0])) |
		(int32(b[1]) << 8) |
		(int32(b[2]) << 16) |
		(int32(b[3]) << 24)

	return nil
}

func writeInt32(w io.Writer, i int32) error {
	b := [4]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	if i, err := w.Write(b[:]); err != nil {
		return err
	} else if i != 4 {
		return errWrite
	}

	return nil
}

// all data in the MongoDB wire protocol is little-endian.
// all the read/write functions below are little-endian.
func getInt32(b []byte, pos int) int32 {
	return (int32(b[pos+0])) |
		(int32(b[pos+1]) << 8) |
		(int32(b[pos+2]) << 16) |
		(int32(b[pos+3]) << 24)
}

func SetInt32(b []byte, pos int, i int32) {
	b[pos] = byte(i)
	b[pos+1] = byte(i >> 8)
	b[pos+2] = byte(i >> 16)
	b[pos+3] = byte(i >> 24)
}
