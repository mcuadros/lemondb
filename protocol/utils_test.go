package protocol

import (
	"bytes"
	"errors"
	"io"

	. "gopkg.in/check.v1"
)

func (s *ProtocolSuite) TestCopyMessageWithEmptyMessage(c *C) {
	msg := MsgHeader{}
	b := bytes.NewBuffer(msg.toWire())

	var w bytes.Buffer
	err := CopyMessage(&w, b)
	c.Assert(err, IsNil)
	c.Assert(w.Bytes(), HasLen, 16)
}

func (s *ProtocolSuite) TestCopyMessageFromReadError(c *C) {
	expectedErr := errors.New("foo")
	r := testReader{
		read: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}

	var w bytes.Buffer
	err := CopyMessage(&w, r)
	c.Assert(err, Equals, expectedErr)
}

func (s *ProtocolSuite) TestCopyMessageFromWriteError(c *C) {
	msg := MsgHeader{}
	r := bytes.NewBuffer(msg.toWire())

	expectedErr := errors.New("foo")
	w := testWriter{
		write: func(b []byte) (int, error) {
			return 0, expectedErr
		},
	}

	err := CopyMessage(w, r)
	c.Assert(err, Equals, expectedErr)

}

func (s *ProtocolSuite) TestReadDocumentEmpty(c *C) {
	var doc Document
	err := readDocument(bytes.NewReader([]byte{}), &doc)
	c.Assert(err, Equals, io.EOF)
	c.Assert(doc, HasLen, 0)
}

func (s *ProtocolSuite) TestReadDocumentPartial(c *C) {
	first := true
	r := testReader{
		read: func(b []byte) (int, error) {
			if first {
				first = false
				i := 5
				b[0] = byte(i)
				b[1] = byte(i >> 8)
				b[2] = byte(i >> 16)
				b[3] = byte(i >> 24)

				return 4, nil
			}
			return 0, io.EOF
		},
	}

	var doc Document
	err := readDocument(r, &doc)
	c.Assert(err, Equals, io.EOF)
	c.Assert(doc, HasLen, 0)
}

func (s *ProtocolSuite) TestReadCString(c *C) {
	cases := []struct {
		Data     []byte
		Expected CSString
		Error    error
	}{
		{nil, nil, io.EOF},
		{[]byte{0}, CSString{0}, nil},
		{[]byte{1, 2, 3, 0}, CSString{1, 2, 3, 0}, nil},
		{[]byte{1, 0, 3}, CSString{1, 0}, nil},
	}

	for _, cs := range cases {
		var cstring CSString
		err := readCString(bytes.NewReader(cs.Data), &cstring)
		c.Assert(err, Equals, cs.Error)
		c.Assert(cstring, DeepEquals, cs.Expected)
	}
}

type testReader struct {
	read func([]byte) (int, error)
}

func (t testReader) Read(b []byte) (int, error) { return t.read(b) }

type testWriter struct {
	write func([]byte) (int, error)
}

func (t testWriter) Write(b []byte) (int, error) { return t.write(b) }
