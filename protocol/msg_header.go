package protocol

import (
	"bytes"
	"fmt"
	"io"
)

const HeaderLen = 16

// MsgHeader is the mongo MsgHeader
type MsgHeader struct {
	// MessageLength is the total message size, including this header
	MessageLength int32
	// RequestID is the identifier for this message
	RequestID int32
	// ResponseTo is the RequestID of the message being responded to. used in DB responses
	ResponseTo int32
	// OpCode is the request type, see consts above.
	OpCode OpCode
	// Message raw content
	Message []byte
}

func ReadMsgHeader(r io.Reader) (*MsgHeader, error) {
	var err error
	m := &MsgHeader{}

	if err = readInt32(r, &m.MessageLength); err != nil {
		return nil, err
	}

	if err = readInt32(r, &m.RequestID); err != nil {
		return nil, err
	}

	if err = readInt32(r, &m.ResponseTo); err != nil {
		return nil, err
	}

	var op int32
	if err = readInt32(r, &op); err != nil {
		return nil, err
	}

	m.OpCode = OpCode(op)

	l := m.MessageLength - HeaderLen
	if l > 0 {
		b := make([]byte, l)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, err
		}

		m.Message = b
	}

	return m, nil
}

func (m *MsgHeader) WriteTo(w io.Writer) error {
	if _, err := w.Write(m.toWire()); err != nil {
		return err
	}

	if _, err := w.Write(m.Message); err != nil {
		return err
	}

	return nil
}

// ToWire converts the MsgHeader to the wire protocol
func (m MsgHeader) toWire() []byte {
	w := bytes.NewBuffer([]byte{})

	writeInt32(w, m.MessageLength)
	writeInt32(w, m.RequestID)
	writeInt32(w, m.ResponseTo)
	writeInt32(w, int32(m.OpCode))

	return w.Bytes()
}

func (m *MsgHeader) GetOpCode() OpCode {
	return m.OpCode
}

func (m *MsgHeader) GetMsgHeader() *MsgHeader {
	return m
}

// String returns a string representation of the message header.
func (m *MsgHeader) String() string {
	return fmt.Sprintf(
		"opCode:%s (%d) msgLen:%d reqID:%d respID:%d",
		m.OpCode,
		m.OpCode,
		m.MessageLength,
		m.RequestID,
		m.ResponseTo,
	)
}
