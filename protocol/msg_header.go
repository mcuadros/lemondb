package protocol

import (
	"fmt"
	"io"
)

const HeaderLen = 16

// MsgHeader is the mongo MsgHeader
type MsgHeader struct {
	// MessageLength is the total message size, including this header
	MessageLength int32
	// RequestID is the identifier for this miessage
	RequestID int32
	// ResponseTo is the RequestID of the message being responded to. used in DB responses
	ResponseTo int32
	// OpCode is the request type, see consts above.
	OpCode OpCode
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
	return m, nil
}

func (m *MsgHeader) WriteTo(w io.Writer) error {
	if err := m.toWire(w); err != nil {
		return err
	}

	return nil
}

// ToWire converts the MsgHeader to the wire protocol
func (m MsgHeader) toWire(w io.Writer) error {
	if err := writeInt32(w, m.MessageLength); err != nil {
		return err
	}
	if err := writeInt32(w, m.RequestID); err != nil {
		return err
	}
	if err := writeInt32(w, m.ResponseTo); err != nil {
		return err
	}
	if err := writeInt32(w, int32(m.OpCode)); err != nil {
		return err
	}

	return nil
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
