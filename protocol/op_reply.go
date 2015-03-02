package protocol

import (
	"io"
)

// MsgHeader is the mongo MsgHeader
type OpReply struct {
	// MsgHeader standard message header
	MsgHeader *MsgHeader
	// ResponseFlags bit vector.
	ResponseFlags int32
	// CursorID cursor id if client needs to do get more's
	CursorID int64
	// StartingFrom where in the cursor this reply is starting
	StartingFrom int32
	// NumberReturned number of documents in the reply
	NumberReturned int32
	// Documents
	Documents []Document
}

func ReadOpReply(h *MsgHeader, r io.Reader) (*OpReply, error) {
	var err error
	op := &OpReply{MsgHeader: h}
	if err = readInt32(r, &op.ResponseFlags); err != nil {
		return nil, err
	}

	if err = readInt64(r, &op.CursorID); err != nil {
		return nil, err
	}

	if err = readInt32(r, &op.StartingFrom); err != nil {
		return nil, err
	}

	if err = readInt32(r, &op.NumberReturned); err != nil {
		return nil, err
	}

	op.Documents = make([]Document, op.NumberReturned)
	for i := 0; i < int(op.NumberReturned); i++ {
		var doc Document
		if err = readDocument(r, &doc); err != nil && err != io.EOF {
			return nil, err
		}

		op.Documents[i] = doc
	}

	return op, nil
}

func (op *OpReply) WriteTo(w io.Writer) error {
	if err := op.MsgHeader.toWire(w); err != nil {
		return err
	}

	if err := op.toWire(w); err != nil {
		return err
	}

	return nil
}

// toWire converts the MsgHeader to the wire protocol
func (op *OpReply) toWire(w io.Writer) error {
	if err := writeInt32(w, op.ResponseFlags); err != nil {
		return err
	}

	if err := writeInt64(w, op.CursorID); err != nil {
		return err
	}

	if err := writeInt32(w, op.StartingFrom); err != nil {
		return err
	}

	if err := writeInt32(w, op.NumberReturned); err != nil {
		return err
	}

	for _, doc := range op.Documents {
		if _, err := w.Write(doc); err != nil {
			return err
		}
	}

	return nil
}

func (op *OpReply) GetOpCode() OpCode {
	return OpReplyCode
}

// String returns a string representation of the message header.
func (op *OpReply) String() string {
	return ""
}
