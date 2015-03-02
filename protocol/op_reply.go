package protocol

import (
	"bytes"
	"gopkg.in/mgo.v2/bson"
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

func NewOpReplay(req Message, requestID int32) *OpReply {
	reqh := req.GetMsgHeader()
	h := &MsgHeader{
		RequestID:  requestID,
		ResponseTo: reqh.RequestID,
		OpCode:     OpReplyCode,
	}

	return &OpReply{
		MsgHeader: h,
		Documents: make([]Document, 0),
	}
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

func (op *OpReply) AddDocument(d interface{}) error {
	op.NumberReturned++
	blob, err := bson.Marshal(d)
	if err != nil {
		return err
	}

	op.Documents = append(op.Documents, Document(blob))
	return nil
}

func (op *OpReply) WriteTo(w io.Writer) error {
	content := op.toWire()
	op.MsgHeader.MessageLength = int32(len(content)) + HeaderLen

	if _, err := w.Write(op.MsgHeader.toWire()); err != nil {
		return err
	}

	if _, err := w.Write(content); err != nil {
		return err
	}

	return nil
}

// toWire converts the MsgHeader to the wire protocol
func (op *OpReply) toWire() []byte {
	w := bytes.NewBuffer([]byte{})
	writeInt32(w, op.ResponseFlags)
	writeInt64(w, op.CursorID)
	writeInt32(w, op.StartingFrom)
	writeInt32(w, op.NumberReturned)

	for _, doc := range op.Documents {
		w.Write(doc)
	}

	return w.Bytes()
}

func (op *OpReply) GetOpCode() OpCode {
	return OpReplyCode
}

// String returns a string representation of the message header.
func (op *OpReply) String() string {
	return ""
}
