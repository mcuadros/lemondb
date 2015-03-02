package protocol

import (
	"bytes"
	"fmt"
	"io"
)

// MsgHeader is the mongo MsgHeader
type OpQuery struct {
	// MsgHeader standard message header
	MsgHeader *MsgHeader
	// Flags bit vector of query options.
	Flags int32
	// FullCollectionName "dbname.collectionname"
	FullCollectionName CSString
	// number of documents to skip
	NumberToSkip int32
	// number of documents to return
	NumberToReturn int32
	//  in the first OP_REPLY batch
	Query Document
	// Optional. Selector indicating the fields
	ReturnFieldsSelector Document
}

func ReadOpQuery(h *MsgHeader, r io.Reader) (*OpQuery, error) {
	var err error
	op := &OpQuery{MsgHeader: h}
	if err = readInt32(r, &op.Flags); err != nil {
		return nil, err
	}

	if err = readCString(r, &op.FullCollectionName); err != nil {
		return nil, err
	}

	if err = readInt32(r, &op.NumberToSkip); err != nil {
		return nil, err
	}

	if err = readInt32(r, &op.NumberToReturn); err != nil {
		return nil, err
	}

	if err = readDocument(r, &op.Query); err != nil && err != io.EOF {
		return nil, err
	}

	if err = readDocument(r, &op.ReturnFieldsSelector); err != nil && err != io.EOF {
		return nil, err
	}

	return op, nil
}

func (op *OpQuery) WriteTo(w io.Writer) error {
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
func (op *OpQuery) toWire() []byte {
	w := bytes.NewBuffer([]byte{})

	writeInt32(w, op.Flags)
	w.Write(op.FullCollectionName)
	writeInt32(w, op.NumberToSkip)
	writeInt32(w, op.NumberToReturn)
	w.Write(op.Query)
	w.Write(op.ReturnFieldsSelector)

	return w.Bytes()
}

func (op *OpQuery) GetOpCode() OpCode {
	return OpQueryCode
}

func (op *OpQuery) GetMsgHeader() *MsgHeader {
	return op.MsgHeader
}

// String returns a string representation of the message header.
func (op *OpQuery) String() string {
	return fmt.Sprintf(
		"opQuery - collection: %s q: %s p: %s skip:%d limit:%d",
		op.FullCollectionName.String(),
		op.Query.String(),
		op.ReturnFieldsSelector.String(),
		op.NumberToSkip,
		op.NumberToReturn,
	)
}
