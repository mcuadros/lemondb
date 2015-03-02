package protocol

import (
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

// toWire converts the MsgHeader to the wire protocol
func (op *OpQuery) toWire(w io.Writer) error {
	if err := writeInt32(w, op.Flags); err != nil {
		return err
	}

	if _, err := w.Write(op.FullCollectionName); err != nil {
		return err
	}

	if err := writeInt32(w, op.NumberToSkip); err != nil {
		return err
	}

	if err := writeInt32(w, op.NumberToReturn); err != nil {
		return err
	}

	if _, err := w.Write(op.Query); err != nil {
		return err
	}

	if _, err := w.Write(op.ReturnFieldsSelector); err != nil {
		return err
	}

	return nil
}

func (op *OpQuery) GetOpCode() OpCode {
	return OpQueryCode
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
