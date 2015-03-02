package proxy

import (
	"io"

	"github.com/mcuadros/exmongodb/protocol"
)

type Middleware interface {
	Handle(m protocol.Message, c io.ReadWriter, s io.ReadWriter) error
}
