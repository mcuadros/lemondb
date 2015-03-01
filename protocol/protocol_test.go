package protocol

import (
	. "gopkg.in/check.v1"
)

func (s *ProtocolSuite) TestOpCode_Strings(c *C) {
	cases := []struct {
		OpCode OpCode
		String string
	}{
		{OpCode(0), "UNKNOWN"},
		{OpReplyCode, "REPLY"},
		{OpMessageCode, "MESSAGE"},
		{OpUpdateCode, "UPDATE"},
		{OpInsertCode, "INSERT"},
		{Reserved, "RESERVED"},
		{OpQueryCode, "QUERY"},
		{OpGetMoreCode, "GET_MORE"},
		{OpDeleteCode, "DELETE"},
		{OpKillCursorsCode, "KILL_CURSORS"},
	}
	for _, cs := range cases {
		c.Assert(cs.OpCode.String(), Equals, cs.String)
	}
}
