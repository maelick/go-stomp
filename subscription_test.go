package stomp

import (
	"github.com/go-stomp/stomp/v3/frame"
	"github.com/go-stomp/stomp/v3/testutil"
	"time"

	. "gopkg.in/check.v1"
)

func (s *StompSuite) Test_successful_unsubscribe_with_receipt_timeout(c *C) {
	resetId()
	fc1, fc2 := testutil.NewFakeConn(c)

	defer func() {
		fc2.Close()
	}()

	go func() {
		reader := frame.NewReader(fc2)
		writer := frame.NewWriter(fc2)

		f1, err := reader.Read()
		c.Assert(err, IsNil)
		c.Assert(f1.Command, Equals, "CONNECT")
		connectedFrame := frame.New("CONNECTED")
		err = writer.Write(connectedFrame)
		c.Assert(err, IsNil)
	}()

	client, err := Connect(fc1, ConnOpt.UnsubscribeTimeout(1*time.Nanosecond))
	c.Assert(err, IsNil)
	c.Assert(client, NotNil)

	sub, err := client.Subscribe("/queue/test", AckAuto)
	c.Assert(err, IsNil)
	c.Assert(sub, NotNil)

	err = sub.Unsubscribe()
	c.Assert(err, Equals, &ErrUnsubscribeTimeout)
}
