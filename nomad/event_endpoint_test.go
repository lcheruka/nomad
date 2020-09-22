package nomad

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-msgpack/codec"
	sframer "github.com/hashicorp/nomad/client/lib/streamframer"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/nomad/mock"
	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/stretchr/testify/require"
)

func TestEventStream(t *testing.T) {
	t.Parallel()

	s1, cleanupS1 := TestServer(t, nil)
	defer cleanupS1()

	req := structs.EventStreamRequest{
		Topics: map[stream.Topic][]string{"*": []string{"*"}},
		QueryOptions: structs.QueryOptions{
			Region: s1.Region(),
		},
	}

	handler, err := s1.StreamingRpcHandler("Event.Stream")
	require.Nil(t, err)

	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()

	errCh := make(chan error)
	streamMsg := make(chan *cstructs.StreamErrWrapper)

	go handler(p2)

	go func() {
		decoder := codec.NewDecoder(p1, structs.MsgpackHandle)
		for {
			var msg cstructs.StreamErrWrapper
			if err := decoder.Decode(&msg); err != nil {
				if err == io.EOF || strings.Contains(err.Error(), "closed") {
					return
				}
				errCh <- fmt.Errorf("error decoding: %w", err)
			}

			streamMsg <- &msg
		}
	}()

	publisher, err := s1.State().EventPublisher()
	require.NoError(t, err)

	node := mock.Node()
	publisher.Publish(uint64(1), []stream.Event{{Topic: "test", Payload: node.ID}})

	// send req
	encoder := codec.NewEncoder(p1, structs.MsgpackHandle)
	require.Nil(t, encoder.Encode(req))

	timeout := time.After(3 * time.Second)

	expected := node.ID
	received := ""
OUTER:
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for event stream")
		case err := <-errCh:
			t.Fatal(err)
		case msg := <-streamMsg:
			if msg.Error != nil {
				t.Fatalf("Got error: %v", msg.Error.Error())
			}

			var frame sframer.StreamFrame
			err := json.Unmarshal(msg.Payload, &frame)

			require.NoError(t, err)
			received += string(frame.Data)
			if strings.Contains(received, expected) {
				require.Nil(t, p2.Close())
				break OUTER
			}
		}
	}
}
