package agent

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/hashicorp/nomad/nomad/stream"
	"github.com/hashicorp/nomad/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventStream(t *testing.T) {
	t.Parallel()

	httpTest(t, nil, func(s *TestAgent) {
		req, err := http.NewRequest("GET", "/v1/events", nil)
		require.Nil(t, err)
		resp := newClosableRecorder()
		defer resp.Close()

		go func() {
			_, err = s.Server.EventStream(resp, req)
			assert.NoError(t, err)
		}()

		// send the same log until monitor sink is set up
		maxLogAttempts := 10
		tried := 0
		testutil.WaitForResult(func() (bool, error) {
			if tried < maxLogAttempts {
				pub, err := s.Agent.server.State().EventPublisher()
				require.NoError(t, err)
				pub.Publish(100, []stream.Event{})
				tried++
			}

			got := resp.Body.String()
			want := `"FileEvent":"stream"`
			if strings.Contains(got, want) {
				return true, nil
			}

			return false, fmt.Errorf("missing expected frame, got: %v, want: %v", got, want)
		}, func(err error) {
			require.Fail(t, err.Error())
		})
	})
}

func TestEventStream_QueryParse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		desc    string
		query   string
		want    map[stream.Topic][]string
		wantErr bool
	}{
		{
			desc:  "all topics and keys specified",
			query: "?topic=*:*",
			want: map[stream.Topic][]string{
				"*": []string{"*"},
			},
		},
		{
			desc:  "all topics and keys inferred",
			query: "",
			want: map[stream.Topic][]string{
				"*": []string{"*"},
			},
		},
		{
			desc:    "invalid key value formatting",
			query:   "?topic=NodeDrain:*:*",
			wantErr: true,
		},
		{
			desc:    "invalid key value formatting no value",
			query:   "?topic=NodeDrain",
			wantErr: true,
		},
		{
			desc:  "single topic and key",
			query: "?topic=NodeDrain:*",
			want: map[stream.Topic][]string{
				"NodeDrain": []string{"*"},
			},
		},
		{
			desc:  "single topic multiple keys",
			query: "?topic=NodeDrain:*&topic=NodeDrain:3caace09-f1f4-4d23-b37a-9ab5eb75069d",
			want: map[stream.Topic][]string{
				"NodeDrain": []string{
					"*",
					"3caace09-f1f4-4d23-b37a-9ab5eb75069d",
				},
			},
		},
		{
			desc:  "multiple topics",
			query: "?topic=NodeRegister:*&topic=NodeDrain:3caace09-f1f4-4d23-b37a-9ab5eb75069d",
			want: map[stream.Topic][]string{
				"NodeDrain": []string{
					"3caace09-f1f4-4d23-b37a-9ab5eb75069d",
				},
				"NodeRegister": []string{
					"*",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			raw := fmt.Sprintf("http://localhost:80/v1/events%s", tc.query)
			req, err := url.Parse(raw)
			require.NoError(t, err)

			got, err := parseEventTopics(req.Query())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
