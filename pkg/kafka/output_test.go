package kafka

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/lib/testutils"
	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
	"gopkg.in/guregu/null.v3"
)

func TestRun(t *testing.T) {
	broker := sarama.NewMockBroker(t, 1)
	coordinator := sarama.NewMockBroker(t, 2)
	seedMeta := new(sarama.MetadataResponse)
	seedMeta.AddBroker(coordinator.Addr(), coordinator.BrokerID())
	seedMeta.AddTopicPartition("my_topic", 0, 1, []int32{}, []int32{}, []int32{}, sarama.ErrNoError)
	broker.Returns(seedMeta)

	c, err := New(output.Params{
		Logger:     testutils.NewLogger(t),
		JSONConfig: json.RawMessage(fmt.Sprintf(`{"brokers":[%q], "topic": "my_topic", "version": "0.8.2.0"}`, broker.Addr())),
	})

	require.NoError(t, err)

	require.NoError(t, c.Start())
	require.NoError(t, c.Stop())
}

func TestFormatSample(t *testing.T) {
	o := Output{}
	metric := stats.New("my_metric", stats.Gauge)
	samples := stats.Samples{
		{Metric: metric, Value: 1.25, Tags: stats.IntoSampleTags(&map[string]string{"a": "1"})},
		{Metric: metric, Value: 2, Tags: stats.IntoSampleTags(&map[string]string{"b": "2"})},
	}

	o.Config.Format = null.NewString("influxdb", false)
	formattedSamples, err := o.formatSamples(samples)

	assert.Nil(t, err)
	assert.Equal(t, []string{"my_metric,a=1 value=1.25", "my_metric,b=2 value=2"}, formattedSamples)

	o.Config.Format = null.NewString("json", false)
	formattedSamples, err = o.formatSamples(samples)

	expJSON1 := "{\"type\":\"Point\",\"data\":{\"time\":\"0001-01-01T00:00:00Z\",\"value\":1.25,\"tags\":{\"a\":\"1\"}},\"metric\":\"my_metric\"}"
	expJSON2 := "{\"type\":\"Point\",\"data\":{\"time\":\"0001-01-01T00:00:00Z\",\"value\":2,\"tags\":{\"b\":\"2\"}},\"metric\":\"my_metric\"}"

	assert.Nil(t, err)
	assert.Equal(t, []string{expJSON1, expJSON2}, formattedSamples)
}