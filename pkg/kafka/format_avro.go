package kafka

import (
	"encoding/binary"
	"io/ioutil"

	"github.com/riferrei/srclient"
	"go.k6.io/k6/stats"
)

func formatAsAvro(config Config, sample stats.Sample, currentSchema *srclient.Schema) ([]byte, error) {
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(currentSchema.ID()))

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)

	native, err := ToAvro(sample)
	if err != nil {
		return nil, err
	}

	record, err := currentSchema.Codec().BinaryFromNative(recordValue, native)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func LoadAvroSchema(schemaPath string) (string, error) {
	rawSchema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return "", err
	}
	return string(rawSchema), nil
}

func ToAvro(sample stats.Sample) (map[string]interface{}, error) {
	time, err := sample.Time.MarshalText()
	if err != nil {
		return nil, err
	}
	tags := make(map[string]interface{})
	for k, v := range sample.Tags.CloneTags() {
		tags[k] = v
	}

	avroFormat := map[string]interface{}{
		"type":   "Point",
		"metric": sample.Metric.Name,
		"data": map[string]interface{}{
			"time":  time,
			"value": sample.Value,
			"tags":  tags,
		},
	}

	return avroFormat, nil
}
