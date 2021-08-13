package kafka

import (
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"
	"go.k6.io/k6/lib/types"
)

func getSchemaRegistryClient(cfg SchemaRegistryConfig) *srclient.SchemaRegistryClient {
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(cfg.Url)
	schemaRegistryClient.SetCredentials(cfg.Api.Key, cfg.Api.Secret)
	if cfg.TimeoutInSeconds >= 0 {
		schemaRegistryClient.SetTimeout(time.Duration(cfg.TimeoutInSeconds) * time.Second)
	}
	return schemaRegistryClient
}

func GetSchemaFromRegistryClient(config Config, schemaRegistryClient srclient.ISchemaRegistryClient, logger logrus.FieldLogger) *srclient.Schema {
	isKey := config.SchemaRegistryConfig.IsAvroAKey

	// Send the avro schema
	schema, err := LoadAvroSchema("schema.avsc")
	if err != nil {
		logger.WithError(err).Error("Kafka: Couldn't get .avsc file")
		return nil
	}

	logger.Debug("Kafka: Sending Avro Schema...")

	currentSchema, err := schemaRegistryClient.GetLatestSchema(config.Topic.String, isKey)
	if err != nil {
		logger.WithError(err).Error("Kafka: Couldn't get current avro schema")
		return nil
	}
	if currentSchema == nil {
		currentSchema, err = schemaRegistryClient.CreateSchema(config.Topic.String, schema, srclient.Avro, isKey)
		if err != nil {
			logger.WithError(err).Error("Kafka: Error creating the schema")
			return nil
		}
	}

	return currentSchema
}

func (c SchemaRegistryConfig) Apply(cfg SchemaRegistryConfig) SchemaRegistryConfig {
	c.Url = cfg.Url
	c.Api = cfg.Api
	c.TimeoutInSeconds = cfg.TimeoutInSeconds

	return c
}

func schemaRegistryParseMap(m map[string]interface{}) (SchemaRegistryConfig, error) {
	c := SchemaRegistryConfig{}

	if v, ok := m["url"].(string); ok {
		m["url"] = v
	}
	if v, ok := m["api"].(map[string]interface{}); ok {
		m["api"] = v
	}
	if v, ok := m["timeoutInSeconds"].(int64); ok {
		m["timeoutInSeconds"] = v
	}

	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: types.NullDecoder,
		Result:     &c,
	})
	if err != nil {
		return c, err
	}

	err = dec.Decode(m)
	return c, err
}

type SchemaRegistryApiConfig struct {
	Key    string `json:"key,omitempty" envconfig:"K6_KAFKA_SCHEMA_REGISTRY_API_KEY"`
	Secret string `json:"secret,omitempty" envconfig:"K6_KAFKA_SCHEMA_REGISTRY_API_SECRET"`
}

type SchemaRegistryConfig struct {
	Url              string                  `json:"url,omitempty" envconfig:"K6_KAFKA_SCHEMA_REGISTRY_URL"`
	Api              SchemaRegistryApiConfig `json:"apiKey,omitempty"`
	TimeoutInSeconds int64                   `json:"timeoutInSeconds,omitempty" envconfig:"K6_KAFKA_SCHEMA_REGISTRY_TIMEOUT_IN_SECONDS"`
	IsAvroAKey       bool                    `json:"isAvroAKey" envconfig:"K6_KAFKA_SCHEMA_REGISTRY_IS_AVRO_A_KEY"`
}

func newSchemaRegistryConfig() SchemaRegistryConfig {
	c := SchemaRegistryConfig{
		Url: "",
		Api: SchemaRegistryApiConfig{
			Key:    "",
			Secret: "",
		},
		TimeoutInSeconds: 5,
	}

	return c
}
