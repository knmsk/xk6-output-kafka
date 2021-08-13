/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2016 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

package kafka

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/riferrei/srclient"
	"github.com/sirupsen/logrus"

	"go.k6.io/k6/output"
	"go.k6.io/k6/stats"
)

// Collector implements the lib.Collector interface and should be used only for testing
type Collector struct {
	Producer             sarama.SyncProducer
	Config               Config
	SchemaRegistryClient srclient.ISchemaRegistryClient

	Samples []stats.Sample
	done    chan struct{}
	logger  logrus.FieldLogger
	lock    sync.Mutex
}

var _ output.Output = new(Collector)

// New creates an instance of the collector
func New(p output.Params) (*Collector, error) {
	conf, err := GetConsolidatedConfig(p.JSONConfig, p.Environment, p.ConfigArgument)
	if err != nil {
		return nil, err
	}
	producer, err := sarama.NewSyncProducer(conf.Brokers, nil)
	if err != nil {
		return nil, err
	}
	srclient := getSchemaRegistryClient(conf.SchemaRegistryConfig)

	return &Collector{
		Producer:             producer,
		Config:               conf,
		SchemaRegistryClient: srclient,
		logger:               p.Logger,
		done:                 make(chan struct{}),
	}, nil
}

func (c *Collector) Description() string {
	return "kafka: TODO"
}

func (c *Collector) Stop() error {
	c.done <- struct{}{}
	<-c.done
	return nil
}

func (c *Collector) Start() error {
	c.logger.Debug("Kafka: starting!")
	go func() {
		ticker := time.NewTicker(time.Duration(c.Config.PushInterval.Duration))
		for {
			select {
			case <-ticker.C:
				c.pushMetrics()
			case <-c.done:
				c.pushMetrics()

				err := c.Producer.Close()
				if err != nil {
					c.logger.WithError(err).Error("Kafka: Failed to close producer.")
				}
				close(c.done)
				return
			}
		}
	}()
	return nil
}

// AddMetricSamples just appends all of the samples passed to it to the internal sample slice.
// According to the the lib.Collector interface, it should never be called concurrently,
// so there's no locking on purpose - that way Go's race condition detector can actually
// detect incorrect usage.
// Also, theoretically the collector doesn't have to actually Run() before samples start
// being collected, it only has to be initialized.
func (c *Collector) AddMetricSamples(scs []stats.SampleContainer) {
	c.lock.Lock()
	for _, sc := range scs {
		c.Samples = append(c.Samples, sc.GetSamples()...)
	}
	c.lock.Unlock()
}

func (c *Collector) formatSamples(samples stats.Samples, currentSchema *srclient.Schema) ([]string, error) {
	var metrics []string

	switch c.Config.Format.String {
	case "avro":
		for _, sample := range samples {
			metric, err := formatAsAvro(c.Config, sample, currentSchema)
			if err != nil {
				return nil, err
			}
			metrics = append(metrics, string(metric))
		}
	case "influxdb":
		var err error
		fieldKinds, err := makeInfluxdbFieldKinds(c.Config.InfluxDBConfig.TagsAsFields)
		if err != nil {
			return nil, err
		}
		metrics, err = formatAsInfluxdbV1(c.logger, samples, newExtractTagsFields(fieldKinds))
		if err != nil {
			return nil, err
		}
	default:
		for _, sample := range samples {
			metric, err := json.Marshal(wrapSample(sample))
			if err != nil {
				return nil, err
			}

			metrics = append(metrics, string(metric))
		}
	}

	return metrics, nil
}

func (c *Collector) pushMetrics() {
	startTime := time.Now()

	c.lock.Lock()
	samples := c.Samples
	c.Samples = nil
	c.lock.Unlock()

	// Get avro schema if the format is avro
	var currentSchema *srclient.Schema
	format := c.Config.Format.String
	if format == "avro" {
		currentSchema = GetSchemaFromRegistryClient(c.Config, c.SchemaRegistryClient, c.logger)
	}

	// Format the samples
	formattedSamples, err := c.formatSamples(samples, currentSchema)
	if err != nil {
		c.logger.WithError(err).Error("Kafka: Couldn't format the samples")
		return
	}

	// Send the samples
	c.logger.Debug("Kafka: Delivering...")

	for _, sample := range formattedSamples {
		var value sarama.Encoder
		switch format {
		case "avro":
			value = sarama.ByteEncoder([]byte(sample))
		default:
			value = sarama.StringEncoder(sample)
		}
		msg := &sarama.ProducerMessage{Topic: c.Config.Topic.String, Value: value}
		partition, offset, err := c.Producer.SendMessage(msg)
		if err != nil {
			c.logger.WithError(err).Error("Kafka: failed to send message.")
		} else {
			c.logger.WithFields(logrus.Fields{
				"partition": partition,
				"offset":    offset,
			}).Debug("Kafka: message sent.")
		}
	}

	t := time.Since(startTime)
	c.logger.WithField("t", t).Debug("Kafka: Delivered!")
}
