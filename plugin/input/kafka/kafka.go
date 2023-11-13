package kafka

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.

**Example**
Standard example:
```yaml
pipelines:
  example_pipeline:
    input:
      type: kafka
      brokers: [kafka:9092, kafka:9091]
      topics: [topic1, topic2]
      offset: newest
    # output plugin is not important in this case, let's emulate s3 output.
    output:
      type: s3
      file_config:
        retention_interval: 10s
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
```
}*/

type Plugin struct {
	config        *Config
	logger        *zap.SugaredLogger
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	controller    pipeline.InputPluginController
	idByTopic     map[string]int
	sessionMu     *sync.RWMutex

	offsetsInfo                *offsetsInfo
	topicsPartitionsOffsetsBuf map[topicIdxPartition][]int64
	topicsPartitionsMinOffsets topicPartitionOffsetsMap

	// plugin metrics

	commitErrorsMetric  *prometheus.CounterVec
	consumeErrorsMetric *prometheus.CounterVec
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The name of kafka brokers to read from.
	Brokers []string `json:"brokers" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The list of kafka topics to read from.
	Topics []string `json:"topics" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The name of consumer group to use.
	ConsumerGroup string `json:"consumer_group" default:"file-d"` // *

	// > @3@4@5@6
	// >
	// > The number of unprocessed messages in the buffer that are loaded in the background from kafka.
	ChannelBufferSize int `json:"channel_buffer_size" default:"256"` // *

	// > @3@4@5@6
	// >
	// > The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
	// > * *`newest`* - set offset to the newest message
	// > * *`oldest`* - set offset to the oldest message
	Offset string `json:"offset" default:"newest" options:"oldest|newest"` // *

	// > @3@4@5@6
	// >
	// > The maximum amount of time the consumer expects a message takes to process for the user.
	ConsumerMaxProcessingTime  cfg.Duration `json:"consumer_max_processing_time" default:"200ms" parse:"duration"` // *
	ConsumerMaxProcessingTime_ time.Duration

	// > @3@4@5@6
	// >
	// > The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways.
	ConsumerMaxWaitTime  cfg.Duration `json:"consumer_max_wait_time" default:"250ms" parse:"duration"` // *
	ConsumerMaxWaitTime_ time.Duration

	// > @3@4@5@6
	// >
	// > If value is set to greater than 0 plugin marks and commits offsets manually periodically by the given interval.
	// > Otherwise enables autocommit. Default 0s no manual commit.
	ManualOffsetsCommitInterval  cfg.Duration `json:"manual_offsets_commit_interval" default:"0s" parse:"duration"` // *
	ManualOffsetsCommitInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > Only used if manual offsets is used.
	// > If value is set to greater than 0 retries until the duration is passed. If set to 0 retries never stop.
	ManualCommitMaxRetriesTime  cfg.Duration `json:"manual_commit_max_retries_time" default:"2ms" parse:"duration"` // *
	ManualCommitMaxRetriesTime_ time.Duration

	// > @3@4@5@6
	// >
	// > Only used if manual offsets is used.
	// > Minimum backoff value for manual commits retries.
	ManualCommitMinBackoff  cfg.Duration `json:"manual_commit_min_backoff" default:"50ms" parse:"duration"` // *
	ManualCommitMinBackoff_ time.Duration

	// > @3@4@5@6
	// >
	// > Only used if manual offsets is used.
	// > Maximum backoff value for manual commits retries.
	ManualCommitMaxBackoff  cfg.Duration `json:"cmanual_ommit_max_backoff" default:"1s" parse:"duration"` // *
	ManualCommitMaxBackoff_ time.Duration

	// > @3@4@5@6
	// >
	// > If value is set to greater than 0 tries to mark offsets every `mark_offsets_rate` finished jobs.
	// > Manual commit mode has priority over this mode. So if both values are set to greater than 0
	// > only manual commit is used and this parameter is ignored. Default 0 usual mark offset.
	MarkOffsetsRate int `json:"mark_offsets_rate" default:"0"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "kafka",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)

	p.idByTopic = make(map[string]int, len(p.config.Topics))
	for i, topic := range p.config.Topics {
		p.idByTopic[topic] = i
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.consumerGroup = p.newConsumerGroup()
	p.controller.UseSpread()
	p.controller.DisableStreams()
	p.sessionMu = &sync.RWMutex{}

	p.topicsPartitionsOffsetsBuf = make(map[topicIdxPartition][]int64)
	p.topicsPartitionsMinOffsets = make(topicPartitionOffsetsMap)

	go p.consume(ctx)
	if p.config.ManualOffsetsCommitInterval_ > 0 {
		p.logger.Infof("starting manual offsets committing to kafka for topics: %s", strings.Join(p.config.Topics, ","))
		if p.config.MarkOffsetsRate > 0 {
			p.logger.Warn("since manual offsets committing is on offsets update rate is ignored")
		}
		p.offsetsInfo = newOffsetsInfo(offsetsInfoOpts{})
		go p.offsetsInfo.commit(ctx,
			offsetsCommitParams{
				p.getSession,
				p.sessionMu,
				p.config.ManualOffsetsCommitInterval_,
				p.config.ManualCommitMaxRetriesTime_,
				p.config.ManualCommitMinBackoff_,
				p.config.ManualCommitMaxBackoff_,
				p.logger,
			})
	} else if p.config.MarkOffsetsRate > 0 {
		p.logger.Infof("starting marking offsets every %d finished jobs for topics: %s",
			p.config.MarkOffsetsRate,
			strings.Join(p.config.Topics, ","),
		)
		p.offsetsInfo = newOffsetsInfo(offsetsInfoOpts{
			p.config.MarkOffsetsRate,
		})
	}

	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) getSession() sarama.ConsumerGroupSession {
	return p.session
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.commitErrorsMetric = ctl.RegisterCounter("input_kafka_commit_errors", "Number of kafka commit errors")
	p.consumeErrorsMetric = ctl.RegisterCounter("input_kafka_consume_errors", "Number of kafka consume errors")
	p.offsetsInfo.registerMetrics(ctl)
}

func (p *Plugin) consume(ctx context.Context) {
	p.logger.Infof("kafka input reading from topics: %s", strings.Join(p.config.Topics, ","))
	for {
		err := p.consumerGroup.Consume(ctx, p.config.Topics, p)
		if err != nil {
			p.consumeErrorsMetric.WithLabelValues().Inc()
			p.logger.Errorf("can't consume from kafka: %s", err.Error())
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func (p *Plugin) Stop() {
	p.cancel()
}

func (p *Plugin) Commit(events ...*pipeline.Event) {
	if len(events) == 0 {
		return
	}
	switch {
	case p.config.ManualOffsetsCommitInterval_ > 0:
		p.commitWithCommitInterval(events)
	case p.config.MarkOffsetsRate > 0:
		p.commitWithOffsetRate(events)
	default:
		p.commitUsual(events)
	}
}

func (p *Plugin) commitUsual(events []*pipeline.Event) {
	if p.session == nil {
		p.commitErrorsMetric.WithLabelValues().Inc()
		p.logger.Errorf("no kafka consumer session for event commit")
		return
	}
	offsets := p.topicsPartitionsOffsetsBuf
	for _, event := range events {
		if event.IsTimeoutKind() {
			continue
		}
		index, partition := disassembleSourceID(event.SourceID)
		key := topicIdxPartition{index, partition}
		offsets[key] = append(offsets[key], event.Offset)
	}
	for key, vals := range offsets {
		if len(vals) == 0 {
			continue
		}
		minOffset := vals[0]
		for i := 1; i < len(vals); i++ {
			if vals[i] < minOffset {
				minOffset = vals[i]
			}
		}
		p.session.MarkOffset(p.config.Topics[key.topicIdx], key.partition, minOffset, "")
		offsets[key] = offsets[key][:0]
	}
}

func (p *Plugin) commitWithCommitInterval(events []*pipeline.Event) {
	offsets := p.topicsPartitionsOffsetsBuf
	minOffsets := p.topicsPartitionsMinOffsets
	for _, event := range events {
		if event.IsTimeoutKind() {
			continue
		}
		index, partition := disassembleSourceID(event.SourceID)
		key := topicIdxPartition{index, partition}
		offsets[key] = append(offsets[key], event.Offset)
	}
	for k, vals := range offsets {
		if len(vals) == 0 {
			continue
		}
		minOffset := vals[0]
		for i := 1; i < len(vals); i++ {
			if vals[i] < minOffset {
				minOffset = vals[i]
			}
		}
		key := topicPartition{p.config.Topics[k.topicIdx], k.partition}
		minOffsets[key] = minOffset
		offsets[k] = offsets[k][:0]
	}
	if len(minOffsets) == 0 {
		return
	}
	p.offsetsInfo.updateOffsets(minOffsets)
	for k := range minOffsets {
		delete(minOffsets, k)
	}
}

func (p *Plugin) commitWithOffsetRate(events []*pipeline.Event) {
	session := p.session
	if session == nil {
		p.commitErrorsMetric.WithLabelValues().Inc()
		p.logger.Errorf("no kafka consumer session for event commit")
		return
	}
	offsets := p.topicsPartitionsOffsetsBuf
	minOffsets := p.topicsPartitionsMinOffsets
	for _, event := range events {
		if event.IsTimeoutKind() {
			continue
		}
		index, partition := disassembleSourceID(event.SourceID)
		key := topicIdxPartition{index, partition}
		offsets[key] = append(offsets[key], event.Offset)
	}
	for k, vals := range offsets {
		if len(vals) == 0 {
			continue
		}
		minOffset := vals[0]
		for i := 1; i < len(vals); i++ {
			if vals[i] < minOffset {
				minOffset = vals[i]
			}
		}
		key := topicPartition{p.config.Topics[k.topicIdx], k.partition}
		minOffsets[key] = minOffset
		offsets[k] = offsets[k][:0]
	}
	if len(minOffsets) == 0 {
		return
	}
	p.offsetsInfo.updateOffsets2(minOffsets, session)
	for k := range minOffsets {
		delete(minOffsets, k)
	}
}

func (p *Plugin) newConsumerGroup() sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.BalanceStrategyRoundRobin}
	config.Version = sarama.V0_10_2_0
	config.ChannelBufferSize = p.config.ChannelBufferSize
	config.Consumer.MaxWaitTime = p.config.ConsumerMaxWaitTime_
	config.Consumer.MaxProcessingTime = p.config.ConsumerMaxProcessingTime_
	if p.config.ManualOffsetsCommitInterval_ > 0 {
		p.logger.Infof("manual_offsets_commit_interval is set to %v, disabling autocommit",
			p.config.ManualOffsetsCommitInterval_,
		)
		config.Consumer.Offsets.AutoCommit.Enable = false
	} else {
		p.logger.Infof("autocommit with interval %v", config.Consumer.Offsets.AutoCommit.Interval)
	}

	switch p.config.Offset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		p.logger.Fatalf("unexpected value of the offset field: %s", p.config.Offset)
	}

	consumerGroup, err := sarama.NewConsumerGroup(p.config.Brokers, p.config.ConsumerGroup, config)
	if err != nil {
		p.logger.Fatalf("can't create kafka consumer: %s", err.Error())
	}

	return consumerGroup
}

func (p *Plugin) Setup(session sarama.ConsumerGroupSession) error {
	p.logger.Infof("kafka consumer created with brokers %q", strings.Join(p.config.Brokers, ","))
	p.sessionMu.Lock()
	p.session = session
	p.sessionMu.Unlock()
	return nil
}

func (p *Plugin) Cleanup(sarama.ConsumerGroupSession) error {
	p.sessionMu.Lock()
	p.session = nil
	p.sessionMu.Unlock()
	return nil
}

func (p *Plugin) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		sourceID := assembleSourceID(p.idByTopic[message.Topic], message.Partition)
		_ = p.controller.In(sourceID, "kafka", message.Offset, message.Value, true)
	}

	return nil
}

func assembleSourceID(index int, partition int32) pipeline.SourceID {
	return pipeline.SourceID(index<<16 + int(partition))
}

func disassembleSourceID(sourceID pipeline.SourceID) (index int, partition int32) {
	index = int(sourceID >> 16)
	partition = int32(sourceID & 0xFFFF)

	return
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}
