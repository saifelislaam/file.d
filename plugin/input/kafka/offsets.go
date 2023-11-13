package kafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type sessionGetter func() sarama.ConsumerGroupSession

type topicIdxPartition struct {
	topicIdx  int
	partition int32
}

type topicPartition struct {
	topic     string
	partition int32
}

type topicPartitionOffsetsMap map[topicPartition]int64

type offsetsInfo struct {
	curOffsets  topicPartitionOffsetsMap
	prevOffsets topicPartitionOffsetsMap
	mu          *sync.RWMutex
	updatesCnt  int
	updateRate  int

	commitIsRunning bool
	commitOffsets   topicPartitionOffsetsMap

	curOffsetLessThanPrevMetric *prometheus.CounterVec
}

type offsetsInfoOpts struct {
	updateRate int
}

func newOffsetsInfo(opts offsetsInfoOpts) *offsetsInfo {
	return &offsetsInfo{
		curOffsets:    make(topicPartitionOffsetsMap),
		prevOffsets:   make(topicPartitionOffsetsMap),
		commitOffsets: make(topicPartitionOffsetsMap),
		mu:            &sync.RWMutex{},
		updateRate:    opts.updateRate,
	}
}

func (o *offsetsInfo) registerMetrics(ctl *metric.Ctl) {
	if o == nil {
		return
	}
	o.curOffsetLessThanPrevMetric = ctl.RegisterCounter("input_kafka_cur_offset_less_than_prev_total", "Number of times when current offset was less than previous")
}

func (o *offsetsInfo) updateOffsets2(newOffsets topicPartitionOffsetsMap, session sarama.ConsumerGroupSession) {
	for key, newOffset := range newOffsets {
		if curOffset, ok := o.curOffsets[key]; !ok || newOffset < curOffset {
			o.curOffsets[key] = newOffset
			if prevOffset, ok := o.prevOffsets[key]; ok && prevOffset > newOffset && o.curOffsetLessThanPrevMetric != nil {
				o.curOffsetLessThanPrevMetric.WithLabelValues().Inc()
			}
		}
	}
	o.updatesCnt++
	if session == nil {
		return
	}
	if o.updatesCnt >= o.updateRate {
		for k, offset := range o.curOffsets {
			session.MarkOffset(k.topic, k.partition, offset, "")
		}
		o.clearCurOffsets()
		o.updatesCnt = 0
	}
}

func (o *offsetsInfo) updateOffsets(newOffsets topicPartitionOffsetsMap) {
	o.mu.Lock()
	for key, newOffset := range newOffsets {
		if curOffset, ok := o.curOffsets[key]; !ok || newOffset < curOffset {
			o.curOffsets[key] = newOffset
			if prevOffset, ok := o.prevOffsets[key]; ok && prevOffset > newOffset && o.curOffsetLessThanPrevMetric != nil {
				o.curOffsetLessThanPrevMetric.WithLabelValues().Inc()
			}
		}
	}
	o.mu.Unlock()
}

func (o *offsetsInfo) clearCurOffsets() {
	for key, curOffset := range o.curOffsets {
		if prevOffset, ok := o.prevOffsets[key]; !ok || (ok && prevOffset < curOffset) {
			o.prevOffsets[key] = curOffset
		}
		delete(o.curOffsets, key)
	}
}

type offsetsCommitParams struct {
	sessionFn      sessionGetter
	sessionMu      *sync.RWMutex
	interval       time.Duration
	maxRetriesTime time.Duration
	minBackoff     time.Duration
	maxBackoff     time.Duration
	logger         *zap.SugaredLogger
}

func (o *offsetsInfo) commit(ctx context.Context, params offsetsCommitParams) {
	var err error
	var sleepDuration time.Duration
	if o.commitIsRunning {
		return
	}
	o.commitIsRunning = true
	ticker := time.NewTicker(params.interval)
	bo := backoff.NewExponentialBackOff()
	bo.InitialInterval = params.minBackoff
	bo.MaxInterval = params.maxBackoff
	bo.MaxElapsedTime = params.maxRetriesTime
	bo.Multiplier = 2

	tryCommit := func() error {
		params.sessionMu.RLock()
		session := params.sessionFn()
		if session == nil {
			params.sessionMu.RUnlock()
			return errors.New("session is nil")
		}
		for k, offset := range o.commitOffsets {
			session.MarkOffset(k.topic, k.partition, offset, "")
		}
		session.Commit()
		params.sessionMu.RUnlock()
		return nil
	}

commitLoop:
	for {
		select {
		case <-ticker.C:
			for k := range o.commitOffsets {
				delete(o.commitOffsets, k)
			}
			o.mu.Lock()
			for k, curOffset := range o.curOffsets {
				if prevOffset, ok := o.prevOffsets[k]; ok && prevOffset == curOffset {
					continue
				}
				o.commitOffsets[k] = curOffset
			}
			o.clearCurOffsets()
			o.mu.Unlock()
			// do not commit if offsets have not changed
			if len(o.commitOffsets) == 0 {
				continue commitLoop
			}
			bo.Reset()
			for {
				err = tryCommit()
				if err == nil {
					break
				}
				sleepDuration = bo.NextBackOff()
				if sleepDuration == bo.Stop {
					params.logger.Errorf("failed to commit offsets: %v", err)
					break
				}
				time.Sleep(sleepDuration)
			}
		case <-ctx.Done():
			return
		}
	}
}
