# Kafka plugin
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

### Config params
**`brokers`** *`[]string`* *`required`* 

The name of kafka brokers to read from.

<br>

**`topics`** *`[]string`* *`required`* 

The list of kafka topics to read from.

<br>

**`consumer_group`** *`string`* *`default=file-d`* 

The name of consumer group to use.

<br>

**`channel_buffer_size`** *`int`* *`default=256`* 

The number of unprocessed messages in the buffer that are loaded in the background from kafka.

<br>

**`offset`** *`string`* *`default=newest`* *`options=oldest|newest`* 

The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
* *`newest`* - set offset to the newest message
* *`oldest`* - set offset to the oldest message

<br>

**`consumer_max_processing_time`** *`cfg.Duration`* *`default=200ms`* 

The maximum amount of time the consumer expects a message takes to process for the user.

<br>

**`consumer_max_wait_time`** *`cfg.Duration`* *`default=250ms`* 

The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways.

<br>

**`manual_offsets_commit_interval`** *`cfg.Duration`* *`default=0s`* 

If value is set to greater than 0 plugin marks and commits offsets manually periodically by the given interval.
Otherwise enables autocommit. Default 0s no manual commit.

<br>

**`manual_commit_max_retries_time`** *`cfg.Duration`* *`default=2ms`* 

Only used if manual offsets is used.
If value is set to greater than 0 retries until the duration is passed. If set to 0 retries never stop.

<br>

**`manual_commit_min_backoff`** *`cfg.Duration`* *`default=50ms`* 

Only used if manual offsets is used.
Minimum backoff value for manual commits retries.

<br>

**`cmanual_ommit_max_backoff`** *`cfg.Duration`* *`default=1s`* 

Only used if manual offsets is used.
Maximum backoff value for manual commits retries.

<br>

**`mark_offsets_rate`** *`int`* *`default=0`* 

If value is set to greater than 0 tries to mark offsets every `mark_offsets_rate` finished jobs.
Manual commit mode has priority over this mode. So if both values are set to greater than 0
only manual commit is used and this parameter is ignored. Default 0 usual mark offset.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*