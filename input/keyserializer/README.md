# key_serializer input

Wraps any Redpanda Connect input and enforces per-key sequential message delivery to the pipeline.

## Problem

Redpanda Connect's pipeline runs multiple processor threads in parallel (defaulting to one per CPU). When an input such as NATS JetStream emits several messages before waiting for ACKs, those messages can be picked up by different pipeline threads simultaneously and processed out of order. For workloads where all messages sharing a key must be processed strictly in sequence — for example, database mutations that must be applied in order — this is a correctness problem.

`key_serializer` solves this by ensuring that for any given key, only one message is ever in-flight (un-ACKed) at a time. The next message for a key is not released to the pipeline until the previous one has been fully ACKed by the output. Messages with different keys are independent and continue to be processed concurrently.

## Configuration

```yaml
input:
  key_serializer:
    key: <bloblang mapping>
    input:
      <nested input>
```

### Fields

| Field | Description |
|---|---|
| `key` | A Bloblang mapping evaluated against each message to produce the serialization key. A `null` result bypasses serialization for that message. |
| `input` | The nested input to read from. Any Redpanda Connect input is supported. |

## Examples

### NATS JetStream with subject-based ordering

```yaml
input:
  key_serializer:
    key: meta("nats_subject")
    input:
      nats_jetstream:
        urls: [nats://localhost:4222]
        subject: orders.>
        durable: orders-consumer
```

All messages on the same NATS subject are delivered to the pipeline in order. Messages on different subjects are processed concurrently.

### Conditional bypass

Messages that should not be serialized (e.g. control messages or one-off events) can opt out by returning `null` from the key expression:

```yaml
input:
  key_serializer:
    key: |
      if meta("type") == "ordered" { meta("entity_id") } else { null }
    input:
      nats_jetstream:
        urls: [nats://localhost:4222]
        subject: events.>
        durable: events-consumer
```

## Behaviour

### Ordering guarantee

Within a key, messages are delivered to the pipeline in the order the nested input produced them. The next message for a key only becomes available to `ReadBatch` after the prior message's `AckFunc` is called — whether that call is an ACK or a NACK.

### Null key bypass

If the `key` expression returns `null`, the message bypasses serialization entirely. It is delivered to the pipeline immediately without regard to any other in-flight messages, including those with the same key that another expression would have produced. Use this for messages that are safe to process concurrently regardless of ordering.

### Backpressure and buffering

The component reads from the nested input as fast as it can, buffering messages for busy keys internally. There is no configured limit on the number of buffered messages; memory is the practical bound. The nested input is never artificially paused — it is the consumer's ACK rate that controls how quickly buffered messages are released.

### NACK handling

A NACK (non-nil error passed to `AckFunc`) releases the next pending message for the key just as a regular ACK does. The component does not retry the NACKed message itself — retry behaviour is the responsibility of the nested input or a surrounding wrapper such as `auto_replay_nacks`.

## Building

This plugin is compiled into a custom Redpanda Connect binary. From the module root:

```bash
go build -o redpanda-connect ./cmd/redpanda-connect
```
