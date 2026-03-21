// Package keyserializer provides an input that serializes message delivery per
// key. For a given key, only one message is ever in-flight (un-ACKed) at a
// time. Additional messages for the same key are buffered until the prior
// message is ACKed. Messages with different keys are independent.
package keyserializer

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	err := service.RegisterBatchInput("key_serializer", spec, newKeySerializerInput)
	if err != nil {
		panic(err)
	}
}

var spec = service.NewConfigSpec().
	Summary("Wraps an input and serializes message delivery per key.").
	Description(`
Reads from a nested input and enforces per-key sequential delivery to the
pipeline. For a given key, only one message batch is ever outstanding
(un-ACKed) at a time. Additional messages for the same key are buffered
internally until the in-flight message is ACKed. Messages with different keys
are independent and may be processed concurrently.

If the key expression returns a null value the message bypasses serialization
entirely and is delivered immediately regardless of any in-flight messages.

This is useful when downstream processing must be ordered per key but the
pipeline runs with multiple threads.`).
	Field(service.NewBloblangField("key").
		Description("A Bloblang mapping evaluated per message to determine the serialization key. Must assign to root using mapping syntax, e.g. `root = this.session_id`. Messages with the same key are delivered sequentially. A null result (`root = null`) bypasses serialization for that message.")).
	Field(service.NewInputField("input").
		Description("The nested input to read messages from."))

type pendingBatch struct {
	batch  service.MessageBatch
	ackFn  service.AckFunc
	key    string
	bypass bool // true when key was null; no in-flight tracking
}

type keySerializerInput struct {
	nested  *service.OwnedInput
	keyExpr *bloblang.Executor
	log     *service.Logger

	// ready receives batches that are eligible for delivery to the pipeline.
	// Batches are placed here when their key has no outstanding message.
	ready chan *pendingBatch

	mu       sync.Mutex
	inFlight map[string]struct{}       // keys with an outstanding batch
	pending  map[string][]*pendingBatch // per-key queue of waiting batches

	readerCtx    context.Context
	readerCancel context.CancelFunc
	startOnce    sync.Once
	wg           sync.WaitGroup

	// closedCh is closed when the nested input terminates.
	closedCh chan struct{}
	closeErr error
}

func newKeySerializerInput(conf *service.ParsedConfig, res *service.Resources) (service.BatchInput, error) {
	keyExpr, err := conf.FieldBloblang("key")
	if err != nil {
		return nil, err
	}

	nested, err := conf.FieldInput("input")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &keySerializerInput{
		nested:       nested,
		keyExpr:      keyExpr,
		log:          res.Logger(),
		ready:        make(chan *pendingBatch, 4096),
		inFlight:     make(map[string]struct{}),
		pending:      make(map[string][]*pendingBatch),
		readerCtx:    ctx,
		readerCancel: cancel,
		closedCh:     make(chan struct{}),
	}, nil
}

// Connect implements service.BatchInput. Starts the background reader on first
// call.
func (k *keySerializerInput) Connect(ctx context.Context) error {
	k.startOnce.Do(func() {
		k.wg.Add(1)
		go k.readerLoop()
	})
	return nil
}

// readerLoop continuously reads from the nested input and dispatches batches to
// per-key queues. Batches whose key is not in-flight are sent directly to the
// ready channel; batches whose key is already in-flight are queued until the
// prior batch ACKs. Batches with a null key bypass serialization entirely.
func (k *keySerializerInput) readerLoop() {
	defer k.wg.Done()

	for {
		batch, ackFn, err := k.nested.ReadBatch(k.readerCtx)
		if err != nil {
			k.closeErr = err
			close(k.closedCh)
			return
		}

		if len(batch) == 0 {
			_ = ackFn(k.readerCtx, nil)
			continue
		}

		keyVal, err := batch[0].BloblangQueryValue(k.keyExpr)
		if err != nil {
			k.log.Errorf("Key expression evaluation failed, message will be nacked: %v", err)
			_ = ackFn(k.readerCtx, err)
			continue
		}

		var pb *pendingBatch
		if keyVal == nil {
			pb = &pendingBatch{batch: batch, ackFn: ackFn, bypass: true}
		} else {
			key, ok := keyVal.(string)
			if !ok {
				typeErr := fmt.Errorf("key expression must return a string or null, got %T", keyVal)
				k.log.Errorf("Key expression returned invalid type, message will be nacked: %v", typeErr)
				_ = ackFn(k.readerCtx, typeErr)
				continue
			}
			pb = &pendingBatch{batch: batch, ackFn: ackFn, key: key}

			k.mu.Lock()
			_, taken := k.inFlight[key]
			if !taken {
				k.inFlight[key] = struct{}{}
			} else {
				k.pending[key] = append(k.pending[key], pb)
			}
			k.mu.Unlock()

			if taken {
				continue
			}
		}

		// Send to ready outside the lock. If the channel is full this blocks
		// until the pipeline drains it.
		select {
		case k.ready <- pb:
		case <-k.readerCtx.Done():
			_ = ackFn(k.readerCtx, k.readerCtx.Err())
			k.closeErr = k.readerCtx.Err()
			close(k.closedCh)
			return
		}
	}
}

// ReadBatch implements service.BatchInput.
func (k *keySerializerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case pb, ok := <-k.ready:
		if !ok {
			return nil, nil, service.ErrEndOfInput
		}
		return pb.batch, k.wrapAckFn(pb), nil
	case <-k.closedCh:
		// Drain any messages that arrived before the close signal.
		select {
		case pb := <-k.ready:
			return pb.batch, k.wrapAckFn(pb), nil
		default:
		}
		if errors.Is(k.closeErr, service.ErrEndOfInput) {
			return nil, nil, service.ErrEndOfInput
		}
		return nil, nil, k.closeErr
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// wrapAckFn wraps the original AckFunc so that when a batch is ACKed, the next
// pending batch for that key (if any) is released to the ready channel.
//
// For bypassed batches (null key) the original AckFunc is returned unwrapped
// since there is no in-flight tracking to update.
//
// The key remains in inFlight until pending is empty to prevent the readerLoop
// from sending a newer message to ready before the next queued one is released.
func (k *keySerializerInput) wrapAckFn(pb *pendingBatch) service.AckFunc {
	if pb.bypass {
		return pb.ackFn
	}
	return func(ctx context.Context, err error) error {
		k.mu.Lock()
		next := k.popPending(pb.key)
		if next == nil {
			delete(k.inFlight, pb.key)
		}
		k.mu.Unlock()

		if next != nil {
			go func() {
				select {
				case k.ready <- next:
				case <-k.readerCtx.Done():
				}
			}()
		}

		return pb.ackFn(ctx, err)
	}
}

// popPending removes and returns the first pending batch for key, or nil if
// none. Must be called with k.mu held.
func (k *keySerializerInput) popPending(key string) *pendingBatch {
	q := k.pending[key]
	if len(q) == 0 {
		return nil
	}
	next := q[0]
	if len(q) == 1 {
		delete(k.pending, key)
	} else {
		k.pending[key] = q[1:]
	}
	return next
}

// Close implements service.BatchInput.
func (k *keySerializerInput) Close(ctx context.Context) error {
	k.readerCancel()
	k.wg.Wait()
	return k.nested.Close(ctx)
}
