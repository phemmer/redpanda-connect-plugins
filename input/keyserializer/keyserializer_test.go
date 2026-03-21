package keyserializer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockInput is a controllable BatchInput for testing. Tests push batches via
// push() and observe ACKs via the returned channel.
type mockInput struct {
	incoming chan *mockMsg
}

type mockMsg struct {
	batch service.MessageBatch
	ackCh chan error
}

func newMockInput() *mockInput {
	return &mockInput{incoming: make(chan *mockMsg, 100)}
}

// push enqueues a batch for the key_serializer to read. The returned channel
// receives the ACK/NACK error when the downstream pipeline calls the AckFunc.
func (m *mockInput) push(batch service.MessageBatch) <-chan error {
	ch := make(chan error, 1)
	m.incoming <- &mockMsg{batch: batch, ackCh: ch}
	return ch
}

// end signals ErrEndOfInput to the key_serializer.
func (m *mockInput) end() { close(m.incoming) }

func (m *mockInput) Connect(_ context.Context) error { return nil }

func (m *mockInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case msg, ok := <-m.incoming:
		if !ok {
			return nil, nil, service.ErrEndOfInput
		}
		return msg.batch, func(_ context.Context, err error) error {
			msg.ackCh <- err
			return nil
		}, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (m *mockInput) Close(_ context.Context) error { return nil }

// newTestInputRes builds a keySerializerInput wired to the given mock and
// resources. Connect is called and Close is registered as a test cleanup.
func newTestInputRes(t *testing.T, keyExpr string, mock *mockInput, res *service.Resources) *keySerializerInput {
	t.Helper()

	env := service.NewEnvironment()
	require.NoError(t, env.RegisterBatchInput("mock_input", service.NewConfigSpec(),
		func(_ *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
			return mock, nil
		}))

	yaml := fmt.Sprintf("key: '%s'\ninput:\n  mock_input: {}", keyExpr)
	pConf, err := spec.ParseYAML(yaml, env)
	require.NoError(t, err)

	inp, err := newKeySerializerInput(pConf, res)
	require.NoError(t, err)

	ks := inp.(*keySerializerInput)
	require.NoError(t, ks.Connect(t.Context()))
	t.Cleanup(func() { _ = ks.Close(context.Background()) })

	return ks
}

// newTestInput builds a keySerializerInput wired to the given mock, using the
// provided Bloblang mapping expression as the key. Connect is called and
// Close is registered as a test cleanup.
func newTestInput(t *testing.T, keyExpr string, mock *mockInput) *keySerializerInput {
	t.Helper()
	return newTestInputRes(t, keyExpr, mock, service.MockResources())
}

// msg builds a single-message batch with a metadata key field set to key.
func msg(body, key string) service.MessageBatch {
	m := service.NewMessage([]byte(body))
	m.MetaSet("key", key)
	return service.MessageBatch{m}
}

// mustReadBatch calls ReadBatch with a 5-second timeout and fails the test if
// it returns an error.
func mustReadBatch(t *testing.T, ks *keySerializerInput) (service.MessageBatch, service.AckFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	batch, ackFn, err := ks.ReadBatch(ctx)
	require.NoError(t, err)
	return batch, ackFn
}

// mustNotRead asserts that ReadBatch does not return a message within a short
// window, confirming the key_serializer is holding the message back.
func mustNotRead(t *testing.T, ks *keySerializerInput) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, _, err := ks.ReadBatch(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"expected ReadBatch to be blocked but it returned a message")
}

// batchBody returns the body of the first message in the batch.
func batchBody(t *testing.T, batch service.MessageBatch) string {
	t.Helper()
	require.NotEmpty(t, batch)
	b, err := batch[0].AsBytes()
	require.NoError(t, err)
	return string(b)
}

// TestDifferentKeysDeliveredConcurrently verifies that messages with different
// keys are both available without needing to ACK either.
func TestDifferentKeysDeliveredConcurrently(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	mock.push(msg("a1", "A"))
	mock.push(msg("b1", "B"))

	batch1, ack1 := mustReadBatch(t, ks)
	batch2, ack2 := mustReadBatch(t, ks)

	bodies := []string{batchBody(t, batch1), batchBody(t, batch2)}
	assert.ElementsMatch(t, []string{"a1", "b1"}, bodies)

	require.NoError(t, ack1(t.Context(), nil))
	require.NoError(t, ack2(t.Context(), nil))
}

// TestSameKeySerializes verifies that a second message for the same key is
// held back until the first is ACKed.
func TestSameKeySerializes(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	ackRecv1 := mock.push(msg("first", "K"))
	mock.push(msg("second", "K"))

	// First message is immediately available.
	batch1, ack1 := mustReadBatch(t, ks)
	assert.Equal(t, "first", batchBody(t, batch1))

	// Second message is held back.
	mustNotRead(t, ks)

	// ACK the first — second should now be released.
	require.NoError(t, ack1(t.Context(), nil))
	assert.NoError(t, <-ackRecv1)

	batch2, ack2 := mustReadBatch(t, ks)
	assert.Equal(t, "second", batchBody(t, batch2))

	require.NoError(t, ack2(t.Context(), nil))
}

// TestSameKeyOrderPreserved verifies that three messages for the same key are
// delivered in the order they were produced.
func TestSameKeyOrderPreserved(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	mock.push(msg("one", "K"))
	mock.push(msg("two", "K"))
	mock.push(msg("three", "K"))

	b1, ack1 := mustReadBatch(t, ks)
	assert.Equal(t, "one", batchBody(t, b1))
	mustNotRead(t, ks)

	require.NoError(t, ack1(t.Context(), nil))

	b2, ack2 := mustReadBatch(t, ks)
	assert.Equal(t, "two", batchBody(t, b2))
	mustNotRead(t, ks)

	require.NoError(t, ack2(t.Context(), nil))

	b3, ack3 := mustReadBatch(t, ks)
	assert.Equal(t, "three", batchBody(t, b3))

	require.NoError(t, ack3(t.Context(), nil))
}

// TestNackReleasesNextPending verifies that a NACK (non-nil ACK error) still
// releases the next pending message for the same key.
func TestNackReleasesNextPending(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	mock.push(msg("first", "K"))
	mock.push(msg("second", "K"))

	_, ack1 := mustReadBatch(t, ks)
	mustNotRead(t, ks)

	// NACK the first message.
	require.NoError(t, ack1(t.Context(), assert.AnError))

	// Second message should now be released regardless.
	b2, ack2 := mustReadBatch(t, ks)
	assert.Equal(t, "second", batchBody(t, b2))
	require.NoError(t, ack2(t.Context(), nil))
}

// TestMixedKeysInterleavedRelease verifies the combined behaviour: two keys
// run independently, and pending messages for each key are released in order.
func TestMixedKeysInterleavedRelease(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	// Enqueue: A1, A2, B1 — A2 must wait for A1; B1 is independent.
	mock.push(msg("a1", "A"))
	mock.push(msg("a2", "A"))
	mock.push(msg("b1", "B"))

	// A1 and B1 should both be readable without ACKing either.
	b1, ack1 := mustReadBatch(t, ks)
	b2, ack2 := mustReadBatch(t, ks)

	bodies := []string{batchBody(t, b1), batchBody(t, b2)}
	assert.ElementsMatch(t, []string{"a1", "b1"}, bodies)

	// A2 is still held back.
	mustNotRead(t, ks)

	// Determine which ack belongs to A.
	var ackA, ackB service.AckFunc
	if batchBody(t, b1) == "a1" {
		ackA, ackB = ack1, ack2
	} else {
		ackA, ackB = ack2, ack1
	}

	// ACK A1 — A2 should now be released.
	require.NoError(t, ackA(t.Context(), nil))

	b3, ack3 := mustReadBatch(t, ks)
	assert.Equal(t, "a2", batchBody(t, b3))

	require.NoError(t, ackB(t.Context(), nil))
	require.NoError(t, ack3(t.Context(), nil))
}

// TestErrEndOfInputPropagates verifies that when the nested input signals
// ErrEndOfInput, ReadBatch eventually returns ErrEndOfInput too.
func TestErrEndOfInputPropagates(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	mock.push(msg("only", "K"))
	mock.end()

	batch, ackFn := mustReadBatch(t, ks)
	assert.Equal(t, "only", batchBody(t, batch))
	require.NoError(t, ackFn(t.Context(), nil))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _, err := ks.ReadBatch(ctx)
	require.ErrorIs(t, err, service.ErrEndOfInput)
}

// msgNoKey builds a single-message batch with no "key" metadata, so
// meta("key") returns null and the message bypasses serialization.
func msgNoKey(body string) service.MessageBatch {
	return service.MessageBatch{service.NewMessage([]byte(body))}
}

// TestNullKeyBypassesSerialization verifies that a message whose key
// expression evaluates to null is delivered immediately, even when another
// message with a keyed in-flight batch would otherwise be blocked.
func TestNullKeyBypassesSerialization(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	// Keyed message first, then a null-key message behind it.
	mock.push(msg("keyed", "K"))
	mock.push(msgNoKey("bypass"))

	// Both should be immediately readable — the null-key message does not
	// wait for the keyed message to be ACKed.
	b1, ack1 := mustReadBatch(t, ks)
	b2, ack2 := mustReadBatch(t, ks)

	bodies := []string{batchBody(t, b1), batchBody(t, b2)}
	assert.ElementsMatch(t, []string{"keyed", "bypass"}, bodies)

	require.NoError(t, ack1(t.Context(), nil))
	require.NoError(t, ack2(t.Context(), nil))
}

// TestNullKeyDoesNotBlockKeyedMessages verifies that a pending null-key
// message does not interfere with the serialization of keyed messages.
func TestNullKeyDoesNotBlockKeyedMessages(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	mock.push(msg("k1", "K"))
	mock.push(msgNoKey("bypass"))
	mock.push(msg("k2", "K"))

	// k1 and bypass are both immediately available; k2 waits for k1.
	b1, ack1 := mustReadBatch(t, ks)
	b2, ack2 := mustReadBatch(t, ks)

	bodies := []string{batchBody(t, b1), batchBody(t, b2)}
	assert.ElementsMatch(t, []string{"k1", "bypass"}, bodies)

	mustNotRead(t, ks)

	// ACK whichever was k1 to release k2.
	var ackK1 service.AckFunc
	if batchBody(t, b1) == "k1" {
		ackK1 = ack1
		require.NoError(t, ack2(t.Context(), nil))
	} else {
		ackK1 = ack2
		require.NoError(t, ack1(t.Context(), nil))
	}
	require.NoError(t, ackK1(t.Context(), nil))

	b3, ack3 := mustReadBatch(t, ks)
	assert.Equal(t, "k2", batchBody(t, b3))
	require.NoError(t, ack3(t.Context(), nil))
}

// msgWithIntKey builds a batch that causes the key expression
// `root = if meta("intkey") != null { 42 } else { meta("key") }` to
// return an integer (invalid) for the given body.
func msgWithIntKey(body string) service.MessageBatch {
	m := service.NewMessage([]byte(body))
	m.MetaSet("intkey", "1")
	return service.MessageBatch{m}
}

// TestNonStringKeyNacks verifies that when the key expression returns a
// non-string, non-null value, the message is nacked and never delivered to
// ReadBatch.
func TestNonStringKeyNacks(t *testing.T) {
	mock := newMockInput()
	// Expression returns int64(42) when meta "intkey" is set, string otherwise.
	ks := newTestInput(t, `root = if meta("intkey") != null { 42 } else { meta("key") }`, mock)

	badAckCh := mock.push(msgWithIntKey("bad"))
	goodAckCh := mock.push(msg("good", "K"))

	// "bad" is nacked (non-string key); "good" has a valid string key and
	// should be delivered immediately since "bad" was never in-flight.
	b, ack := mustReadBatch(t, ks)
	assert.Equal(t, "good", batchBody(t, b))
	require.NoError(t, ack(t.Context(), nil))
	assert.NoError(t, <-goodAckCh)

	// The nack for "bad" should have been delivered before ReadBatch returned.
	select {
	case err := <-badAckCh:
		require.Error(t, err)
	default:
		t.Fatal("expected nack for bad message but none received")
	}
}

// newCapturingLogger returns a service.Logger that writes to buf and a
// *service.Resources wired to it, for asserting on logged output.
func newCapturingLogger(buf *bytes.Buffer) *service.Resources {
	handler := slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := service.NewLoggerFromSlog(slog.New(handler))
	return service.MockResources(service.MockResourcesOptUseLogger(logger))
}

// TestNonStringKeyLogsError verifies that when the key expression returns a
// non-string, non-null value the error is logged at error level.
func TestNonStringKeyLogsError(t *testing.T) {
	var buf bytes.Buffer
	res := newCapturingLogger(&buf)

	mock := newMockInput()
	newTestInputRes(t, `root = if meta("intkey") != null { 42 } else { meta("key") }`, mock, res)

	ackCh := mock.push(msgWithIntKey("bad"))

	// Wait for the nack to confirm the readerLoop processed the message.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-ackCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for nack")
	}

	assert.True(t, strings.Contains(buf.String(), "invalid type"),
		"expected error log about invalid type, got: %s", buf.String())
}

// TestEvalErrorLogsError verifies that when the key expression returns an
// evaluation error the error is logged at error level.
func TestEvalErrorLogsError(t *testing.T) {
	var buf bytes.Buffer
	res := newCapturingLogger(&buf)

	mock := newMockInput()
	// throw() forces a runtime evaluation error from the mapping.
	newTestInputRes(t, `root = throw("forced error")`, mock, res)

	ackCh := mock.push(msg("bad", "K"))

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	select {
	case <-ackCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for nack")
	}

	assert.True(t, strings.Contains(buf.String(), "forced error"),
		"expected error log containing eval error, got: %s", buf.String())
}

// TestCloseDoesNotHang verifies that Close returns promptly even when
// ReadBatch is blocked waiting for a message.
func TestCloseDoesNotHang(t *testing.T) {
	mock := newMockInput()
	ks := newTestInput(t, `root = meta("key")`, mock)

	// ReadBatch in background — will block because no messages are enqueued.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = ks.ReadBatch(t.Context())
	}()

	// Close should not hang.
	closeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, ks.Close(closeCtx))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ReadBatch goroutine did not unblock after Close")
	}
}
