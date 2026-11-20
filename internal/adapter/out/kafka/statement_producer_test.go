package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMessageWriter struct {
	messages []kafkago.Message
	err      error
	closed   bool
}

func (m *mockMessageWriter) WriteMessages(ctx context.Context, msgs ...kafkago.Message) error {
	if m.err != nil {
		return m.err
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockMessageWriter) Close() error {
	m.closed = true
	return nil
}

func TestStatementEventProducer(t *testing.T) {
	mockWriter := &mockMessageWriter{}
	producer := NewStatementEventProducerWithWriter(mockWriter)
	ctx := context.Background()

	// 1. Nil or empty statement
	err := producer.PublishStatementTransactions(ctx, nil)
	require.NoError(t, err)

	emptyStmt := &domain.Statement{
		Transactions: nil,
	}
	err = producer.PublishStatementTransactions(ctx, emptyStmt)
	require.NoError(t, err)

	// 2. Statement with transactions
	dec, _ := domain.ParseExactDecimal("150.00")
	stmt := &domain.Statement{
		StatementID: "stmt-1",
		TenantID:    "tenant-1",
		Transactions: []domain.RawTransaction{
			{
				TenantID:           "tenant-1",
				StatementID:        "stmt-1",
				IdempotencyKey:     "idemp-1",
				CardLast4:          "1234",
				CompetenceDate:     "2026-04-10",
				RawDescription:     "MERCADO LIVRE",
				MerchantNormalized: "MERCADO LIVRE",
				Amount:             dec,
				Currency:           "BRL",
				CurrentInstallment: 1,
				TotalInstallments:  1,
				Kind:               domain.TransactionKindSpot,
			},
		},
	}

	err = producer.PublishStatementTransactions(ctx, stmt)
	require.NoError(t, err)
	assert.Len(t, mockWriter.messages, 1)
	assert.Equal(t, []byte("tenant-1"), mockWriter.messages[0].Key)

	// 3. Close
	err = producer.Close()
	require.NoError(t, err)
	assert.True(t, mockWriter.closed)

	// 4. Producer with brokers constructor
	pReal := NewStatementEventProducer([]string{"localhost:9092"})
	require.NotNil(t, pReal)
	_ = pReal.Close()

	// 5. Producer with writer error
	mockFail := &mockMessageWriter{err: errors.New("kafka error")}
	pFail := NewStatementEventProducerWithWriter(mockFail)
	err = pFail.PublishStatementTransactions(ctx, stmt)
	assert.Error(t, err)
}

func TestStatementEventProducer_NilWriterClose(t *testing.T) {
	p := &StatementEventProducer{}
	err := p.Close()
	assert.NoError(t, err)
}

func TestNewUpstashStatementEventProducer_SASLSSL(t *testing.T) {
	p, err := NewUpstashStatementEventProducer(KafkaConfig{
		Brokers:    []string{"upstash-broker.upstash.io:9092"},
		Username:   "upstash-user",
		Password:   "upstash-pass",
		UseSASLSSL: true,
	})
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, p.Close())

	_, errEmpty := NewUpstashStatementEventProducer(KafkaConfig{})
	assert.Error(t, errEmpty)

	t.Setenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
	t.Setenv("KAFKA_SASL_USERNAME", "env-user")
	t.Setenv("KAFKA_SASL_PASSWORD", "env-pass")
	pEnv, errEnv := NewStatementEventProducerFromEnv([]string{"upstash-broker.upstash.io:9092"})
	require.NoError(t, errEnv)
	require.NotNil(t, pEnv)
	require.NoError(t, pEnv.Close())
}
