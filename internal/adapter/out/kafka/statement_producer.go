package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const TopicStatementRawExtracted = "statement.raw.extracted"

// StatementExtractedKafkaEvent mirrors the `StatementTransactionExtractedEvent` contract in `pfm-contracts`
// with exact decimal string representation and SHA-256 `idempotency_key`.
type StatementExtractedKafkaEvent struct {
	TenantID           string `json:"tenant_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	StatementID        string `json:"statement_id"`
	CardLastFour       string `json:"card_last_four"`
	TransactionDate    string `json:"transaction_date"`
	RawDescription     string `json:"raw_description"`
	MerchantNormalized string `json:"merchant_normalized"`
	Amount             string `json:"amount"`
	Currency           string `json:"currency"`
	InstallmentNumber  int    `json:"installment_number"`
	TotalInstallments  int    `json:"total_installments"`
	Kind               string `json:"kind"`
	OccurredAt         string `json:"occurred_at"`
}

// MessageWriter abstracts kafka-go Writer for unit testing and production Kafka brokers.
type MessageWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafkago.Message) error
	Close() error
}

// KafkaConfig configures Kafka producer connectivity, supporting both local KRaft and Upstash Kafka Serverless (SASL_SSL).
type KafkaConfig struct {
	Brokers    []string
	Topic      string
	Username   string
	Password   string
	UseSASLSSL bool
}

// StatementEventProducer publishes extracted statement transactions to the `statement.raw.extracted` topic,
// strictly partitioned by `tenant_id` (ADR-005).
type StatementEventProducer struct {
	writer MessageWriter
}

func NewStatementEventProducer(brokers []string) *StatementEventProducer {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        TopicStatementRawExtracted,
		Balancer:     &kafkago.Hash{}, // Deterministic hash partitioning by Key (`tenant_id`)
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireAll,
	}
	return &StatementEventProducer{writer: w}
}

// NewUpstashStatementEventProducer creates a Kafka producer compatible with Upstash Kafka Serverless (SASL_SSL / SCRAM-SHA-256 + TLS 1.2).
func NewUpstashStatementEventProducer(cfg KafkaConfig) (*StatementEventProducer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers must not be empty")
	}
	topic := cfg.Topic
	if topic == "" {
		topic = TopicStatementRawExtracted
	}
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireAll,
	}
	if cfg.UseSASLSSL {
		mechanism, err := scram.Mechanism(scram.SHA256, cfg.Username, cfg.Password)
		if err != nil {
			return nil, fmt.Errorf("failed to create SCRAM-SHA-256 mechanism: %w", err)
		}
		w.Transport = &kafkago.Transport{
			SASL: mechanism,
			TLS:  &tls.Config{MinVersion: tls.VersionTLS13},
		}
	}
	return &StatementEventProducer{writer: w}, nil
}

// NewStatementEventProducerFromEnv builds a Kafka producer from environment variables (supporting SASL_SSL for Upstash).
func NewStatementEventProducerFromEnv(brokers []string) (*StatementEventProducer, error) {
	protocol := strings.ToUpper(strings.TrimSpace(os.Getenv("KAFKA_SECURITY_PROTOCOL")))
	useSASL := protocol == "SASL_SSL"
	return NewUpstashStatementEventProducer(KafkaConfig{
		Brokers:    brokers,
		Topic:      TopicStatementRawExtracted,
		Username:   os.Getenv("KAFKA_SASL_USERNAME"),
		Password:   os.Getenv("KAFKA_SASL_PASSWORD"),
		UseSASLSSL: useSASL,
	})
}

func NewStatementEventProducerWithWriter(writer MessageWriter) *StatementEventProducer {
	return &StatementEventProducer{writer: writer}
}

func (p *StatementEventProducer) PublishStatementTransactions(
	ctx context.Context,
	statement *domain.Statement,
) error {
	if statement == nil || len(statement.Transactions) == 0 {
		return nil
	}

	msgs := make([]kafkago.Message, 0, len(statement.Transactions))
	nowISO := time.Now().UTC().Format(time.RFC3339)

	for _, tx := range statement.Transactions {
		evt := StatementExtractedKafkaEvent{
			TenantID:           tx.TenantID,
			IdempotencyKey:     tx.IdempotencyKey,
			StatementID:        tx.StatementID,
			CardLastFour:       tx.CardLast4,
			TransactionDate:    tx.CompetenceDate,
			RawDescription:     tx.RawDescription,
			MerchantNormalized: tx.MerchantNormalized,
			Amount:             tx.Amount.Fixed4(),
			Currency:           tx.Currency,
			InstallmentNumber:  tx.CurrentInstallment,
			TotalInstallments:  tx.TotalInstallments,
			Kind:               string(tx.Kind),
			OccurredAt:         nowISO,
		}
		payload, err := json.Marshal(evt)
		if err != nil {
			return err
		}
		msgs = append(msgs, kafkago.Message{
			// Partition strictly by tenant_id (ADR-005)
			Key:   []byte(tx.TenantID),
			Value: payload,
			Headers: []kafkago.Header{
				{Key: "tenant_id", Value: []byte(tx.TenantID)},
				{Key: "idempotency_key", Value: []byte(tx.IdempotencyKey)},
				{Key: "schema_version", Value: []byte("1.0.0")},
			},
		})
	}

	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *StatementEventProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
