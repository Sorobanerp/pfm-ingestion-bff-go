package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	httpin "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/in/http"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/aws"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/kafka"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/pdf"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	port := envOrDefault("PORT", "8081")
	localstackURL := envOrDefault("LOCALSTACK_ENDPOINT", aws.DefaultLocalStackEndpoint)
	kafkaBrokers := strings.Split(envOrDefault("KAFKA_BROKERS", "localhost:9092"), ",")
	coreLedgerGrpcTarget := envOrDefault("CORE_LEDGER_GRPC_ADDR", "localhost:9090")

	// Establish East-West gRPC client connection readiness (ADR-003)
	grpcConn, err := grpc.NewClient(
		coreLedgerGrpcTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("WARN: failed to initialize gRPC client channel to %s: %v", coreLedgerGrpcTarget, err)
	} else {
		defer grpcConn.Close()
	}

	pdfParser := pdf.NewZeroPersistencePDFParser()
	awsAdapter := aws.NewResilientLocalStackAdapter(localstackURL, aws.DefaultResiliencePolicy())
	kafkaProducer := kafka.NewStatementEventProducer(kafkaBrokers)
	defer kafkaProducer.Close()

	ingestService := service.NewStatementIngestionService(
		pdfParser,
		awsAdapter,
		awsAdapter,
		kafkaProducer,
	)

	httpServer := httpin.NewServer(ingestService)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("pfm-ingestion-bff-go listening on :%s (HTTP/2 & HTTP/3-Ready)", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func envOrDefault(key, fallback string) string {
	if val := strings.TrimSpace(os.Getenv(key)); val != "" {
		return val
	}
	return fallback
}
