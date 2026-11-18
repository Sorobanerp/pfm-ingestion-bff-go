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
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/kafka"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/pdf"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/sefaz"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/service"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/observability"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Getenv); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func run(ctx context.Context, getenv func(string) string) error {
	_ = observability.InitOtlpConfig(getenv, "pfm-ingestion-bff-go")
	port := envOrDefault(getenv, "SERVER_PORT", envOrDefault(getenv, "PORT", "18100"))
	localstackURL := envOrDefault(getenv, "AWS_ENDPOINT_URL", envOrDefault(getenv, "LOCALSTACK_ENDPOINT", aws.DefaultLocalStackEndpoint))
	kafkaBrokers := strings.Split(envOrDefault(getenv, "KAFKA_BROKERS", "localhost:19092"), ",")
	coreLedgerGrpcTarget := envOrDefault(getenv, "CORE_LEDGER_GRPC_ADDR", "localhost:18210")
	eastWestScheme := envOrDefault(getenv, "EAST_WEST_SCHEME", "http")
	coreLedgerHttpURL := envOrDefault(getenv, "CORE_LEDGER_HTTP_URL", eastWestScheme+"://localhost:18200")
	forecastHttpURL := envOrDefault(getenv, "FORECAST_ENGINE_HTTP_URL", eastWestScheme+"://"+envOrDefault(getenv, "FORECAST_GRPC_ADDR", "localhost:18110"))

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

	eastWestClient := grpcout.NewEastWestGrpcBridgeClient(coreLedgerHttpURL, forecastHttpURL)
	ledgerForwarder := sefaz.NewHttpGrpcBridgeForwarder(coreLedgerHttpURL)
	nfceScraper := sefaz.NewSefazNfceScraper(ledgerForwarder)

	httpServer := httpin.NewServer(ingestService).
		WithEastWestClient(eastWestClient).
		WithNfceScraper(nfceScraper)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           httpServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("pfm-ingestion-bff-go listening on :%s (HTTP/2 & HTTP/3-Ready)", port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func envOrDefault(getenv func(string) string, key, fallback string) string {
	if val := strings.TrimSpace(getenv(key)); val != "" {
		return val
	}
	return fallback
}
