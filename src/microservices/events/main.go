package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Event struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

var kafkaBroker string

func main() {
	kafkaBroker = getEnv("KAFKA_BROKERS", "kafka:9092")

	// Запускаем consumers для каждого топика
	go startConsumer("movie-events")
	go startConsumer("user-events")
	go startConsumer("payment-events")

	// Роуты
	http.HandleFunc("/api/events/movie", eventHandler("movie-events"))
	http.HandleFunc("/api/events/user", eventHandler("user-events"))
	http.HandleFunc("/api/events/payment", eventHandler("payment-events"))
	http.HandleFunc("/api/events/health", handleHealth)

	port := getEnv("PORT", "8082")
	log.Printf("Events service listening on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

// eventHandler возвращает http.HandlerFunc для определенного топика
func eventHandler(topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		defer r.Body.Close()

		if err := produceEvent(topic, b); err != nil {
			http.Error(w, "failed to produce event: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}
}

// produceEvent отправляет событие в Kafka
func produceEvent(topic string, eventData []byte) error {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
	})
	defer writer.Close()

	return writer.WriteMessages(context.Background(), kafka.Message{
		Value: eventData,
	})
}

// startConsumer слушает Kafka-топик и логирует сообщения
func startConsumer(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   topic,
		GroupID: topic + "-consumer",
	})

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("[%s] consumer error: %v", topic, err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf("[%s] Consumed event: %s", topic, string(msg.Value))
	}
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
