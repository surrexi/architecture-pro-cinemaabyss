package main

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
)

var (
	monolithURL        string
	moviesServiceURL   string
	gradualMigration   bool
	moviesMigrationPct int
)

func main() {
	port := getEnv("PORT", "8000")
	monolithURL = getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesServiceURL = getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	gradualMigration = getEnv("GRADUAL_MIGRATION", "false") == "true"
	moviesMigrationPct, _ = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))

	rand.New(rand.NewSource(rand.Int63()))

	http.HandleFunc("/api/movies", eventHandler(true))
	http.HandleFunc("/api/users", eventHandler(false))
	http.HandleFunc("/health", handleHealth)

	log.Printf("Proxy service started on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func eventHandler(isMovies bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targetURL := monolithURL
		if isMovies && gradualMigration && shouldRouteToMovies() {
			targetURL = moviesServiceURL
		}

		// Проксируем запрос
		proxyURL, _ := url.Parse(targetURL)
		req, err := http.NewRequest(r.Method, proxyURL.String()+r.RequestURI, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		req.Header = r.Header

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			http.Error(w, "proxy error: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		// Проксируем ответ
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func shouldRouteToMovies() bool {
	return rand.Intn(100) < moviesMigrationPct
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
