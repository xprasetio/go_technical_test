package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
)

// Event adalah struktur data yang mewakili sebuah peristiwa
type Event struct {
	ID      int
	Payload string
}

// Worker bertugas memproses peristiwa dari eventQueue
func Worker(workerID int, eventQueue <-chan Event, wg *sync.WaitGroup, redisClient *redis.Client, db *sql.DB) {
	defer wg.Done() // Memastikan worker selesai saat prosesnya selesai
	for event := range eventQueue {
		// Simulasi pemrosesan peristiwa
		fmt.Printf("Worker %d memproses Event ID: %d dengan data: %s\n", workerID, event.ID, event.Payload)
		time.Sleep(1 * time.Second) // Simulasi waktu pemrosesan

		// Simpan data peristiwa ke Redis sebagai data sementara
		err := redisClient.Set(context.Background(), fmt.Sprintf("event:%d", event.ID), event.Payload, 0).Err()
		if err != nil {
			log.Printf("Error menyimpan ke Redis: %v\n", err)
			continue
		}

		// Setelah pemrosesan selesai, simpan ke database MySQL
		_, err = db.Exec("INSERT INTO events (id, payload) VALUES (?, ?)", event.ID, event.Payload)
		if err != nil {
			log.Printf("Error menyimpan ke MySQL: %v\n", err)
		} else {
			fmt.Printf("Event ID: %d berhasil disimpan ke MySQL\n", event.ID)
		}
	}
}

func main() {
	// Konfigurasi Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Alamat Redis
	})
	ctx := context.Background()

	// Cek koneksi ke Redis
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Gagal terhubung ke Redis: %v\n", err)
	}
	fmt.Println("Terhubung ke Redis")

	// Koneksi ke MySQL
	dsn := "root:@tcp(localhost:3306)/go_technical_test"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Gagal terhubung ke MySQL: %v\n", err)
	}
	defer db.Close()

	// Pastikan koneksi MySQL valid
	err = db.Ping()
	if err != nil {
		log.Fatalf("Gagal ping MySQL: %v\n", err)
	}
	fmt.Println("Terhubung ke MySQL")

	// Membuat tabel events jika belum ada
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id INT PRIMARY KEY,
			payload VARCHAR(255) NOT NULL
		)`)
	if err != nil {
		log.Fatalf("Gagal membuat tabel MySQL: %v\n", err)
	}

	// Daftar peristiwa
	events := []Event{
		{ID: 1, Payload: "Data Pertama"},
		{ID: 2, Payload: "Data Kedua"},
		{ID: 3, Payload: "Data Ketiga"},
		{ID: 4, Payload: "Data Keempat"},
		{ID: 5, Payload: "Data Kelima"},
	}

	// Channel untuk menyalurkan peristiwa ke para worker
	eventQueue := make(chan Event, len(events))
	var wg sync.WaitGroup

	// Jumlah worker
	const numWorkers = 3

	// Memulai worker
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1) // Tambahkan satu tugas ke worker
		go Worker(i, eventQueue, &wg, rdb, db)
	}

	// Mengirim peristiwa ke eventQueue
	for _, event := range events {
		eventQueue <- event
	}
	close(eventQueue) // Tutup eventQueue setelah semua peristiwa dikirim

	// Tunggu semua worker menyelesaikan tugasnya
	wg.Wait()
	fmt.Println("Semua peristiwa selesai diproses.")
}
