package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var mtx sync.RWMutex
var TotalRequests atomic.Int64
var SuccessfulBookings atomic.Int64
var FailedBookings atomic.Int64

type Ticket struct {
	ID     string
	Flight string
	Seat   string
	Price  float64
}

type BookingRequest struct {
	Flight     string     `json:"flight"`
	CustomerID string     `json:"customer_id"`
	ResultChan chan error `json:"result"`
}

type BookingStatusResponse struct {
	Flight         string `json:"flight"`
	AvailableSeats int    `json:"available_seats"`
}

type MetricsInfoResponse struct {
	TotalRequests      int64 `json:"total_requests"`
	SuccessfulBookings int64 `json:"successful_bookings"`
	FailedBookings     int64 `json:"failed_bookings"`
}

type TicketsResponse struct {
	Tickets []Ticket `json:"tickets"`
}

type FlightStorage struct {
	seats  map[string]int
	prices map[string]float64
	sold   []Ticket
}

func NewFlightStorage(seats map[string]int, prices map[string]float64) *FlightStorage {
	return &FlightStorage{seats: seats, prices: prices}
}

func (fs *FlightStorage) CheckSeats(flight string) int {
	defer mtx.RUnlock()

	mtx.RLock()
	seats, ok := fs.seats[flight]
	if !ok {
		fmt.Println("Проверка мест на несуществующем рейсе")
		return seats
	}
	return seats
}

func (fs *FlightStorage) GetAllSeats() map[string]int {
	defer mtx.RUnlock()

	mtx.RLock()
	copySeats := make(map[string]int, len(fs.seats))
	for flight, count := range fs.seats {
		copySeats[flight] = count
	}

	return copySeats
}

func (fs *FlightStorage) BookSeat(req BookingRequest) (Ticket, error) {
	defer mtx.Unlock()

	mtx.Lock()
	if seats, ok := fs.seats[req.Flight]; ok && seats > 0 {
		ticket := Ticket{
			ID:     req.CustomerID,
			Flight: req.Flight,
			Seat:   strconv.Itoa(seats),
			Price:  fs.prices[req.Flight],
		}
		fs.seats[req.Flight] -= 1
		fs.sold = append(fs.sold, ticket)

		return ticket, nil
	} else {
		fmt.Println("Бронирование не удалось")

		if seats == 0 {
			return Ticket{}, errors.New("на текущем рейсе нету свободных мест")
		}
		return Ticket{}, errors.New("такого рейса не существует")
	}
}

func worker(ctx context.Context, id int, queue <-chan BookingRequest, storage *FlightStorage, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("Воркер %d: завершение работы\n", id)
			return
		case req, ok := <-queue:
			if !ok {
				return
			}

			TotalRequests.Add(1)

			time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
			_, err := storage.BookSeat(req)
			if err != nil {
				FailedBookings.Add(1)
			} else {
				SuccessfulBookings.Add(1)
			}

			if req.ResultChan != nil { // в канале по умолчанию ничего нет поэтому в любом случае добавляем err
				req.ResultChan <- err
			}
		}
	}
}

func MakeBuyHandler(bookingQueue chan<- BookingRequest) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req BookingRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		req.ResultChan = make(chan error, 1)

		select {
		case bookingQueue <- req:
		default:
			http.Error(w, "сервер перегружен", http.StatusServiceUnavailable)
			return
		}

		select {
		case err := <-req.ResultChan:
			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte("Бронирование прошло успешно!"))
		case <-time.After(2 * time.Second):
			http.Error(w, "таймаут ожидания ответа", http.StatusGatewayTimeout)
		}
	}
}

func MakeBookingStatusHandler(fs *FlightStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		flightParam := r.URL.Query().Get("flight")
		if flightParam == "" {
			httpStatusResponse := fs.GetAllSeats()
			if err := json.NewEncoder(w).Encode(httpStatusResponse); err != nil {
				log.Printf("ошибка кодирования JSON ответа: %v\n", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		} else {
			httpStatusResponse := BookingStatusResponse{
				Flight:         flightParam,
				AvailableSeats: fs.CheckSeats(flightParam),
			}

			if err := json.NewEncoder(w).Encode(httpStatusResponse); err != nil {
				log.Printf("ошибка кодирования JSON ответа: %v\n", err.Error())
				w.WriteHeader(http.StatusInternalServerError)
			}
		}
	}
}

func GetMetricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metricsResponse := MetricsInfoResponse{
		TotalRequests:      TotalRequests.Load(),
		SuccessfulBookings: SuccessfulBookings.Load(),
		FailedBookings:     FailedBookings.Load(),
	}

	if err := json.NewEncoder(w).Encode(metricsResponse); err != nil {
		log.Printf("ошибка кодирования JSON ответа: %v\n", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func MakeTicketsListHandler(fs *FlightStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		flightParam := r.URL.Query().Get("flight")
		ticketResponse := TicketsResponse{Tickets: fs.sold}

		if flightParam == "" {
			if b, err := json.Marshal(ticketResponse); err != nil {
				http.Error(w, "ошибка кодирования JSON ответа", http.StatusInternalServerError)
				return
			} else {
				_, _ = w.Write(b)
			}
		} else {
			soldTickets := ticketResponse.Tickets
			for i, v := range soldTickets {
				if v.Flight != flightParam {
					soldTickets = append(soldTickets[:i], soldTickets[i+1:]...)
				}
			}

			sort.Slice(soldTickets, func(i, j int) bool {
				if soldTickets[i].Flight == flightParam {
					return true
				}

				if soldTickets[j].Flight == flightParam {
					return false
				}

				return soldTickets[i].Flight < soldTickets[j].Flight
			})

			ticketResponse.Tickets = soldTickets
			if b, err := json.Marshal(ticketResponse); err != nil {
				http.Error(w, "ошибка кодирования JSON ответа", http.StatusInternalServerError)
				return
			} else {
				_, _ = w.Write(b)
			}
		}

	}
}

func ShutDown(cancel context.CancelFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		cancel()
		_, _ = w.Write([]byte("Завершение системы инициировано"))
	}
}

func main() {
	seats := map[string]int{
		"Astana-Almaty": 15000,
		"Almaty-Astana": 95,
		"Astana-Dubai":  0,
	}

	prices := map[string]float64{
		"Astana-Almaty": 15000,
		"Almaty-Astana": 16500,
		"Astana-Dubai":  120000,
	}

	bookingQueue := make(chan BookingRequest, 100)
	flightStorage := NewFlightStorage(seats, prices)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go worker(ctx, i, bookingQueue, flightStorage, wg)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/buy", MakeBuyHandler(bookingQueue))
	mux.HandleFunc("/status", MakeBookingStatusHandler(flightStorage)) // query param `flight`
	mux.HandleFunc("/metrics", GetMetricsHandler)
	mux.HandleFunc("/tickets", MakeTicketsListHandler(flightStorage)) // query param `flight` [optional]
	mux.HandleFunc("/shutdown", ShutDown(cancel))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Printf("произошла ошибка при запуске сервера: %v\n", err.Error())
		}
	}()

	<-ctx.Done() // заблокируемся здесь пока не сработает cancel() -> нужен чтобы main понял что пора закрываться
	// а тот что в worker он чтобы горутина поняла что уже надо завершать работу свою

	fmt.Println("Начинаем завершение работы . . .")

	close(bookingQueue)
	wg.Wait()

	fmt.Printf("Итого: total=%d, success: %d, failed: %d, продано билетов: %d\n",
		TotalRequests.Load(), SuccessfulBookings.Load(), FailedBookings.Load(), len(flightStorage.sold))

	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("ошибка при остановке сервера: %v", err.Error())
	}
}

// handler + worker - канал нужен для того чтобы хендлер понимал когда завершилась именна его работа а не какая то другая
// канал нужен чтобы таскать err как статус сделал или не сделал

// mux - для создания маршрутов он без сетевой активности
// srv - для создания конфигурации будущего сервера для его запуска
