package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type PipelineNode struct {
	ID         string  `json:"id"`
	Name       string  `json:"name"`
	Type       string  `json:"type"`
	X          float64 `json:"x"`
	Y          float64 `json:"y"`
	Status     string  `json:"status"`
	Throughput float64 `json:"throughput"`
	Latency    float64 `json:"latency"`
	ErrorRate  float64 `json:"errorRate"`
	QueueDepth int     `json:"queueDepth"`
}

type PipelineEdge struct {
	ID       string  `json:"id"`
	Source   string  `json:"source"`
	Target   string  `json:"target"`
	FlowRate float64 `json:"flowRate"`
	Active   bool    `json:"active"`
}

type DataPacket struct {
	ID       string  `json:"id"`
	EdgeID   string  `json:"edgeId"`
	Progress float64 `json:"progress"`
	Size     float64 `json:"size"`
	Type     string  `json:"packetType"`
}

type PipelineState struct {
	Nodes      []PipelineNode `json:"nodes"`
	Edges      []PipelineEdge `json:"edges"`
	Packets    []DataPacket   `json:"packets"`
	Timestamp  int64          `json:"timestamp"`
	TotalFlow  float64        `json:"totalFlow"`
	Throughput float64        `json:"throughput"`
	Errors     int            `json:"errors"`
	Uptime     float64        `json:"uptime"`
}

type Hub struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

func newHub() *Hub {
	return &Hub{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *Hub) run() {
	for {
		select {
		case conn := <-h.register:
			h.mu.Lock()
			h.clients[conn] = true
			h.mu.Unlock()
			log.Printf("Client connected. Total: %d", len(h.clients))
		case conn := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, conn)
			h.mu.Unlock()
			_ = conn.Close()
			log.Printf("Client disconnected. Total: %d", len(h.clients))
		case msg := <-h.broadcast:
			h.mu.Lock()
			for conn := range h.clients {
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					delete(h.clients, conn)
					_ = conn.Close()
				}
			}
			h.mu.Unlock()
		}
	}
}

type StateStore struct {
	mu    sync.RWMutex
	state PipelineState
}

func (s *StateStore) Set(state PipelineState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
}

func (s *StateStore) Get() PipelineState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

var nodes = []PipelineNode{
	{ID: "source-1", Name: "Kafka Stream", Type: "source", X: 80, Y: 70, Status: "active"},
	{ID: "source-2", Name: "Event Bus", Type: "source", X: 80, Y: 270, Status: "active"},
	{ID: "transform-1", Name: "Parser", Type: "transform", X: 300, Y: 20, Status: "active"},
	{ID: "transform-2", Name: "Enricher", Type: "transform", X: 300, Y: 170, Status: "active"},
	{ID: "transform-3", Name: "Validator", Type: "transform", X: 300, Y: 320, Status: "active"},
	{ID: "aggregate-1", Name: "Aggregator", Type: "aggregate", X: 520, Y: 90, Status: "active"},
	{ID: "aggregate-2", Name: "Joiner", Type: "aggregate", X: 520, Y: 250, Status: "active"},
	{ID: "sink-1", Name: "PostgreSQL", Type: "sink", X: 740, Y: 20, Status: "active"},
	{ID: "sink-2", Name: "Redis Cache", Type: "sink", X: 740, Y: 170, Status: "active"},
	{ID: "sink-3", Name: "S3 Archive", Type: "sink", X: 740, Y: 320, Status: "active"},
}

var edges = []PipelineEdge{
	{ID: "e1", Source: "source-1", Target: "transform-1", Active: true},
	{ID: "e2", Source: "source-1", Target: "transform-2", Active: true},
	{ID: "e3", Source: "source-2", Target: "transform-2", Active: true},
	{ID: "e4", Source: "source-2", Target: "transform-3", Active: true},
	{ID: "e5", Source: "transform-1", Target: "aggregate-1", Active: true},
	{ID: "e6", Source: "transform-2", Target: "aggregate-1", Active: true},
	{ID: "e7", Source: "transform-2", Target: "aggregate-2", Active: true},
	{ID: "e8", Source: "transform-3", Target: "aggregate-2", Active: true},
	{ID: "e9", Source: "aggregate-1", Target: "sink-1", Active: true},
	{ID: "e10", Source: "aggregate-1", Target: "sink-2", Active: true},
	{ID: "e11", Source: "aggregate-2", Target: "sink-2", Active: true},
	{ID: "e12", Source: "aggregate-2", Target: "sink-3", Active: true},
}

var packets []DataPacket
var packetCounter int
var totalErrors int
var startTime = time.Now()

func simulatePipeline() PipelineState {
	t := float64(time.Now().UnixNano()) / 1e9

	updatedNodes := make([]PipelineNode, len(nodes))
	for i, n := range nodes {
		n.Throughput = 800 + 400*math.Sin(t*0.3+float64(i)*0.7) + rand.Float64()*200
		n.Latency = 5 + 15*math.Abs(math.Sin(t*0.2+float64(i)*0.5)) + rand.Float64()*5
		n.ErrorRate = math.Max(0, 0.5*math.Sin(t*0.1+float64(i)*1.2)+0.3+rand.Float64()*0.3)
		n.QueueDepth = int(50 + 30*math.Sin(t*0.4+float64(i)*0.3) + rand.Float64()*20)

		if n.ErrorRate > 1.8 {
			n.Status = "error"
			totalErrors++
		} else if n.ErrorRate > 1.2 {
			n.Status = "warning"
			totalErrors++
		} else {
			n.Status = "active"
		}
		updatedNodes[i] = n
	}

	updatedEdges := make([]PipelineEdge, len(edges))
	for i, e := range edges {
		e.FlowRate = 500 + 300*math.Sin(t*0.25+float64(i)*0.6) + rand.Float64()*150
		updatedEdges[i] = e
	}

	if rand.Float64() < 0.4 {
		edgeIdx := rand.Intn(len(edges))
		packetCounter++
		ptype := "data"
		if rand.Float64() < 0.1 {
			ptype = "error"
		} else if rand.Float64() < 0.2 {
			ptype = "control"
		}
		packets = append(packets, DataPacket{
			ID:       fmt.Sprintf("pkt-%d", packetCounter),
			EdgeID:   edges[edgeIdx].ID,
			Progress: 0,
			Size:     0.3 + rand.Float64()*0.7,
			Type:     ptype,
		})
	}

	alive := packets[:0]
	for _, p := range packets {
		p.Progress += 0.03 + rand.Float64()*0.02
		if p.Progress < 1.0 {
			alive = append(alive, p)
		}
	}
	packets = alive
	if len(packets) > 60 {
		packets = packets[len(packets)-60:]
	}

	totalFlow := 0.0
	for _, e := range updatedEdges {
		totalFlow += e.FlowRate
	}

	return PipelineState{
		Nodes:      updatedNodes,
		Edges:      updatedEdges,
		Packets:    packets,
		Timestamp:  time.Now().UnixMilli(),
		TotalFlow:  totalFlow,
		Throughput: totalFlow / float64(len(updatedEdges)),
		Errors:     totalErrors,
		Uptime:     time.Since(startTime).Seconds(),
	}
}

func withCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		handler(w, r)
	}
}

func wsHandler(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	hub.register <- conn

	go func() {
		defer func() { hub.unregister <- conn }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	}()
}

func main() {
	rand.Seed(time.Now().UnixNano())
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	hub := newHub()
	store := &StateStore{}
	go hub.run()

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			state := simulatePipeline()
			store.Set(state)
			data, err := json.Marshal(state)
			if err == nil {
				select {
				case hub.broadcast <- data:
				default:
				}
			}
		}
	}()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(hub, w, r)
	})

	http.HandleFunc("/api/health", withCORS(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))

	http.HandleFunc("/api/state", withCORS(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.Get())
	}))

	addr := ":" + port
	log.Printf("Pipeline backend running on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
