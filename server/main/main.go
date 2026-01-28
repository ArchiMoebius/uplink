package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	pb "github.com/ArchiMoebius/uplink/pkg/gen/v1"
	"github.com/ArchiMoebius/uplink/server/handler"
)

//go:embed static
var staticFiles embed.FS

var (
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all origins for development
		},
	}

	// PubSub for new event notifications
	subscribers = make(map[*websocket.Conn]bool)
	subMutex    sync.RWMutex
)

type Server struct {
	db           *gorm.DB
	eventHandler *handler.SSHEventHandler
	geoIP        *GeoIPService
	pb.UnimplementedTransporterServer
}

// API Response structures
type ServiceResponse struct {
	ID        uint      `json:"id"`
	UUID      string    `json:"uuid"`
	CreatedAt time.Time `json:"created_at"`
}

type HeatmapDataPoint struct {
	Username string `json:"username"`
	SourceIP string `json:"source_ip"`
	Count    int64  `json:"count"`
}

type SankeyDataPoint struct {
	Source string `json:"source"`
	Middle string `json:"middle"`
	Target string `json:"target"`
	Count  int64  `json:"count"`
}

type BubbleMapDataPoint struct {
	Username  string  `json:"username"`
	Password  string  `json:"password"`
	SourceIP  string  `json:"source_ip"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Count     int64   `json:"count"`
}

func NewServer(db *gorm.DB, eventHandler *handler.SSHEventHandler, geoIP *GeoIPService) *Server {
	return &Server{
		db:           db,
		eventHandler: eventHandler,
		geoIP:        geoIP,
	}
}

// gRPC: Beam - Receive SSH events via streaming RPC
func (s *Server) Beam(stream pb.Transporter_BeamServer) error {
	ctx := stream.Context()

	// Notify handler of stream start
	if err := s.eventHandler.OnStreamStart(ctx); err != nil {
		log.Printf("Error on stream start: %v", err)
	}

	log.Println("New gRPC stream established")

	defer func() {
		if err := s.eventHandler.OnStreamEnd(ctx); err != nil {
			log.Printf("Error on stream end: %v", err)
		}
		log.Println("gRPC stream ended")
	}()

	for {
		event, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				log.Println("Stream completed normally")
				return nil
			}
			log.Printf("Stream receive error: %v", err)
			return err
		}

		// Handle the event
		if err := s.eventHandler.Handle(ctx, event); err != nil {
			log.Printf("Error handling event: %v", err)
			continue
		}
	}
}

// Subscribe new WebSocket connection
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	subMutex.Lock()
	subscribers[conn] = true
	subMutex.Unlock()

	log.Printf("New WebSocket connection established. Total subscribers: %d", len(subscribers))

	// Send initial connection confirmation
	conn.WriteJSON(map[string]interface{}{
		"type":    "connected",
		"message": "WebSocket connection established",
		"time":    time.Now(),
	})

	// Keep connection alive and handle client messages
	defer func() {
		subMutex.Lock()
		delete(subscribers, conn)
		subMutex.Unlock()
		conn.Close()
		log.Printf("WebSocket connection closed. Remaining subscribers: %d", len(subscribers))
	}()

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// Notify all subscribers of new data
func NotifyNewEvent(serviceUUID string) {
	subMutex.RLock()
	defer subMutex.RUnlock()

	message := map[string]interface{}{
		"type":         "new_event",
		"service_uuid": serviceUUID,
		"timestamp":    time.Now(),
	}

	for conn := range subscribers {
		err := conn.WriteJSON(message)
		if err != nil {
			log.Printf("Error sending to subscriber: %v", err)
		}
	}
}

// GET /api/services - List all services
func (s *Server) getServices(w http.ResponseWriter, r *http.Request) {
	var services []handler.Service

	result := s.db.Order("created_at DESC").Find(&services)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	response := make([]ServiceResponse, len(services))
	for i, svc := range services {
		response[i] = ServiceResponse{
			ID:        svc.ID,
			UUID:      svc.UUID,
			CreatedAt: svc.CreatedAt,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// GET /api/heatmap/{service_uuid}?hours=24
func (s *Server) getHeatmapData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceUUID := vars["service_uuid"]

	// Parse time range query parameter (default 24 hours)
	hours := 24
	if hoursParam := r.URL.Query().Get("hours"); hoursParam != "" {
		var err error
		if _, err = fmt.Sscanf(hoursParam, "%d", &hours); err != nil {
			hours = 24
		}
	}

	// Parse axis parameters (default: username x source_ip)
	xAxis := r.URL.Query().Get("x_axis")
	yAxis := r.URL.Query().Get("y_axis")

	if xAxis == "" {
		xAxis = "source_ip"
	}
	if yAxis == "" {
		yAxis = "username"
	}

	timeRange := time.Now().Add(-time.Duration(hours) * time.Hour)

	// Build dynamic query based on selected axes
	query := s.buildHeatmapQuery(xAxis, yAxis)

	// Query for combinations with counts
	var results []HeatmapDataPoint

	result := s.db.Raw(query, serviceUUID, timeRange).Scan(&results)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// GET /api/bubblemap/{service_uuid}?hours=24
func (s *Server) getBubbleMapData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceUUID := vars["service_uuid"]

	// Parse time range query parameter (default 24 hours)
	hours := 24
	if hoursParam := r.URL.Query().Get("hours"); hoursParam != "" {
		var err error
		if _, err = fmt.Sscanf(hoursParam, "%d", &hours); err != nil {
			hours = 24
		}
	}

	timeRange := time.Now().Add(-time.Duration(hours) * time.Hour)

	// Query for username/password/IP combinations with counts
	type QueryResult struct {
		Username string
		Password string
		SourceIP string
		Count    int64
	}

	var queryResults []QueryResult

	query := `
		SELECT 
			COALESCE(u.username, 'anonymous') as username,
			COALESCE(pwd.password, '[none]') as password,
			ip.address as source_ip,
			COUNT(*) as count
		FROM ssh_connection_events e
		INNER JOIN services s ON s.id = e.service_id
		INNER JOIN ip_addresses ip ON ip.id = e.source_ip_id
		LEFT JOIN usernames u ON u.id = e.username_id
		LEFT JOIN passwords pwd ON pwd.id = e.password_id
		WHERE s.uuid = ?
		AND e.timestamp >= ?
		GROUP BY username, password, source_ip
		ORDER BY count DESC
	`

	result := s.db.Raw(query, serviceUUID, timeRange).Scan(&queryResults)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	// Convert to bubble map data points with geolocation
	var results []BubbleMapDataPoint
	for _, qr := range queryResults {
		loc := s.geoIP.Lookup(qr.SourceIP)
		results = append(results, BubbleMapDataPoint{
			Username:  qr.Username,
			Password:  qr.Password,
			SourceIP:  qr.SourceIP,
			Latitude:  loc.Latitude,
			Longitude: loc.Longitude,
			Count:     qr.Count,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// GET /api/sankey/{service_uuid}?hours=24&source=source_ip&middle=username&target=password
func (s *Server) getSankeyData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceUUID := vars["service_uuid"]

	// Parse time range query parameter (default 24 hours)
	hours := 24
	if hoursParam := r.URL.Query().Get("hours"); hoursParam != "" {
		var err error
		if _, err = fmt.Sscanf(hoursParam, "%d", &hours); err != nil {
			hours = 24
		}
	}

	// Parse field parameters
	sourceField := r.URL.Query().Get("source")
	middleField := r.URL.Query().Get("middle")
	targetField := r.URL.Query().Get("target")

	if sourceField == "" {
		sourceField = "source_ip"
	}
	if middleField == "" {
		middleField = "username"
	}
	if targetField == "" {
		targetField = "password"
	}

	timeRange := time.Now().Add(-time.Duration(hours) * time.Hour)

	// Build the query
	query := s.buildSankeyQuery(sourceField, middleField, targetField)

	var results []SankeyDataPoint

	result := s.db.Raw(query, serviceUUID, timeRange).Scan(&results)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// Build dynamic SQL query for Sankey diagram
func (s *Server) buildSankeyQuery(sourceField, middleField, targetField string) string {
	sourceSQL := s.getFieldSQL(sourceField, "source")
	middleSQL := s.getFieldSQL(middleField, "middle")
	targetSQL := s.getFieldSQL(targetField, "target")

	// Collect all required joins
	joins := make(map[string]string)

	// Always need IP addresses
	joins["ip"] = "INNER JOIN ip_addresses ip ON ip.id = e.source_ip_id"

	// Add joins for each field
	for table, join := range s.getJoinsForField(sourceField, "source") {
		joins[table] = join
	}
	for table, join := range s.getJoinsForField(middleField, "middle") {
		joins[table] = join
	}
	for table, join := range s.getJoinsForField(targetField, "target") {
		joins[table] = join
	}

	// Build join clause
	joinClause := ""
	for _, join := range joins {
		joinClause += join + "\n"
	}

	query := fmt.Sprintf(`
		SELECT 
			%s as source,
			%s as middle,
			%s as target,
			COUNT(*) as count
		FROM ssh_connection_events e
		INNER JOIN services s ON s.id = e.service_id
		%s
		WHERE s.uuid = ?
		AND e.timestamp >= ?
		GROUP BY source, middle, target
		HAVING count > 0
		ORDER BY count DESC
		LIMIT 100
	`, sourceSQL, middleSQL, targetSQL, joinClause)

	return query
}

// Build dynamic SQL query based on axis selections
func (s *Server) buildHeatmapQuery(xAxis, yAxis string) string {
	// Special handling for auth_methods - requires completely different query structure
	if xAxis == "auth_methods" || yAxis == "auth_methods" {
		return s.buildAuthMethodsHeatmapQuery(xAxis, yAxis)
	}

	xField := s.getFieldSQL(xAxis, "x")
	yField := s.getFieldSQL(yAxis, "y")
	xJoins := s.getJoinsForField(xAxis, "x")
	yJoins := s.getJoinsForField(yAxis, "y")

	// Combine joins (deduplicate)
	joins := xJoins
	for table, join := range yJoins {
		if _, exists := joins[table]; !exists {
			joins[table] = join
		}
	}

	// Build join clause
	joinClause := ""
	for _, join := range joins {
		joinClause += join + "\n"
	}

	query := fmt.Sprintf(`
		SELECT 
			%s as username,
			%s as source_ip,
			COUNT(*) as count
		FROM ssh_connection_events e
		INNER JOIN services s ON s.id = e.service_id
		%s
		WHERE s.uuid = ?
		AND e.timestamp >= ?
		GROUP BY username, source_ip
		ORDER BY count DESC
	`, yField, xField, joinClause)

	return query
}

// Build special query for auth_methods heatmap
func (s *Server) buildAuthMethodsHeatmapQuery(xAxis, yAxis string) string {
	var otherAxis string
	var authIsX bool

	if xAxis == "auth_methods" {
		otherAxis = yAxis
		authIsX = true
	} else {
		otherAxis = xAxis
		authIsX = false
	}

	otherField := s.getFieldSQL(otherAxis, "other")
	otherJoins := s.getJoinsForField(otherAxis, "other")

	// Build join clause
	joinClause := ""
	for _, join := range otherJoins {
		joinClause += join + "\n"
	}

	var query string
	if authIsX {
		// Auth methods on X-axis (source_ip), other field on Y-axis (username)
		query = fmt.Sprintf(`
			SELECT 
				%s as username,
				COALESCE(am.method_name, 'none') as source_ip,
				COUNT(*) as count
			FROM ssh_connection_events e
			INNER JOIN services s ON s.id = e.service_id
			%s
			LEFT JOIN ssh_event_auth_methods eam ON eam.event_id = e.id
			LEFT JOIN auth_methods am ON am.id = eam.auth_method_id
			WHERE s.uuid = ?
			AND e.timestamp >= ?
			GROUP BY username, source_ip
			ORDER BY count DESC
		`, otherField, joinClause)
	} else {
		// Auth methods on Y-axis (username), other field on X-axis (source_ip)
		query = fmt.Sprintf(`
			SELECT 
				COALESCE(am.method_name, 'none') as username,
				%s as source_ip,
				COUNT(*) as count
			FROM ssh_connection_events e
			INNER JOIN services s ON s.id = e.service_id
			%s
			LEFT JOIN ssh_event_auth_methods eam ON eam.event_id = e.id
			LEFT JOIN auth_methods am ON am.id = eam.auth_method_id
			WHERE s.uuid = ?
			AND e.timestamp >= ?
			GROUP BY username, source_ip
			ORDER BY count DESC
		`, otherField, joinClause)
	}

	return query
}

// Get GROUP BY field (for aggregated fields like auth_methods, we group by event ID first)
func (s *Server) getGroupByField(field string, alias string) string {
	switch field {
	case "auth_methods":
		return "e.id"
	default:
		return s.getFieldSQL(field, alias)
	}
}

// Get SQL expression for a field
func (s *Server) getFieldSQL(field string, alias string) string {
	switch field {
	case "username":
		return "COALESCE(u.username, 'anonymous')"
	case "source_ip":
		return "ip.address"
	case "source_port":
		return "CAST(e.source_port AS TEXT)"
	case "hassh":
		return "hassh.fingerprint"
	case "password":
		return "COALESCE(pwd.password, '[no password]')"
	default:
		return "'unknown'"
	}
}

// Get required joins for a field
func (s *Server) getJoinsForField(field string, alias string) map[string]string {
	joins := make(map[string]string)

	joins["ip"] = "INNER JOIN ip_addresses ip ON ip.id = e.source_ip_id"

	switch field {
	case "username":
		joins["u"] = "LEFT JOIN usernames u ON u.id = e.username_id"
	case "hassh":
		joins["hassh"] = "INNER JOIN ha_ssh_fingerprints hassh ON hassh.id = e.ha_ssh_fingerprint_id"
	case "password":
		joins["pwd"] = "LEFT JOIN passwords pwd ON pwd.id = e.password_id"
	}

	return joins
}

// GET /api/stats/{service_uuid}?hours=24 - General statistics
func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	serviceUUID := vars["service_uuid"]

	hours := 24
	if hoursParam := r.URL.Query().Get("hours"); hoursParam != "" {
		fmt.Sscanf(hoursParam, "%d", &hours)
	}

	timeRange := time.Now().Add(-time.Duration(hours) * time.Hour)

	type Stats struct {
		TotalEvents        int64 `json:"total_events"`
		UniqueIPs          int64 `json:"unique_ips"`
		UniqueUsernames    int64 `json:"unique_usernames"`
		UniqueFingerprints int64 `json:"unique_fingerprints"`
	}

	var stats Stats

	s.db.Table("ssh_connection_events").
		Joins("INNER JOIN services ON services.id = ssh_connection_events.service_id").
		Where("services.uuid = ? AND ssh_connection_events.timestamp >= ?", serviceUUID, timeRange).
		Count(&stats.TotalEvents)

	s.db.Table("ssh_connection_events").
		Select("DISTINCT source_ip_id").
		Joins("INNER JOIN services ON services.id = ssh_connection_events.service_id").
		Where("services.uuid = ? AND ssh_connection_events.timestamp >= ?", serviceUUID, timeRange).
		Count(&stats.UniqueIPs)

	s.db.Table("ssh_connection_events").
		Select("DISTINCT username_id").
		Joins("INNER JOIN services ON services.id = ssh_connection_events.service_id").
		Where("services.uuid = ? AND ssh_connection_events.timestamp >= ? AND username_id IS NOT NULL", serviceUUID, timeRange).
		Count(&stats.UniqueUsernames)

	s.db.Table("ssh_connection_events").
		Select("DISTINCT ha_ssh_fingerprint_id").
		Joins("INNER JOIN services ON services.id = ssh_connection_events.service_id").
		Where("services.uuid = ? AND ssh_connection_events.timestamp >= ?", serviceUUID, timeRange).
		Count(&stats.UniqueFingerprints)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

func enableCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	db, err := gorm.Open(sqlite.Open("ssh_events.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Initialize GeoIP service - download from: https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
	geoIP, err := NewGeoIPService("GeoLite2-City.mmdb")
	if err != nil {
		log.Printf("GeoIP initialization warning: %v", err)
		log.Println("Continuing with mock GeoIP data")
	}
	defer geoIP.Close()

	eventHandler, err := handler.NewSSHEventHandlerWithCallback(db, NotifyNewEvent)
	if err != nil {
		log.Fatalf("Failed to initialize handler: %v", err)
	}

	server := NewServer(db, eventHandler, geoIP)

	go func() {
		lis, err := net.Listen("tcp", ":50051")
		if err != nil {
			log.Fatalf("Failed to listen on gRPC port: %v", err)
		}

		// Configure keepalive enforcement
		kaep := keepalive.EnforcementPolicy{
			MinTime:             5 * time.Minute,
			PermitWithoutStream: true,
		}

		kasp := keepalive.ServerParameters{
			MaxConnectionIdle:     45 * time.Second,
			MaxConnectionAge:      30 * time.Second,
			MaxConnectionAgeGrace: 30 * time.Second,
			Time:                  30 * time.Second,
			Timeout:               10 * time.Second,
		}

		grpcServer := grpc.NewServer(
			grpc.KeepaliveEnforcementPolicy(kaep),
			grpc.KeepaliveParams(kasp),
		)
		pb.RegisterTransporterServer(grpcServer, server)

		log.Println("gRPC server listening on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	r := mux.NewRouter()

	r.HandleFunc("/ws", server.handleWebSocket)

	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/services", server.getServices).Methods("GET")
	api.HandleFunc("/heatmap/{service_uuid}", server.getHeatmapData).Methods("GET")
	api.HandleFunc("/sankey/{service_uuid}", server.getSankeyData).Methods("GET")
	api.HandleFunc("/bubblemap/{service_uuid}", server.getBubbleMapData).Methods("GET")
	api.HandleFunc("/stats/{service_uuid}", server.getStats).Methods("GET")

	// Serve embedded static files
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatalf("Failed to create sub filesystem: %v", err)
	}
	r.PathPrefix("/").Handler(http.FileServer(http.FS(staticFS)))

	httpHandler := enableCORS(r)

	log.Println("HTTP server starting on :8080")
	log.Println("gRPC server listening on :50051")
	log.Println("WebSocket endpoint: ws://localhost:8080/ws")
	log.Println("Dashboard: http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", httpHandler))
}
