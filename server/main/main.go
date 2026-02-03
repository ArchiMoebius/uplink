package main

import (
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

const (
	mirrorBasePath = "/var/log/fishler/fishyfs/mirror"
	maxFileSize    = 100 * 1024 * 1024 // 100MB
)

// FileInfo represents a file or directory in the mirror
type MirrorFileInfo struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	IsDir   bool   `json:"is_dir"`
	Size    int64  `json:"size"`
	ModTime string `json:"mod_time"`
}

// DirectoryListing represents the contents of a directory
type MirrorDirectoryListing struct {
	Path    string           `json:"path"`
	Entries []MirrorFileInfo `json:"entries"`
}

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
	Username  string `json:"username"`
	SourceIP  string `json:"source_ip"`
	Count     int64  `json:"count"`
	SessionID string `json:"session_id"`
}

type SankeyDataPoint struct {
	Source    string `json:"source"`
	Middle    string `json:"middle"`
	Target    string `json:"target"`
	Count     int64  `json:"count"`
	SessionID string `json:"session_id"`
}

type BubbleMapDataPoint struct {
	Username  string  `json:"username"`
	Password  string  `json:"password"`
	SourceIP  string  `json:"source_ip"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Count     int64   `json:"count"`
	SessionID string  `json:"session_id"`
}

type EventDataPoint struct {
	ID            uint      `json:"id"`
	Timestamp     time.Time `json:"timestamp"`
	SourceIP      string    `json:"source_ip"`
	SourcePort    int       `json:"source_port"`
	Username      string    `json:"username"`
	Password      string    `json:"password"`
	SSHClientName string    `json:"ssh_client_name"`
	HASSH         string    `json:"hassh"`
	AuthMethods   string    `json:"auth_methods"`
	Country       string    `json:"country"`
	City          string    `json:"city"`
	SessionID     string    `json:"session_id"`
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

// GET /api/events/{service_uuid}?hours=24&limit=1000 - Get raw events for table view
func (s *Server) getEvents(w http.ResponseWriter, r *http.Request) {
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

	// Parse limit parameter (default 1000, max 10000)
	limit := 1000
	if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
		var err error
		if _, err = fmt.Sscanf(limitParam, "%d", &limit); err != nil {
			limit = 1000
		}
	}
	if limit > 10000 {
		limit = 10000
	}

	timeRange := time.Now().Add(-time.Duration(hours) * time.Hour)

	// Query for raw events with all relevant fields
	type QueryResult struct {
		ID            uint      `gorm:"column:id"`
		Timestamp     time.Time `gorm:"column:timestamp"`
		SourceIP      string    `gorm:"column:source_ip"`
		SourcePort    int       `gorm:"column:source_port"`
		Username      *string   `gorm:"column:username"`
		Password      *string   `gorm:"column:password"`
		SSHClientName *string   `gorm:"column:ssh_client_name"`
		HASSH         string    `gorm:"column:hassh"`
		SessionID     string    `gorm:"column:session_id"`
	}

	var queryResults []QueryResult

	query := `
		SELECT 
			e.id,
			e.timestamp,
			e.session_id,
			ip.address as source_ip,
			e.source_port,
			u.username,
			pwd.password,
			scn.value as ssh_client_name,
			COALESCE(ha.fingerprint, '') as hassh
		FROM ssh_connection_events e
		INNER JOIN services s ON s.id = e.service_id
		INNER JOIN ip_addresses ip ON ip.id = e.source_ip_id
		LEFT JOIN usernames u ON u.id = e.username_id
		LEFT JOIN passwords pwd ON pwd.id = e.password_id
		LEFT JOIN ssh_client_names scn ON scn.id = e.ssh_client_name_id
		LEFT JOIN ha_ssh_fingerprints ha ON ha.id = e.ha_ssh_fingerprint_id
		WHERE s.uuid = ?
		AND e.timestamp >= ?
		ORDER BY e.timestamp DESC
		LIMIT ?
	`

	result := s.db.Raw(query, serviceUUID, timeRange, limit).Scan(&queryResults)
	if result.Error != nil {
		http.Error(w, result.Error.Error(), http.StatusInternalServerError)
		return
	}

	// Get auth methods for each event (many-to-many relationship)
	eventIDs := make([]uint, len(queryResults))
	for i, qr := range queryResults {
		eventIDs[i] = qr.ID
	}

	type AuthMethodResult struct {
		EventID    uint
		MethodName string
	}

	var authMethods []AuthMethodResult
	if len(eventIDs) > 0 {
		authQuery := `
			SELECT 
				eam.event_id,
				am.method_name
			FROM ssh_event_auth_methods eam
			INNER JOIN auth_methods am ON am.id = eam.auth_method_id
			WHERE eam.event_id IN (?)
			ORDER BY eam.event_id, am.method_name
		`
		s.db.Raw(authQuery, eventIDs).Scan(&authMethods)
	}

	// Build auth methods map
	authMethodsMap := make(map[uint][]string)
	for _, am := range authMethods {
		authMethodsMap[am.EventID] = append(authMethodsMap[am.EventID], am.MethodName)
	}

	// Convert to event data points
	var events []EventDataPoint
	for _, qr := range queryResults {
		hassh := qr.HASSH
		if hassh == "" {
			hassh = "unknown"
		}

		event := EventDataPoint{
			ID:            qr.ID,
			Timestamp:     qr.Timestamp,
			SourceIP:      qr.SourceIP,
			SourcePort:    qr.SourcePort,
			Username:      stringOrDefault(qr.Username, "anonymous"),
			Password:      stringOrDefault(qr.Password, "[none]"),
			SSHClientName: stringOrDefault(qr.SSHClientName, "unknown"),
			HASSH:         hassh,
			SessionID:     qr.SessionID,
		}

		// Add auth methods
		if methods, ok := authMethodsMap[qr.ID]; ok && len(methods) > 0 {
			authMethodsStr := ""
			for i, method := range methods {
				if i > 0 {
					authMethodsStr += ", "
				}
				authMethodsStr += method
			}
			event.AuthMethods = authMethodsStr
		} else {
			event.AuthMethods = "none"
		}

		// Add geolocation if available
		if s.geoIP != nil {
			loc := s.geoIP.Lookup(qr.SourceIP)
			event.Country = loc.Country
			event.City = loc.City
		}

		events = append(events, event)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// Helper function to handle nullable strings
func stringOrDefault(value *string, defaultValue string) string {
	if value != nil && *value != "" {
		return *value
	}
	return defaultValue
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
		Username  string
		Password  string
		SourceIP  string
		Count     int64
		SessionID string
	}

	var queryResults []QueryResult

	query := `
		SELECT 
		    e.session_id,
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
			SessionID: qr.SessionID,
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
		    e.session_id,
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
		    e.session_id,
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
			    e.session_id,
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
			    e.session_id,
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
		return "COALESCE(usernames.username, 'anonymous')"
	case "source_ip":
		return "ip.address"
	case "source_port":
		return "CAST(e.source_port AS TEXT)"
	case "hassh":
		return "ha_ssh_fingerprints.fingerprint"
	case "ssh_client_name":
		return "ssh_client_names.value"
	case "password":
		return "COALESCE(passwords.password, '[no password]')"
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
		joins["u"] = "LEFT JOIN usernames ON usernames.id = e.username_id"
	case "hassh":
		joins["hassh"] = "INNER JOIN ha_ssh_fingerprints ON ha_ssh_fingerprints.id = e.ha_ssh_fingerprint_id"
	case "ssh_client_name":
		joins["ssh_client_name"] = "INNER JOIN ssh_client_names ON ssh_client_names.id = e.ssh_client_name_id"
	case "password":
		joins["pwd"] = "LEFT JOIN passwords ON passwords.id = e.password_id"
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

// GET /api/session/{session_id}/content - Get raw log content
func (s *Server) getSessionLogContent(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	sessionID := vars["session_id"]

	// Validate session_id format (64 character hex string)
	if len(sessionID) != 64 {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Validate hex string using standard library
	if _, err := hex.DecodeString(sessionID); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Construct path safely
	logPath := filepath.Join("/var/log/fishler/session", sessionID+".log")

	// Verify resolved path is within allowed directory (prevents symlink attacks)
	realPath, err := filepath.EvalSymlinks(logPath)
	if err != nil || !strings.HasPrefix(realPath, "/var/log/fishler/session/") {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	// Open file for streaming
	file, err := os.Open(realPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Session not found", http.StatusNotFound)
		} else {
			log.Printf("Error opening session log: %v", err)
			http.Error(w, "Session not found", http.StatusNotFound)
		}
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("Error getting file info: %v", err)
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}

	if fileInfo.Size() > maxFileSize {
		http.Error(w, "Log file too large", http.StatusRequestEntityTooLarge)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")

	// Stream the file with http.ServeContent
	// This handles:
	// - Streaming in chunks (constant memory usage)
	// - Range requests (partial content / resume downloads)
	// - ETag generation
	// - Last-Modified headers
	// - Content-Length headers
	// - If-Modified-Since / If-None-Match conditional requests
	http.ServeContent(w, r, fileInfo.Name(), fileInfo.ModTime(), file)
}

// GET /api/mirror/browse?path=<path> - Browse directory contents (JSON)
func (s *Server) browseMirrorDirectory(w http.ResponseWriter, r *http.Request) {
	// Get the requested path (relative to mirror base)
	requestedPath := r.URL.Query().Get("path")
	if requestedPath == "" {
		requestedPath = "."
	}

	// Sanitize and validate path
	cleanPath := filepath.Clean(requestedPath)

	// Prevent path traversal attacks
	if strings.Contains(cleanPath, "..") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Construct full path
	fullPath := filepath.Join(mirrorBasePath, cleanPath)

	// Verify the resolved path is within mirror directory
	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		// If symlink resolution fails, use the cleaned path for checking
		realPath = fullPath
	}

	// Ensure the path is within the mirror base directory
	realBase, err := filepath.EvalSymlinks(mirrorBasePath)
	if err != nil {
		realBase = mirrorBasePath
	}

	if !strings.HasPrefix(realPath, realBase) {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// Check if path exists and is a directory
	fileInfo, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Directory not found", http.StatusNotFound)
		} else {
			http.Error(w, "Error accessing directory", http.StatusInternalServerError)
		}
		return
	}

	if !fileInfo.IsDir() {
		http.Error(w, "Path is not a directory", http.StatusBadRequest)
		return
	}

	// Read directory contents
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		http.Error(w, "Error reading directory", http.StatusInternalServerError)
		return
	}

	// Build response
	listing := MirrorDirectoryListing{
		Path:    cleanPath,
		Entries: make([]MirrorFileInfo, 0, len(entries)),
	}

	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue // Skip entries we can't read
		}

		var relativePath string
		if cleanPath == "." {
			relativePath = entry.Name()
		} else {
			relativePath = filepath.Join(cleanPath, entry.Name())
		}

		fileInfo := MirrorFileInfo{
			Name:    entry.Name(),
			Path:    relativePath,
			IsDir:   entry.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime().Format("2006-01-02 15:04"),
		}

		listing.Entries = append(listing.Entries, fileInfo)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(listing)
}

// GET /api/mirror/download?path=<path> - Download a file
func (s *Server) downloadMirrorFile(w http.ResponseWriter, r *http.Request) {
	// Get the requested file path (relative to mirror base)
	requestedPath := r.URL.Query().Get("path")
	if requestedPath == "" {
		http.Error(w, "Path parameter required", http.StatusBadRequest)
		return
	}

	// Sanitize and validate path
	cleanPath := filepath.Clean(requestedPath)

	// Prevent path traversal attacks
	if strings.Contains(cleanPath, "..") {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	// Construct full path
	fullPath := filepath.Join(mirrorBasePath, cleanPath)

	// Verify the resolved path is within mirror directory
	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		// If symlink resolution fails, use the cleaned path for checking
		realPath = fullPath
	}

	// Ensure the path is within the mirror base directory
	realBase, err := filepath.EvalSymlinks(mirrorBasePath)
	if err != nil {
		realBase = mirrorBasePath
	}

	if !strings.HasPrefix(realPath, realBase) {
		http.Error(w, "Access denied", http.StatusForbidden)
		return
	}

	// Open file for streaming
	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
		} else {
			http.Error(w, "Error accessing file", http.StatusInternalServerError)
		}
		return
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		http.Error(w, "Error accessing file", http.StatusInternalServerError)
		return
	}

	// Don't allow downloading directories
	if fileInfo.IsDir() {
		http.Error(w, "Cannot download directory", http.StatusBadRequest)
		return
	}

	// Check file size
	if fileInfo.Size() > maxFileSize {
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Determine content type
	contentType := "application/octet-stream"

	// Try to detect content type from extension
	ext := strings.ToLower(filepath.Ext(fileInfo.Name()))
	switch ext {
	case ".txt", ".log":
		contentType = "text/plain; charset=utf-8"
	case ".json":
		contentType = "application/json"
	case ".xml":
		contentType = "application/xml"
	case ".html", ".htm":
		contentType = "text/html; charset=utf-8"
	case ".csv":
		contentType = "text/csv"
	case ".db", ".sqlite", ".sqlite3":
		contentType = "application/x-sqlite3"
	}

	// Set headers for download
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileInfo.Name()))
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")

	// Stream the file
	http.ServeContent(w, r, fileInfo.Name(), fileInfo.ModTime(), file)
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
		lis, err := net.Listen("tcp", "127.0.0.1:50051")
		if err != nil {
			log.Fatalf("Failed to listen on gRPC port: %v", err)
		}

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

		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Failed to serve gRPC: %v", err)
		}
	}()

	r := mux.NewRouter()

	r.HandleFunc("/ws", server.handleWebSocket)

	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/services", server.getServices).Methods("GET")
	api.HandleFunc("/events/{service_uuid}", server.getEvents).Methods("GET")
	api.HandleFunc("/heatmap/{service_uuid}", server.getHeatmapData).Methods("GET")
	api.HandleFunc("/sankey/{service_uuid}", server.getSankeyData).Methods("GET")
	api.HandleFunc("/bubblemap/{service_uuid}", server.getBubbleMapData).Methods("GET")
	api.HandleFunc("/stats/{service_uuid}", server.getStats).Methods("GET")
	api.HandleFunc("/session/{session_id}/content", server.getSessionLogContent).Methods("GET")
	api.HandleFunc("/mirror/browse", server.browseMirrorDirectory).Methods("GET")
	api.HandleFunc("/mirror/download", server.downloadMirrorFile).Methods("GET")

	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		log.Fatalf("Failed to create sub filesystem: %v", err)
	}
	r.PathPrefix("/").Handler(http.FileServer(http.FS(staticFS)))

	httpHandler := enableCORS(r)

	log.Println("HTTP server starting on :8080")
	log.Println("gRPC server listening on :50051")
	log.Println("WebSocket endpoint: ws://localhost:8080/ws")
	log.Println("Dashboard: http://localhost:8080/")
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", httpHandler))
}
