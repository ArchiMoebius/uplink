package main

import (
	"log"
	"net"
	"sync"

	"github.com/oschwald/geoip2-golang"
)

type GeoIPService struct {
	db    *geoip2.Reader
	mutex sync.RWMutex
	cache map[string]*GeoLocation
}

type GeoLocation struct {
	Latitude  float64
	Longitude float64
	Country   string
	City      string
}

// NewGeoIPService creates a new GeoIP service with MaxMind database
// Download the free GeoLite2 database from:
// https://dev.maxmind.com/geoip/geolite2-free-geolocation-data
func NewGeoIPService(dbPath string) (*GeoIPService, error) {
	db, err := geoip2.Open(dbPath)
	if err != nil {
		log.Printf("Warning: Could not open GeoIP database at %s: %v", dbPath, err)
		log.Println("Falling back to mock GeoIP data. Download GeoLite2-City.mmdb from:")
		log.Println("https://dev.maxmind.com/geoip/geolite2-free-geolocation-data")
		return &GeoIPService{
			db:    nil,
			cache: make(map[string]*GeoLocation),
		}, nil
	}

	log.Printf("Successfully loaded GeoIP database from %s", dbPath)
	return &GeoIPService{
		db:    db,
		cache: make(map[string]*GeoLocation),
	}, nil
}

// Lookup returns geographic information for an IP address
func (g *GeoIPService) Lookup(ipStr string) *GeoLocation {
	// Check cache first
	g.mutex.RLock()
	if loc, exists := g.cache[ipStr]; exists {
		g.mutex.RUnlock()
		return loc
	}
	g.mutex.RUnlock()

	// If no database is available, use mock data
	if g.db == nil {
		return g.mockLookup(ipStr)
	}

	// Parse IP address
	ip := net.ParseIP(ipStr)
	if ip == nil {
		log.Printf("Invalid IP address: %s", ipStr)
		return g.mockLookup(ipStr)
	}

	// Lookup in MaxMind database
	record, err := g.db.City(ip)
	if err != nil {
		log.Printf("GeoIP lookup failed for %s: %v, using mock data", ipStr, err)
		return g.mockLookup(ipStr)
	}

	loc := &GeoLocation{
		Latitude:  record.Location.Latitude,
		Longitude: record.Location.Longitude,
		Country:   record.Country.Names["en"],
		City:      record.City.Names["en"],
	}

	// Cache the result
	g.mutex.Lock()
	g.cache[ipStr] = loc
	g.mutex.Unlock()

	return loc
}

// mockLookup provides fallback mock geolocation data when MaxMind DB is unavailable
func (g *GeoIPService) mockLookup(ipStr string) *GeoLocation {
	// Check cache for mock data
	g.mutex.RLock()
	if loc, exists := g.cache[ipStr]; exists {
		g.mutex.RUnlock()
		return loc
	}
	g.mutex.RUnlock()

	// Generate consistent but pseudo-random coordinates for demo/testing
	hash := 0
	for _, c := range ipStr {
		hash = hash*31 + int(c)
	}

	// Generate more realistic coordinates
	// Bias towards populated regions
	lat := float64((hash%140)-60) + float64(hash%100)/100.0       // -60 to 80
	lon := float64((hash%340)-170) + float64((hash>>8)%100)/100.0 // -170 to 170

	// Add some known locations for common IP ranges (for demo purposes)
	if len(ipStr) > 0 {
		switch ipStr[0] {
		case '1', '2': // Asia-Pacific region
			lat = 35.0 + float64(hash%20)
			lon = 100.0 + float64(hash%60)
		case '3', '4': // North America
			lat = 30.0 + float64(hash%30)
			lon = -120.0 + float64(hash%40)
		case '5', '6': // Europe
			lat = 45.0 + float64(hash%25)
			lon = 5.0 + float64(hash%40)
		case '7', '8': // South America / Africa
			lat = -20.0 + float64(hash%40)
			lon = -40.0 + float64(hash%80)
		}
	}

	loc := &GeoLocation{
		Latitude:  lat,
		Longitude: lon,
		Country:   "Unknown",
		City:      "Unknown",
	}

	// Cache the mock result
	g.mutex.Lock()
	g.cache[ipStr] = loc
	g.mutex.Unlock()

	return loc
}

// Close closes the GeoIP database
func (g *GeoIPService) Close() {
	if g.db != nil {
		g.db.Close()
	}
}
