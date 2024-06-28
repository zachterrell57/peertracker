package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	_ "github.com/joho/godotenv/autoload"
)

type (
	PeerAddress struct {
		Network       string    `gorm:"primaryKey"`
		Address       string    `gorm:"primaryKey"`
		LastSeen      time.Time `gorm:"not null"`
		GossipAddress string
		GossipFamily  int
		GossipPort    int
		GossipDNSName string
		RPCAddress    string
		RPCFamily     int
		RPCPort       int
		RPCDNSName    string
		Count         int64
		HubVersion    string
		AppVersion    string
		PeerTimestamp time.Time

		// Geo Data
		Country          *string
		CountryCode      *string
		Region           *string
		RegionName       *string
		City             *string
		Zip              *string
		Latitude         *float64
		Longitude        *float64
		Hosting          *bool
		Org              *string
		GeoDataFetchedAt *time.Time

		// Hub Info
		Latency        *int64
		Version        *string
		IsSyncing      *bool
		Nickname       *string
		RootHash       *string
		NumMessages    *int `gorm:"column:num_messages"`
		NumFidEvents   *int `gorm:"column:num_fid_events"`
		NumFnameEvents *int `gorm:"column:num_fname_events"`
		PeerId         *string
		HubOperatorFid *int
		InfoFetchedAt  *time.Time

		CreatedAt time.Time
		UpdatedAt time.Time
	}

	PeerListResponse struct {
		Contacts []PeerContact `json:"contacts"`
	}

	PeerContact struct {
		GossipAddress struct {
			Address string `json:"address"`
			Family  int    `json:"family"`
			Port    int    `json:"port"`
			DNSName string `json:"dnsName"`
		} `json:"gossipAddress"`
		RPCAddress struct {
			Address string `json:"address"`
			Family  int    `json:"family"`
			Port    int    `json:"port"`
			DNSName string `json:"dnsName"`
		} `json:"rpcAddress"`
		Count      uint64 `json:"count"`
		HubVersion string `json:"hubVersion"`
		Network    string `json:"network"`
		AppVersion string `json:"appVersion"`
		Timestamp  int64  `json:"timestamp"`
	}

	GeoData struct {
		Country     string
		CountryCode string
		Region      string
		RegionName  string
		City        string
		Zip         string
		Latitude    float64
		Longitude   float64
		Hosting     bool
		Org         string
	}

	HubInfo struct {
		Version        string
		IsSyncing      bool
		Nickname       string
		RootHash       string
		PeerId         string
		HubOperatorFid int
		NumMessages    int
		NumFidEvents   int
		NumFnameEvents int
		Latency        int64
	}

	HubInfoResponse struct {
		Version        string  `json:"version"`
		IsSyncing      bool    `json:"isSyncing"`
		Nickname       string  `json:"nickname"`
		RootHash       string  `json:"rootHash"`
		DbStats        DbStats `json:"dbStats" gorm:"type:json"`
		PeerId         string  `json:"peerId"`
		HubOperatorFid int     `json:"hubOperatorFid"`
	}

	DbStats struct {
		NumMessages    int `json:"numMessages"`
		NumFidEvents   int `json:"numFidEvents"`
		NumFnameEvents int `json:"numFnameEvents"`
	}

	IPAPIResponse struct {
		Message     string  `json:"message"`
		Query       string  `json:"query"`
		Status      string  `json:"status"`
		Country     string  `json:"country"`
		CountryCode string  `json:"countryCode"`
		Region      string  `json:"region"`
		RegionName  string  `json:"regionName"`
		City        string  `json:"city"`
		Zip         string  `json:"zip"`
		Latitude    float64 `json:"lat"`
		Longitude   float64 `json:"lon"`
		Timezone    string  `json:"timezone"`
		ISP         string  `json:"isp"`
		Org         string  `json:"org"`
		As          string  `json:"as"`
		Hosting     bool    `json:"hosting"`
	}
)

func main() {
	db := initDB()

	go func() {
		// Seed the database with the initial peer list.
		getPeerList(db)

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			getPeerList(db)
		}
	}()

	// Get coordinated and other geo data
	go resolveIPToGeo(db)

	// Get specific hub info and check latency
	go getHubInfo(db)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)

	// Block until an interrupt signal is received
	<-quit
	fmt.Println("Shutting down...")
}

func getPeerList(db *gorm.DB) {
	log.Printf("Getting peer list...")

	url := fmt.Sprintf("http://%s/v1/currentPeers", os.Getenv("HUB_URL"))

	log.Printf("Fetching peer list from: %s", url)

	resp, err := http.Get(url)
	if err != nil {
		log.Printf("Error fetching peer list: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Non-OK HTTP status when fetching peer list: %s", resp.Status)
		return
	};

	var peerListResponse PeerListResponse
	if err := json.NewDecoder(resp.Body).Decode(&peerListResponse); err != nil {
		log.Printf("Error decoding peer list response: %v", err)
		return
	}

	for _, contact := range peerListResponse.Contacts {
		peerAddress := PeerAddress{
			Address:       contact.GossipAddress.Address,
			LastSeen:      time.Now().UTC(),
			GossipAddress: contact.GossipAddress.Address,
			GossipFamily:  contact.GossipAddress.Family,
			GossipPort:    contact.GossipAddress.Port,
			GossipDNSName: contact.GossipAddress.DNSName,
			RPCAddress:    contact.RPCAddress.Address,
			RPCFamily:     contact.RPCAddress.Family,
			RPCPort:       contact.RPCAddress.Port,
			RPCDNSName:    contact.RPCAddress.DNSName,
			PeerTimestamp: time.UnixMilli(contact.Timestamp),
			HubVersion:    contact.HubVersion,
			AppVersion:    contact.AppVersion,
			Network:       contact.Network,
			Count:         int64(contact.Count),
		}

		err = db.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "address"}, {Name: "network"}},
			DoUpdates: clause.AssignmentColumns(
				[]string{"last_seen", "gossip_address", "gossip_family", "gossip_port", "gossip_dns_name", "rpc_address", "rpc_family", "rpc_port", "rpc_dns_name", "peer_timestamp", "hub_version", "app_version", "count"},
			),
		}).Create(&peerAddress).Error

		if err != nil {
			log.Printf("Error inserting/updating peer in database: %v", err)
		}
	}
}

// ***********************
// IP to Geo logic
// ***********************

func resolveIPToGeo(db *gorm.DB) {
	batchSize := 10
	now := time.Now().UTC()

	for {
		var peers []PeerAddress
		db.Where("geo_data_fetched_at IS NULL OR geo_data_fetched_at < now() - interval '1 day'").Limit(batchSize).Find(&peers)

		for _, peer := range peers {
			geoData, err := getGeoDataFromAPI(peer.GossipAddress)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			peerAddressUpdate := PeerAddress{GeoDataFetchedAt: &now}

			if geoData != nil {
				peerAddressUpdate.Country = &geoData.Country
				peerAddressUpdate.CountryCode = &geoData.CountryCode
				peerAddressUpdate.Region = &geoData.Region
				peerAddressUpdate.RegionName = &geoData.RegionName
				peerAddressUpdate.City = &geoData.City
				peerAddressUpdate.Zip = &geoData.Zip
				peerAddressUpdate.Latitude = &geoData.Latitude
				peerAddressUpdate.Longitude = &geoData.Longitude
				peerAddressUpdate.Hosting = &geoData.Hosting
				peerAddressUpdate.Org = &geoData.Org
			}

			err = db.Model(&peer).Updates(peerAddressUpdate).Error

			if err != nil {
				fmt.Printf("Error saving geo data to DB %v \n", err)
			}

		}

		time.Sleep(1 * time.Minute) // General pause between batches
	}
}

func getGeoDataFromAPI(ipAddress string) (*GeoData, error) {
	apiUrl := fmt.Sprintf("http://ip-api.com/json/%s?fields=status,message,country,countryCode,region,regionName,city,zip,lat,lon,timezone,isp,org,as,hosting,query", ipAddress)

	resp, err := http.Get(apiUrl)
	if err != nil {
		return nil, fmt.Errorf("error making request to IP API: %v", err)
	}
	defer resp.Body.Close()

	// Read the rate limiting headers
	requestsRemaining := resp.Header.Get("X-Rl")
	ttl := resp.Header.Get("X-Ttl")

	if resp.StatusCode == http.StatusTooManyRequests || requestsRemaining == "0" {
		// Convert X-Ttl to integer
		ttlSeconds, err := strconv.Atoi(ttl)
		if err != nil {
			return nil, fmt.Errorf("error parsing X-Ttl header: %v", err)
		}
		// Sleep for the duration of X-Ttl
		time.Sleep(time.Duration(ttlSeconds) * time.Second)
		return nil, fmt.Errorf("rate limit exceeded, waiting for %d seconds", ttlSeconds)
	}

	var apiResponse IPAPIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, fmt.Errorf("error decoding response from IP API: %v", err)
	}

	if apiResponse.Message == "reserved range" {
		return nil, nil
	}

	if apiResponse.Latitude == 0 && apiResponse.Longitude == 0 {
		return nil, fmt.Errorf("no geo data found for: %+v", ipAddress)
	}

	return &GeoData{
		Country:     apiResponse.Country,
		CountryCode: apiResponse.CountryCode,
		Region:      apiResponse.Region,
		RegionName:  apiResponse.RegionName,
		City:        apiResponse.City,
		Zip:         apiResponse.Zip,
		Latitude:    apiResponse.Latitude,
		Longitude:   apiResponse.Longitude,
		Hosting:     apiResponse.Hosting,
		Org:         apiResponse.Org,
	}, nil
}

// ***********************
// Hub Info
// ***********************

func getHubInfo(db *gorm.DB) {
	log.Printf("Updating hub info...")

	batchSize := 10
	now := time.Now().UTC()

	for {
		var peers []PeerAddress
		db.Where("info_fetched_at IS NULL OR info_fetched_at < now() - interval '1 hour'").Limit(batchSize).Find(&peers)

		for _, peer := range peers {

			infoUpdate := PeerAddress{InfoFetchedAt: &now}

			info, err := fetchHubInfo(db, peer)
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

			if info != nil {
				infoUpdate.Version = &info.Version
				infoUpdate.IsSyncing = &info.IsSyncing
				infoUpdate.Nickname = &info.Nickname
				infoUpdate.RootHash = &info.RootHash
				infoUpdate.NumMessages = &info.NumMessages
				infoUpdate.NumFidEvents = &info.NumFidEvents
				infoUpdate.NumFnameEvents = &info.NumFnameEvents
				infoUpdate.PeerId = &info.PeerId
				infoUpdate.HubOperatorFid = &info.HubOperatorFid
				infoUpdate.Latency = &info.Latency
			}

			err = db.Model(&peer).Updates(infoUpdate).Error

			if err != nil {
				fmt.Printf("Error saving hub info to DB %v \n", err)
			}
		}

		time.Sleep(time.Second * 30)
	}
}

func fetchHubInfo(db *gorm.DB, peer PeerAddress) (*HubInfo, error) {
	// Start the timer just before making the request
	startTime := time.Now()

	url := fmt.Sprintf("http://%s:2281/v1/info?dbstats=1", peer.RPCAddress)

	client := http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Get(url)

	// Stop the timer after the request is made
	latency := time.Since(startTime).Milliseconds()

	if err != nil {
		return nil, fmt.Errorf("error fetching info for peer %s: %v", peer.GossipAddress, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Non-OK HTTP status for peer %s: %s", peer.GossipAddress, resp.Status)
	}

	var hubInfoResponse HubInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&hubInfoResponse); err != nil {
		return nil, fmt.Errorf("error decoding response for peer %s: %v", peer.GossipAddress, err)
	}

	var hubInfo = &HubInfo{
		Version:        hubInfoResponse.Version,
		IsSyncing:      hubInfoResponse.IsSyncing,
		Nickname:       hubInfoResponse.Nickname,
		RootHash:       hubInfoResponse.RootHash,
		NumMessages:    hubInfoResponse.DbStats.NumMessages,
		NumFidEvents:   hubInfoResponse.DbStats.NumFidEvents,
		NumFnameEvents: hubInfoResponse.DbStats.NumFnameEvents,
		PeerId:         hubInfoResponse.PeerId,
		Latency:        latency,
	}

	if hubInfoResponse.HubOperatorFid != 0 {
		hubInfo.HubOperatorFid = hubInfoResponse.HubOperatorFid
	}

	return hubInfo, nil
}

// ***********************
// Database
// ***********************

func initDB() *gorm.DB {
	pgUrl := os.Getenv("VERCEL_POSTGRES_URL")
	db, err := gorm.Open(postgres.Open(pgUrl), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	if err = db.AutoMigrate(&PeerAddress{}); err != nil {
		panic(err)
	}
	return db
}
