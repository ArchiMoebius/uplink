package main

import (
	"encoding/binary"
	"log"
	"net"
	"time"

	"uplink/client"
	pb "uplink/pkg/gen/v1"

	"github.com/google/uuid"
)

func main() {
	serverAddr := "127.0.0.1:50051"
	beamClient, err := client.NewBeamClient(serverAddr)
	if err != nil {
		log.Fatalf("Failed to create beam client: %v", err)
	}
	defer beamClient.Close()

	log.Printf("Connected to server at %s", serverAddr)
	log.Printf("Connection state: %v", beamClient.GetState())

	events := createExampleEvents()

	for i, event := range events {
		log.Printf("Sending event %d/%d", i+1, len(events))
		if err := beamClient.SendEvent(event); err != nil {
			log.Fatalf("Failed to send event: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	log.Println("All events sent successfully")

	if err := beamClient.Close(); err != nil {
		log.Printf("Error closing send: %v", err)
	}
}

func createExampleEvents() []*pb.SSHConnectionEvent {
	// serviceUUID := uuid.New()
	serviceUUID := uuid.MustParse("dd40d669-0b5d-4298-a62b-a19b79ad813c")

	return []*pb.SSHConnectionEvent{
		// Event 1: Successful password authentication (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("2.1.1.1"),
			},
			SourcePort:  54321,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_PASSWORD,
			},
			SshClientName: "openssh v2",
			Username:      []byte("admin"),
			Password:      []byte("P@ssw0rd123"),
		},

		// Event 2: Public key authentication attempt (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("8.7.7.3"),
			},
			SourcePort:  49152,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_PUBLICKEY,
			},
			SshClientName: "openssh v1",
			Username:      []byte("deploy"),
			PublicKeySums: [][]byte{
				[]byte("bd8990e57a90d478a0fb91e2df6e9d2a"),
			},
		},

		// Event 3: Brute force attack pattern (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("210.20.13.2"),
			},
			SourcePort:  35678,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_PASSWORD,
			},
			SshClientName: "openssh v2",
			Username:      []byte("root"),
			Password:      []byte("123456"),
		},

		// Event 4: Multiple auth methods attempted (IPv6)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv6{
				SourceIpv6: ipv6ToBytes("2001:db8::1"),
			},
			SourcePort:  44444,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_KEYBOARD_INTERACTIVE,
				pb.AuthMethod_AUTH_METHOD_PASSWORD,
			},
			SshClientName: "openssh v2",
			Username:      []byte("user"),
			Password:      []byte("SecurePass!"),
		},

		// Event 5: GSSAPI authentication (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("172.16.0.10"),
			},
			SourcePort:  60000,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_GSSAPI_WITH_MIC,
			},
			SshClientName: "openssh v2",
			Username:      []byte("admin@DOMAIN.COM"),
		},

		// Event 6: Hostbased authentication (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("192.168.10.25"),
			},
			SourcePort:  55555,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_HOSTBASED,
			},
			SshClientName: "openssh v2",
			Username:      []byte("sysadmin"),
			PublicKeySums: [][]byte{
				[]byte("bd8990e57a90d478a0fb91e2df6e9d2a"),
			},
		},

		// Event 7: No authentication method (probe/scan) (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("198.51.100.123"),
			},
			SourcePort:  12345,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_NONE,
			},
			SshClientName: "openssh v2",
		},

		// Event 8: Multiple public keys attempted (IPv6)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv6{
				SourceIpv6: ipv6ToBytes("fe80::1"),
			},
			SourcePort:  33333,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_PUBLICKEY,
			},
			SshClientName: "openssh v2",
			Username:      []byte("devops"),
			PublicKeySums: [][]byte{
				[]byte("bd8990e57a90d478a0fb91e2df6e9d2a"),
				[]byte("bd8990e57a90d478a0fb91e2df6e9d2b"),
				[]byte("bd8990e57a90d478a0fb91e2df6e9d2c"),
			},
		},

		// Event 9: Common bot/scanner attempt (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("185.220.101.50"),
			},
			SourcePort:  22222,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_PASSWORD,
			},
			SshClientName: "openssh v2",
			Username:      []byte("admin"),
			Password:      []byte("admin"),
		},

		// Event 10: Legitimate user with keyboard-interactive (IPv4)
		{
			TimestampMicros: time.Now().UnixMicro(),
			SourceIp: &pb.SSHConnectionEvent_SourceIpv4{
				SourceIpv4: ipv4ToFixed32("10.1.1.100"),
			},
			SourcePort:  52000,
			ServiceUuid: uuidToBytes(serviceUUID),
			Hassh:       uuidToBytes(serviceUUID),
			AuthMethods: []pb.AuthMethod{
				pb.AuthMethod_AUTH_METHOD_KEYBOARD_INTERACTIVE,
			},
			SshClientName: "openssh v2",
			Username:      []byte("jane.doe"),
		},
	}
}

// ipv4ToFixed32 converts an IPv4 string to a fixed32 (uint32)
func ipv4ToFixed32(ipStr string) uint32 {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return 0
	}
	// Convert to IPv4 (4 bytes)
	ipv4 := ip.To4()
	if ipv4 == nil {
		return 0
	}
	return binary.BigEndian.Uint32(ipv4)
}

// ipv6ToBytes converts an IPv6 string to a byte slice (16 bytes)
func ipv6ToBytes(ipStr string) []byte {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return make([]byte, 16)
	}
	// Convert to IPv6 (16 bytes)
	ipv6 := ip.To16()
	if ipv6 == nil {
		return make([]byte, 16)
	}
	return ipv6
}

// uuidToBytes converts a UUID to a byte slice (16 bytes)
func uuidToBytes(u uuid.UUID) []byte {
	b, _ := u.MarshalBinary()
	return b
}
