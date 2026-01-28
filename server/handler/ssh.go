package handler

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	pb "uplink/pkg/gen/v1"

	"gorm.io/gorm"
)

type Service struct {
	ID        uint      `gorm:"primaryKey"`
	UUID      string    `gorm:"uniqueIndex;size:32;not null"` // hex-encoded UUID
	CreatedAt time.Time `gorm:"index"`
}

type IPAddress struct {
	ID        uint   `gorm:"primaryKey"`
	Version   uint8  `gorm:"not null;index:idx_ip_version_address"`               // 4 or 6
	Address   string `gorm:"uniqueIndex:idx_ip_version_address;size:39;not null"` // IPv4 or IPv6 as string
	CreatedAt time.Time
}

type HASSHFingerprint struct {
	ID          uint   `gorm:"primaryKey"`
	Fingerprint string `gorm:"uniqueIndex;size:255;not null"`
	CreatedAt   time.Time
}

type Username struct {
	ID        uint   `gorm:"primaryKey"`
	Username  string `gorm:"uniqueIndex;size:255;not null"`
	CreatedAt time.Time
}

type Password struct {
	ID        uint   `gorm:"primaryKey"`
	Password  string `gorm:"uniqueIndex;size:255;not null"`
	CreatedAt time.Time
}

type SshClientName struct {
	ID    uint   `gorm:"primaryKey"`
	Value string `gorm:"uniqueIndex;size:255;not null"`
}

type AuthMethod struct {
	ID         uint   `gorm:"primaryKey"`
	MethodName string `gorm:"uniqueIndex;size:100;not null"`
	CreatedAt  time.Time
}

type SSHConnectionEvent struct {
	ID                 uint             `gorm:"primaryKey"`
	ServiceID          uint             `gorm:"not null;index:idx_service_timestamp"`
	Service            Service          `gorm:"foreignKey:ServiceID;constraint:OnDelete:RESTRICT"`
	SourceIPID         uint             `gorm:"not null;index"`
	SourceIP           IPAddress        `gorm:"foreignKey:SourceIPID;constraint:OnDelete:RESTRICT"`
	SourcePort         uint32           `gorm:"not null"`
	HASSHFingerprintID uint             `gorm:"not null;index"`
	HASSHFingerprint   HASSHFingerprint `gorm:"foreignKey:HASSHFingerprintID;constraint:OnDelete:RESTRICT"`
	UsernameID         *uint            `gorm:"index"` // nullable
	Username           *Username        `gorm:"foreignKey:UsernameID;constraint:OnDelete:SET NULL"`
	PasswordID         *uint            `gorm:"index"` // nullable
	Password           *Password        `gorm:"foreignKey:SshClientNameID;constraint:OnDelete:SET NULL"`
	SshClientNameID    *uint            `gorm:"index"` // nullable
	SshClientName      *SshClientName   `gorm:"foreignKey:SshClientNameID;constraint:OnDelete:SET NULL"`
	Timestamp          time.Time        `gorm:"not null;index:idx_service_timestamp"`
	CreatedAt          time.Time        `gorm:"index"`
}

type SSHEventAuthMethod struct {
	ID           uint               `gorm:"primaryKey"`
	EventID      uint               `gorm:"not null;index:idx_event_auth"`
	Event        SSHConnectionEvent `gorm:"foreignKey:EventID;constraint:OnDelete:CASCADE"`
	AuthMethodID uint               `gorm:"not null;index:idx_event_auth"`
	AuthMethod   AuthMethod         `gorm:"foreignKey:AuthMethodID;constraint:OnDelete:RESTRICT"`
	CreatedAt    time.Time
}

type SSHEventHandler struct {
	db             *gorm.DB
	notifyCallback func(serviceUUID string)
}

func NewSSHEventHandler(db *gorm.DB) (*SSHEventHandler, error) {
	return NewSSHEventHandlerWithCallback(db, nil)
}

func NewSSHEventHandlerWithCallback(db *gorm.DB, notifyCallback func(serviceUUID string)) (*SSHEventHandler, error) {
	err := db.AutoMigrate(
		&Service{},
		&IPAddress{},
		&HASSHFingerprint{},
		&SshClientName{},
		&Username{},
		&Password{},
		&AuthMethod{},
		&SSHConnectionEvent{},
		&SSHEventAuthMethod{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to migrate database schema: %w", err)
	}

	return &SSHEventHandler{
		db:             db,
		notifyCallback: notifyCallback,
	}, nil
}

func (h *SSHEventHandler) Handle(ctx context.Context, event *pb.SSHConnectionEvent) error {

	if len(event.ServiceUuid) != 16 {
		return fmt.Errorf("invalid service UUID length: %d", len(event.ServiceUuid))
	}
	if len(event.Hassh) != 16 {
		return fmt.Errorf("hassh is required")
	}

	log.Printf("Processing SSH event: service_uuid=%x, hassh=%x, source_port=%d",
		event.ServiceUuid, event.Hassh, event.SourcePort)

	serviceUUID := hex.EncodeToString(event.ServiceUuid)

	return h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		service, err := h.getOrCreateService(tx, serviceUUID)
		if err != nil {
			return fmt.Errorf("failed to get/create service: %w", err)
		}

		sourceIP, err := h.getOrCreateSourceIP(tx, event)
		if err != nil {
			return fmt.Errorf("failed to get/create source IP: %w", err)
		}

		hassh, err := h.getOrCreateHASSH(tx, event.Hassh)
		if err != nil {
			return fmt.Errorf("failed to get/create HASSH fingerprint: %w", err)
		}

		_, err = h.getOrCreateSshClientName(tx, string(event.SshClientName))
		var SshClientNameID *uint
		if err != nil {
			return fmt.Errorf("failed to get/create scn: %w", err)
		}

		var usernameID *uint
		if len(event.Username) > 0 {
			usernameStr := string(event.Username)
			username, err := h.getOrCreateUsername(tx, usernameStr)
			if err != nil {
				return fmt.Errorf("failed to get/create username: %w", err)
			}
			usernameID = &username.ID
			log.Printf("Auth attempt - username: %s, methods: %v", usernameStr, event.AuthMethods)
		}

		var passwordID *uint
		if len(event.Password) > 0 {
			passwordStr := string(event.Password)
			password, err := h.getOrCreatePassword(tx, passwordStr)
			if err != nil {
				return fmt.Errorf("failed to get/create password: %w", err)
			}
			passwordID = &password.ID
		}

		eventTimestamp := time.Unix(0, event.TimestampMicros*1000)
		sshEvent := SSHConnectionEvent{
			ServiceID:          service.ID,
			SourceIPID:         sourceIP.ID,
			SourcePort:         event.SourcePort,
			HASSHFingerprintID: hassh.ID,
			SshClientNameID:    SshClientNameID,
			UsernameID:         usernameID,
			PasswordID:         passwordID,
			Timestamp:          eventTimestamp,
		}

		if err := tx.Create(&sshEvent).Error; err != nil {
			return fmt.Errorf("failed to create SSH event: %w", err)
		}

		if len(event.AuthMethods) > 0 {
			if err := h.createEventAuthMethods(tx, sshEvent.ID, event.AuthMethods); err != nil {
				return fmt.Errorf("failed to create auth methods: %w", err)
			}
		}

		log.Printf("Successfully stored SSH event with ID: %d", sshEvent.ID)

		if h.notifyCallback != nil {
			h.notifyCallback(serviceUUID)
		}

		return nil
	})
}

func (h *SSHEventHandler) getOrCreateService(tx *gorm.DB, uuid string) (*Service, error) {
	var service Service
	result := tx.Where("uuid = ?", uuid).First(&service)

	if result.Error == gorm.ErrRecordNotFound {
		service = Service{UUID: uuid}
		if err := tx.Create(&service).Error; err != nil {
			return nil, err
		}
		return &service, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &service, nil
}

func (h *SSHEventHandler) getOrCreateSourceIP(tx *gorm.DB, event *pb.SSHConnectionEvent) (*IPAddress, error) {
	var version uint8
	var address string

	switch ip := event.SourceIp.(type) {
	case *pb.SSHConnectionEvent_SourceIpv4:
		version = 4
		address = fmt.Sprintf("%d.%d.%d.%d",
			byte(ip.SourceIpv4>>24),
			byte(ip.SourceIpv4>>16),
			byte(ip.SourceIpv4>>8),
			byte(ip.SourceIpv4))
		log.Printf("Source IPv4: %s", address)
	case *pb.SSHConnectionEvent_SourceIpv6:
		version = 6
		address = formatIPv6(ip.SourceIpv6)
		log.Printf("Source IPv6: %s", address)
	default:
		return nil, fmt.Errorf("unknown IP address type")
	}

	var ipAddr IPAddress
	result := tx.Where("version = ? AND address = ?", version, address).First(&ipAddr)

	if result.Error == gorm.ErrRecordNotFound {
		ipAddr = IPAddress{
			Version: version,
			Address: address,
		}
		if err := tx.Create(&ipAddr).Error; err != nil {
			return nil, err
		}
		return &ipAddr, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &ipAddr, nil
}

func (h *SSHEventHandler) getOrCreateHASSH(tx *gorm.DB, fingerprint []byte) (*HASSHFingerprint, error) {
	var hassh HASSHFingerprint
	fp := hex.EncodeToString(fingerprint)
	result := tx.Where("fingerprint = ?", fp).First(&hassh)

	if result.Error == gorm.ErrRecordNotFound {
		hassh = HASSHFingerprint{Fingerprint: fp}
		if err := tx.Create(&hassh).Error; err != nil {
			return nil, err
		}
		return &hassh, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &hassh, nil
}

func (h *SSHEventHandler) getOrCreateSshClientName(tx *gorm.DB, clientname string) (*SshClientName, error) {
	var scn SshClientName
	result := tx.Where("value = ?", clientname).First(&scn)

	if result.Error == gorm.ErrRecordNotFound {
		scn = SshClientName{Value: clientname}
		if err := tx.Create(&scn).Error; err != nil {
			return nil, err
		}
		return &scn, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &scn, nil
}

func (h *SSHEventHandler) getOrCreateUsername(tx *gorm.DB, username string) (*Username, error) {
	var user Username
	result := tx.Where("username = ?", username).First(&user)

	if result.Error == gorm.ErrRecordNotFound {
		user = Username{Username: username}
		if err := tx.Create(&user).Error; err != nil {
			return nil, err
		}
		return &user, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &user, nil
}

func (h *SSHEventHandler) getOrCreatePassword(tx *gorm.DB, password string) (*Password, error) {
	var pwd Password
	result := tx.Where("password = ?", password).First(&pwd)

	if result.Error == gorm.ErrRecordNotFound {
		pwd = Password{Password: password}
		if err := tx.Create(&pwd).Error; err != nil {
			return nil, err
		}
		return &pwd, nil
	}

	if result.Error != nil {
		return nil, result.Error
	}

	return &pwd, nil
}

func (h *SSHEventHandler) createEventAuthMethods(tx *gorm.DB, eventID uint, methods []pb.AuthMethod) error {
	for _, method := range methods {
		methodName := method.String()

		var authMethod AuthMethod
		result := tx.Where("method_name = ?", methodName).First(&authMethod)

		if result.Error == gorm.ErrRecordNotFound {
			authMethod = AuthMethod{MethodName: methodName}
			if err := tx.Create(&authMethod).Error; err != nil {
				return err
			}
		} else if result.Error != nil {
			return result.Error
		}

		eventAuthMethod := SSHEventAuthMethod{
			EventID:      eventID,
			AuthMethodID: authMethod.ID,
		}
		if err := tx.Create(&eventAuthMethod).Error; err != nil {
			return err
		}
	}
	return nil
}

func (h *SSHEventHandler) OnStreamStart(ctx context.Context) error {
	log.Println("SSH stream started")
	return nil
}

func (h *SSHEventHandler) OnStreamEnd(ctx context.Context) error {
	log.Println("SSH stream ended")
	return nil
}

func formatIPv6(ipv6 []byte) string {
	if len(ipv6) != 16 {
		return hex.EncodeToString(ipv6)
	}

	return fmt.Sprintf("%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
		ipv6[0], ipv6[1], ipv6[2], ipv6[3],
		ipv6[4], ipv6[5], ipv6[6], ipv6[7],
		ipv6[8], ipv6[9], ipv6[10], ipv6[11],
		ipv6[12], ipv6[13], ipv6[14], ipv6[15])
}
