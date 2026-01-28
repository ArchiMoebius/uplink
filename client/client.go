package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/ArchiMoebius/uplinkpkg/gen/v1"
)

type BeamClient struct {
	conn   *grpc.ClientConn
	client pb.TransporterClient
	stream pb.Transporter_BeamClient
	ctx    context.Context
	cancel context.CancelFunc
}

type ClientOptions struct {
	ConnectTimeout               time.Duration
	KeepaliveTime                time.Duration
	KeepaliveTimeout             time.Duration
	KeepalivePermitWithoutStream bool
}

func DefaultClientOptions() *ClientOptions {
	return &ClientOptions{
		ConnectTimeout:               10 * time.Second,
		KeepaliveTime:                30 * time.Second,
		KeepaliveTimeout:             10 * time.Second,
		KeepalivePermitWithoutStream: true,
	}
}

func NewBeamClient(serverAddr string) (*BeamClient, error) {
	return NewBeamClientWithOptions(serverAddr, DefaultClientOptions())
}

func NewBeamClientWithOptions(serverAddr string, opts *ClientOptions) (*BeamClient, error) {
	ctx, cancel := context.WithCancel(context.Background())

	kacp := keepalive.ClientParameters{
		Time:                opts.KeepaliveTime,
		Timeout:             opts.KeepaliveTimeout,
		PermitWithoutStream: opts.KeepalivePermitWithoutStream,
	}

	conn, err := grpc.NewClient(
		serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(kacp),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	client := pb.NewTransporterClient(conn)

	stream, err := client.Beam(ctx)
	if err != nil {
		cancel()
		conn.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &BeamClient{
		conn:   conn,
		client: client,
		stream: stream,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (c *BeamClient) SendEvent(event *pb.SSHConnectionEvent) error {
	return c.stream.Send(event)
}

func (c *BeamClient) CloseSend() error {
	if c.stream != nil {
		return c.stream.CloseSend()
	}
	return nil
}

func (c *BeamClient) Close() error {
	c.cancel()
	if c.stream != nil {
		if err := c.stream.CloseSend(); err != nil {
			fmt.Printf("close error %v", err)
		}
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *BeamClient) GetState() connectivity.State {
	if c.conn == nil {
		return connectivity.Shutdown
	}
	return c.conn.GetState()
}

func (c *BeamClient) IsConnected() bool {
	state := c.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

func (c *BeamClient) WaitForReady(ctx context.Context) error {
	state := c.GetState()
	if state == connectivity.Ready {
		return nil
	}

	for {
		if !c.conn.WaitForStateChange(ctx, state) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("connection not ready")
		}

		state = c.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if state == connectivity.Shutdown {
			return fmt.Errorf("connection shutdown")
		}
	}
}

func (c *BeamClient) Reconnect() error {
	if c.stream != nil {
		c.stream.CloseSend()
	}

	stream, err := c.client.Beam(c.ctx)
	if err != nil {
		return fmt.Errorf("failed to recreate stream: %w", err)
	}

	c.stream = stream
	return nil
}
