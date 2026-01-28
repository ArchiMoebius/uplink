package server

import (
	"context"

	pb "github.com/ArchiMoebius/uplinkpkg/gen/v1"
)

type SSHTransporterServer struct {
	pb.UnimplementedTransporterServer
	beamServer *BeamServer[*pb.SSHConnectionEvent]
}

func NewSSHTransporterServer(handler EventHandler[*pb.SSHConnectionEvent], concurrency int) *SSHTransporterServer {
	return &SSHTransporterServer{
		beamServer: NewBeamServer(handler, concurrency),
	}
}

func (s *SSHTransporterServer) Beam(stream pb.Transporter_BeamServer) error {
	return s.beamServer.HandleStream(&sshStreamAdapter{stream})
}

func (s *SSHTransporterServer) GetStats() StreamStats {
	return s.beamServer.GetStats()
}

type sshStreamAdapter struct {
	pb.Transporter_BeamServer
}

func (a *sshStreamAdapter) Recv() (*pb.SSHConnectionEvent, error) {
	return a.Transporter_BeamServer.Recv()
}

func (a *sshStreamAdapter) Context() context.Context {
	return a.Transporter_BeamServer.Context()
}
