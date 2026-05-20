package remora

import (
	"io"
	"log/slog"

	sdk "github.com/collinpfeifer/beluga-ext-sdk/belugav1"
)

// RemoraServiceServer implements the gRPC RemoraService for Beluga.
// It handles bidirectional streams from remora daemons running on remote hosts.
type RemoraServiceServer struct {
	sdk.UnimplementedRemoraServiceServer
	manager *Manager
	logger  *slog.Logger
}

// NewRemoraServiceServer creates a new gRPC server handler.
func NewRemoraServiceServer(manager *Manager, logger *slog.Logger) *RemoraServiceServer {
	return &RemoraServiceServer{
		manager: manager,
		logger:  logger,
	}
}

// Connect handles the bidirectional stream from a remora daemon.
// The daemon sends messages (registration, heartbeats, command output, directory chunks).
// Beluga sends commands through this stream.
func (s *RemoraServiceServer) Connect(stream sdk.RemoraService_ConnectServer) error {
	var hostID string

	defer func() {
		if hostID != "" {
			s.manager.UnregisterDaemon(hostID)
		}
	}()

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := msg.Payload.(type) {
		case *sdk.RemotaMessage_Registration:
			reg := payload.Registration
			hostID = reg.HostId

			conn := &DaemonConnection{
				HostID:      reg.HostId,
				Hostname:    reg.Hostname,
				AllowedDirs: reg.AllowedDirectories,
				Stream:      stream,
			}
			s.manager.RegisterDaemon(conn)

			s.logger.Info("remora daemon connected",
				"host_id", reg.HostId,
				"hostname", reg.Hostname,
			)

		case *sdk.RemotaMessage_Heartbeat:
			// Heartbeat acknowledged — connection is alive.
			s.logger.Debug("heartbeat from remora daemon", "host_id", hostID)

		case *sdk.RemotaMessage_CommandOutput:
			s.manager.HandleCommandOutput(payload.CommandOutput)

		case *sdk.RemotaMessage_DirectoryChunk:
			s.logger.Debug("directory chunk from remora daemon",
				"host_id", hostID,
				"request_id", payload.DirectoryChunk.RequestId,
			)

		default:
			s.logger.Warn("unknown message from remora daemon", "host_id", hostID)
		}
	}
}
