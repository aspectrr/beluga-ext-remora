package remora

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	sdk "github.com/aspectrr/beluga-ext-sdk/belugav1"
	"github.com/google/uuid"
)

// DaemonConnection represents a connected remora daemon.
type DaemonConnection struct {
	HostID      string
	Hostname    string
	AllowedDirs []string
	Stream      sdk.RemoraService_ConnectServer
}

// PendingCommand tracks a command sent to a daemon, waiting for a response.
type PendingCommand struct {
	Response chan *sdk.CommandOutput
}

// DaemonInfo holds summary information about a connected remora daemon.
type DaemonInfo struct {
	HostID      string   `json:"host_id"`
	Hostname    string   `json:"hostname"`
	AllowedDirs []string `json:"allowed_dirs"`
}

// Manager tracks connected remora daemons and routes commands to them.
type Manager struct {
	mu      sync.RWMutex
	daemons map[string]*DaemonConnection // hostID → connection
	pending map[string]*PendingCommand   // requestID → pending response
	logger  *slog.Logger
}

// NewManager creates a new remora connection manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		daemons: make(map[string]*DaemonConnection),
		pending: make(map[string]*PendingCommand),
		logger:  logger,
	}
}

// RegisterDaemon adds a newly connected remora daemon.
func (m *Manager) RegisterDaemon(conn *DaemonConnection) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if old, exists := m.daemons[conn.HostID]; exists {
		m.logger.Warn("replacing existing daemon connection",
			"host_id", conn.HostID,
			"old_hostname", old.Hostname,
			"new_hostname", conn.Hostname,
		)
	}

	m.daemons[conn.HostID] = conn
	m.logger.Info("daemon registered",
		"host_id", conn.HostID,
		"hostname", conn.Hostname,
		"allowed_dirs", conn.AllowedDirs,
	)
}

// UnregisterDaemon removes a daemon (disconnected).
func (m *Manager) UnregisterDaemon(hostID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.daemons[hostID]; exists {
		delete(m.daemons, hostID)
		m.logger.Info("daemon unregistered", "host_id", hostID)
	}
}

// GetDaemon returns a daemon connection by host ID.
func (m *Manager) GetDaemon(hostID string) (*DaemonConnection, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	conn, ok := m.daemons[hostID]
	if !ok {
		return nil, fmt.Errorf("daemon %q is not connected", hostID)
	}
	return conn, nil
}

// ListConnectedDaemons returns summary information for all connected daemons.
func (m *Manager) ListConnectedDaemons() []DaemonInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	infos := make([]DaemonInfo, 0, len(m.daemons))
	for _, conn := range m.daemons {
		infos = append(infos, DaemonInfo{
			HostID:      conn.HostID,
			Hostname:    conn.Hostname,
			AllowedDirs: conn.AllowedDirs,
		})
	}
	return infos
}

// SendCommandAndWait sends a command to a remora daemon and waits for the response.
// hostID must be non-empty — use the host_list_daemons tool to discover available hosts.
func (m *Manager) SendCommandAndWait(ctx context.Context, hostID string, req *sdk.ExecuteCommandRequest) (*sdk.CommandOutput, error) {
	if hostID == "" {
		return nil, fmt.Errorf("host is required: no default daemon host")
	}

	if req.RequestId == "" {
		req.RequestId = uuid.New().String()
	}

	m.mu.Lock()

	conn, ok := m.daemons[hostID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("daemon %q is not connected", hostID)
	}

	pending := &PendingCommand{
		Response: make(chan *sdk.CommandOutput, 1),
	}
	m.pending[req.RequestId] = pending
	m.mu.Unlock()

	// Send the command through the stream.
	belugaCmd := &sdk.BelugaCommand{
		Payload: &sdk.BelugaCommand_ExecuteCommand{
			ExecuteCommand: req,
		},
	}
	if err := conn.Stream.Send(belugaCmd); err != nil {
		m.mu.Lock()
		delete(m.pending, req.RequestId)
		m.mu.Unlock()
		return nil, fmt.Errorf("sending command to daemon %q: %w", hostID, err)
	}

	select {
	case resp := <-pending.Response:
		return resp, nil
	case <-ctx.Done():
		m.mu.Lock()
		delete(m.pending, req.RequestId)
		m.mu.Unlock()
		return nil, fmt.Errorf("command timed out: %w", ctx.Err())
	}
}

// HandleCommandOutput routes a command response from a daemon to the
// pending command channel.
func (m *Manager) HandleCommandOutput(output *sdk.CommandOutput) {
	m.mu.Lock()
	pending, ok := m.pending[output.RequestId]
	if ok {
		delete(m.pending, output.RequestId)
	}
	m.mu.Unlock()

	if !ok {
		m.logger.Warn("received command output for unknown request",
			"request_id", output.RequestId,
		)
		return
	}

	pending.Response <- output
}
