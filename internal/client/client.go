package client

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	stdsync "sync"
	"time"

	sdk "github.com/collinpfeifer/beluga-ext-sdk/belugav1"
	"github.com/collinpfeifer/beluga-ext-remora/internal/executor"
	remorasync "github.com/collinpfeifer/beluga-ext-remora/internal/sync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the remora daemon's gRPC client that connects to Beluga.
type Client struct {
	cfg         ClientConfig
	executor    *executor.Executor
	logger      *slog.Logger
	conn        *grpc.ClientConn
	stream      sdk.RemoraService_ConnectClient
	streamMu    stdsync.Mutex
	cancelFn    context.CancelFunc
	connected   bool
	connectedMu stdsync.Mutex
}

// ClientConfig holds the configuration for the remora gRPC client.
type ClientConfig struct {
	BelugaAddress     string
	HostID            string
	Hostname          string
	AllowedDirs       []string
	ReconnectInterval time.Duration
	TLSCert           string
	TLSKey            string
	TLSCA             string
}

// NewClient creates a new remora gRPC client.
func NewClient(cfg ClientConfig, exec *executor.Executor, logger *slog.Logger) *Client {
	return &Client{
		cfg:      cfg,
		executor: exec,
		logger:   logger,
	}
}

// Connect establishes the bidirectional stream to Beluga.
// It sends a Registration message first, then starts receiving commands.
func (c *Client) Connect(ctx context.Context) error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(c.cfg.BelugaAddress, opts...)
	if err != nil {
		return fmt.Errorf("dialing beluga: %w", err)
	}
	c.conn = conn

	client := sdk.NewRemoraServiceClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("opening stream: %w", err)
	}
	c.stream = stream

	// Send registration
	if err := c.send(&sdk.RemotaMessage{
		Payload: &sdk.RemotaMessage_Registration{
			Registration: &sdk.Registration{
				HostId:             c.cfg.HostID,
				Hostname:           c.cfg.Hostname,
				AllowedDirectories: c.cfg.AllowedDirs,
			},
		},
	}); err != nil {
		_ = conn.Close()
		return fmt.Errorf("sending registration: %w", err)
	}

	c.setConnected(true)
	c.logger.Info("connected to beluga",
		"address", c.cfg.BelugaAddress,
		"host_id", c.cfg.HostID,
	)

	return nil
}

// Run starts the receive loop. It blocks until the context is cancelled
// or the stream encounters a fatal error. Auto-reconnects on failure.
func (c *Client) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancelFn = cancel
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := c.Connect(ctx); err != nil {
			c.logger.Error("connection failed, retrying",
				"error", err,
				"retry_in", c.cfg.ReconnectInterval,
			)
			c.setConnected(false)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.cfg.ReconnectInterval):
				continue
			}
		}

		// Receive loop
		err := c.receiveLoop(ctx)
		c.setConnected(false)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			c.logger.Error("stream error, reconnecting", "error", err)
		}
	}
}

// receiveLoop reads commands from the stream and dispatches them.
func (c *Client) receiveLoop(ctx context.Context) error {
	for {
		cmd, err := c.stream.Recv()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("stream closed by server")
			}
			return fmt.Errorf("receiving command: %w", err)
		}

		switch payload := cmd.Payload.(type) {
		case *sdk.BelugaCommand_ExecuteCommand:
			go c.handleExecuteCommand(ctx, payload.ExecuteCommand)
		case *sdk.BelugaCommand_SyncDirectory:
			go c.handleSyncDirectory(ctx, payload.SyncDirectory)
		default:
			c.logger.Warn("unknown command type", "payload", payload)
		}
	}
}

// handleExecuteCommand runs a command and sends the result back.
func (c *Client) handleExecuteCommand(ctx context.Context, req *sdk.ExecuteCommandRequest) {
	cmdName := req.Command.String()
	c.logger.Info("executing command",
		"request_id", req.RequestId,
		"command", cmdName,
		"args", req.Args,
	)

	// Map proto CommandType to string
	cmdStr, ok := protoCmdToString(req.Command)
	if !ok {
		c.sendCommandOutput(req.RequestId, 1, "", fmt.Sprintf("unknown command: %v", req.Command), false)
		return
	}

	// Execute with timeout override if specified
	execTimeout := c.executor.GetTimeout()
	if req.TimeoutSeconds > 0 {
		execTimeout = time.Duration(req.TimeoutSeconds) * time.Second
	}
	execCtx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	result, err := c.executor.Execute(execCtx, cmdStr, req.Args, req.WorkingDir)
	if err != nil {
		c.sendCommandOutput(req.RequestId, 1, "", err.Error(), false)
		return
	}

	c.sendCommandOutput(req.RequestId, int32(result.ExitCode), result.Stdout, result.Stderr, result.Timeout)
}

// handleSyncDirectory syncs a directory and sends it back in chunks.
func (c *Client) handleSyncDirectory(ctx context.Context, req *sdk.SyncDirectoryRequest) {
	c.logger.Info("syncing directory",
		"request_id", req.RequestId,
		"directory", req.Directory,
	)

	result, err := remorasync.Directory(req.Directory, c.cfg.AllowedDirs)
	if err != nil {
		// Send error as a done chunk
		c.sendNow(&sdk.RemotaMessage{
			Payload: &sdk.RemotaMessage_DirectoryChunk{
				DirectoryChunk: &sdk.DirectoryChunk{
					RequestId: req.RequestId,
					Data: &sdk.DirectoryChunk_Done{
						Done: true,
					},
				},
			},
		})
		c.logger.Error("directory sync failed", "error", err)
		return
	}

	// Send file info entries
	for _, fi := range result.Files {
		c.sendNow(&sdk.RemotaMessage{
			Payload: &sdk.RemotaMessage_DirectoryChunk{
				DirectoryChunk: &sdk.DirectoryChunk{
					RequestId: req.RequestId,
					Data: &sdk.DirectoryChunk_FileInfo{
						FileInfo: &sdk.FileInfo{
							Path:           fi.Path,
							Size:           fi.Size,
							ChecksumSha256: fi.ChecksumSHA,
						},
					},
				},
			},
		})
	}

	// Send tar data in chunks (64KB each)
	const chunkSize = 64 * 1024
	for i := 0; i < len(result.TarData); i += chunkSize {
		end := i + chunkSize
		if end > len(result.TarData) {
			end = len(result.TarData)
		}

		c.sendNow(&sdk.RemotaMessage{
			Payload: &sdk.RemotaMessage_DirectoryChunk{
				DirectoryChunk: &sdk.DirectoryChunk{
					RequestId: req.RequestId,
					Data: &sdk.DirectoryChunk_Chunk{
						Chunk: result.TarData[i:end],
					},
				},
			},
		})
	}

	// Send done signal
	c.sendNow(&sdk.RemotaMessage{
		Payload: &sdk.RemotaMessage_DirectoryChunk{
			DirectoryChunk: &sdk.DirectoryChunk{
				RequestId: req.RequestId,
				Data: &sdk.DirectoryChunk_Done{
					Done: true,
				},
			},
		},
	})
}

// sendCommandOutput sends a CommandOutput message back to beluga.
func (c *Client) sendCommandOutput(requestID string, exitCode int32, stdout, stderr string, timeout bool) {
	c.sendNow(&sdk.RemotaMessage{
		Payload: &sdk.RemotaMessage_CommandOutput{
			CommandOutput: &sdk.CommandOutput{
				RequestId: requestID,
				ExitCode:  exitCode,
				Stdout:    stdout,
				Stderr:    stderr,
				Timeout:   timeout,
			},
		},
	})
}

// SendHeartbeat sends a heartbeat to beluga.
func (c *Client) SendHeartbeat() error {
	return c.send(&sdk.RemotaMessage{
		Payload: &sdk.RemotaMessage_Heartbeat{
			Heartbeat: &sdk.Heartbeat{
				TimestampMs: time.Now().UnixMilli(),
			},
		},
	})
}

// IsConnected returns whether the client is currently connected.
func (c *Client) IsConnected() bool {
	c.connectedMu.Lock()
	defer c.connectedMu.Unlock()
	return c.connected
}

func (c *Client) setConnected(v bool) {
	c.connectedMu.Lock()
	defer c.connectedMu.Unlock()
	c.connected = v
}

func (c *Client) send(msg *sdk.RemotaMessage) error {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if c.stream == nil {
		return fmt.Errorf("not connected")
	}
	return c.stream.Send(msg)
}

// sendNow logs and ignores send errors. Used for fire-and-forget messages
// (directory chunks, heartbeats) where a failed send just means the stream
// is broken and will be reconnected.
func (c *Client) sendNow(msg *sdk.RemotaMessage) {
	if err := c.send(msg); err != nil {
		c.logger.Warn("send failed", "error", err)
	}
}

// Close shuts down the client.
func (c *Client) Close() error {
	if c.cancelFn != nil {
		c.cancelFn()
	}
	if c.stream != nil {
		c.stream.CloseSend() //nolint:errcheck
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// protoCmdToString maps a proto CommandType to its string name.
func protoCmdToString(cmd sdk.CommandType) (string, bool) {
	switch cmd {
	case sdk.CommandType_GREP:
		return "grep", true
	case sdk.CommandType_AWK:
		return "awk", true
	case sdk.CommandType_FIND:
		return "find", true
	case sdk.CommandType_CAT:
		return "cat", true
	case sdk.CommandType_READ_FILE:
		return "cat", true // read_file maps to cat
	case sdk.CommandType_TAIL:
		return "tail", true
	case sdk.CommandType_SYSTEMCTL_STATUS:
		return "systemctl", true
	case sdk.CommandType_JOURNALCTL:
		return "journalctl", true
	default:
		return "", false
	}
}
