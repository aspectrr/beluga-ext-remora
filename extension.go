package remora

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	sdk "github.com/aspectrr/beluga-ext-sdk/belugav1"
	"github.com/collinpfeifer/beluga/pkg/extension"
	"github.com/collinpfeifer/beluga/pkg/tools"
	"google.golang.org/grpc"
)

// GRPCServiceProvider is the interface that ext_host provides.
// Remora depends on this to register its gRPC service.
type GRPCServiceProvider interface {
	RegisterService(desc *grpc.ServiceDesc, impl interface{})
}

// Extension manages connections from remora daemons running on remote hosts.
// It registers a gRPC service on ext_host's shared server and provides
// host tools that route commands through daemon connections.
//
// Requires ext_host to be enabled — Init fails with a clear error otherwise.
type Extension struct {
	manager *Manager
	logger  *slog.Logger
}

// Name returns the extension identifier.
func (e *Extension) Name() string { return "remora" }

// Init checks that ext_host is available, creates the connection manager,
// registers the gRPC service, and registers host tools.
func (e *Extension) Init(ctx extension.ExtensionContext) error {
	if ctx.GRPC == nil {
		return fmt.Errorf("ext_host extension is required for remora — enable ext_host first")
	}

	provider, ok := ctx.GRPC.(GRPCServiceProvider)
	if !ok {
		return fmt.Errorf("ctx.GRPC does not implement GRPCServiceProvider — ensure ext_host is enabled")
	}

	e.logger = ctx.Logger
	e.manager = NewManager(ctx.Logger)

	// Register gRPC service on ext_host's shared server.
	server := NewRemoraServiceServer(e.manager, ctx.Logger)
	provider.RegisterService(&sdk.RemoraService_ServiceDesc, server)

	// Register host tools.
	if err := RegisterHostTools(ctx.Registry, e.manager); err != nil {
		return fmt.Errorf("registering host tools: %w", err)
	}

	ctx.Logger.Info("remora extension initialized")
	return nil
}

// Start blocks until the context is cancelled.
// The gRPC service is already registered and handled by ext_host's server.
func (e *Extension) Start(ctx context.Context) error {
	e.logger.Info("remora extension started")
	<-ctx.Done()
	return nil
}

// Stop is called on graceful shutdown.
func (e *Extension) Stop(ctx context.Context) error {
	e.logger.Info("remora extension stopped")
	return nil
}

// mustJSON marshals v to JSON, returning an empty object on error.
func mustJSON(v interface{}) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return json.RawMessage(data)
}

// Compile-time check that Extension satisfies the interface.
var _ extension.Extension = (*Extension)(nil)

// Compile-time check that tools.Tool is satisfied.
var _ tools.Tool = (*HostTool)(nil)
var _ tools.Tool = (*ListDaemonsTool)(nil)
