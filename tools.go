package remora

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	sdk "github.com/aspectrr/beluga-ext-sdk/belugav1"
	"github.com/collinpfeifer/beluga/pkg/tools"
)

// HostTool executes a command on a remote host via a remora daemon.
type HostTool struct {
	name        string
	description string
	parameters  json.RawMessage
	commandType sdk.CommandType
	manager     *Manager
}

func (t *HostTool) Definition() tools.ToolDef {
	return tools.ToolDef{
		Name:        t.name,
		Description: t.description,
		Parameters:  t.parameters,
	}
}

func (t *HostTool) Execute(ctx context.Context, args json.RawMessage, _ tools.ToolContext) (json.RawMessage, error) {
	if os.Getenv("BELUGA_DRY_RUN") == "true" {
		return json.Marshal(map[string]interface{}{
			"stdout":    "dry-run: command would execute on remote host",
			"stderr":    "",
			"exit_code": 0,
		})
	}

	var params struct {
		Host       string   `json:"host"`
		Args       []string `json:"args"`
		WorkingDir string   `json:"working_dir,omitempty"`
	}
	if err := json.Unmarshal(args, &params); err != nil {
		return nil, fmt.Errorf("parsing args: %w", err)
	}

	if params.Host == "" {
		return nil, fmt.Errorf("host is required — specify which daemon to route to")
	}

	output, err := t.manager.SendCommandAndWait(ctx, params.Host, &sdk.ExecuteCommandRequest{
		Command:    t.commandType,
		Args:       params.Args,
		WorkingDir: params.WorkingDir,
	})
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]interface{}{
		"stdout":    output.Stdout,
		"stderr":    output.Stderr,
		"exit_code": output.ExitCode,
		"timeout":   output.Timeout,
	})
}

// --- host_list_daemons ---

type ListDaemonsTool struct {
	manager *Manager
}

func (t *ListDaemonsTool) Definition() tools.ToolDef {
	return tools.ToolDef{
		Name:        "host_list_daemons",
		Description: "List all connected remora daemons on remote hosts. Returns host ID, hostname, and allowed directories for each daemon. Use this to discover which hosts are available before running remote commands.",
		Parameters:  json.RawMessage(`{"type":"object","properties":{},"required":[]}`),
	}
}

func (t *ListDaemonsTool) Execute(ctx context.Context, _ json.RawMessage, _ tools.ToolContext) (json.RawMessage, error) {
	if os.Getenv("BELUGA_DRY_RUN") == "true" {
		return json.Marshal(map[string]interface{}{
			"daemons": []map[string]interface{}{
				{"host_id": "dry-run-host", "hostname": "dry-run-host.example.com", "allowed_dirs": []string{"/var/log"}},
			},
			"count": 1,
		})
	}

	daemons := t.manager.ListConnectedDaemons()
	return json.Marshal(map[string]interface{}{
		"daemons": daemons,
		"count":   len(daemons),
	})
}

// --- Registration ---

// RegisterHostTools registers all host tools for remora.
func RegisterHostTools(registry *tools.Registry, manager *Manager) error {
	hostTools := []HostTool{
		{
			name:        "host_exec",
			description: "Execute a whitelisted command on a remote host via its remora daemon. Commands are restricted to read-only operations (grep, awk, find, cat, tail, systemctl status, journalctl).",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon (use host_list_daemons to discover)"},"args":{"type":"array","items":{"type":"string"},"description":"Command and arguments (e.g. [\"grep\", \"-r\", \"error\", \"/var/log/\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host (must be within allowed directories)"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_GREP, // will be overridden by args
			manager:     manager,
		},
		{
			name:        "host_grep",
			description: "Run grep on a remote host. Searches file contents for matching patterns. Safer than host_exec — constrains the command to grep only.",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon"},"args":{"type":"array","items":{"type":"string"},"description":"Grep arguments (e.g. [\"-r\", \"-i\", \"error\", \"/var/log/app/\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_GREP,
			manager:     manager,
		},
		{
			name:        "host_cat",
			description: "Read a file on a remote host. Returns the full file contents. The file path must be within the daemon's allowed directories.",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon"},"args":{"type":"array","items":{"type":"string"},"description":"File path(s) to read (e.g. [\"/etc/logstash/logstash.yml\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_CAT,
			manager:     manager,
		},
		{
			name:        "host_tail",
			description: "Tail a file on a remote host. Returns the last N lines. Useful for viewing recent log entries.",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon"},"args":{"type":"array","items":{"type":"string"},"description":"Tail arguments (e.g. [\"-n\", \"100\", \"/var/log/app/app.log\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_TAIL,
			manager:     manager,
		},
		{
			name:        "host_find",
			description: "Find files on a remote host. Searches for files matching criteria in allowed directories.",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon"},"args":{"type":"array","items":{"type":"string"},"description":"Find arguments (e.g. [\"/var/log\", \"-name\", \"*.log\", \"-mtime\", \"-1\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_FIND,
			manager:     manager,
		},
		{
			name:        "host_journalctl",
			description: "Read systemd journal logs on a remote host. Query service logs, filter by time, unit, or priority. Always read-only.",
			parameters:  json.RawMessage(`{"type":"object","properties":{"host":{"type":"string","description":"Host ID of the remote daemon"},"args":{"type":"array","items":{"type":"string"},"description":"Journalctl arguments (e.g. [\"-u\", \"logstash\", \"-n\", \"100\", \"--no-pager\"])"},"working_dir":{"type":"string","description":"Working directory on the remote host"}},"required":["host","args"]}`),
			commandType: sdk.CommandType_JOURNALCTL,
			manager:     manager,
		},
	}

	for i := range hostTools {
		if err := registry.Register(&hostTools[i]); err != nil {
			return err
		}
	}

	// Register the list daemons tool separately (different type).
	if err := registry.Register(&ListDaemonsTool{manager: manager}); err != nil {
		return err
	}

	return nil
}
