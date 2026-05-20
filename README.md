# beluga-ext-remora

Remora is the Beluga remote host daemon extension. It has two components:

1. **Extension** (runs inside Beluga) — manages connections from remora daemons on remote hosts, provides host tools for the agent
2. **Daemon binary** (`cmd/remora/`) — runs on remote hosts, executes whitelisted read-only commands and syncs directories back to Beluga via gRPC

## Install into Beluga

```bash
beluga extend install github.com/collinpfeifer/beluga-ext-remora
```

Requires the `ext_host` extension to be enabled (provides the gRPC server).

## Host Tools (Agent-facing)

After installation, the agent gains these tools:

| Tool | Description |
|------|-------------|
| `host_exec` | Execute a whitelisted command on a remote host |
| `host_grep` | Run grep on a remote host |
| `host_cat` | Read a file on a remote host |
| `host_tail` | Tail a file on a remote host |
| `host_find` | Find files on a remote host |
| `host_journalctl` | Read systemd journal logs on a remote host |
| `host_list_daemons` | List all connected remora daemons |

## Running the Daemon

Build and deploy `cmd/remora/` to remote hosts:

```bash
go build -o remora ./cmd/remora
```

Configure via `/etc/beluga-remora/config.yaml`:

```yaml
beluga:
  address: "beluga-host:50051"
  reconnect_interval: 5s

allowed_directories:
  - /var/log
  - /etc/app

allowed_commands:
  - grep
  - cat
  - tail
  - find
  - awk
  - systemctl
  - journalctl

command_timeout: 30s
log_level: info
```

Run:
```bash
./remora -config /etc/beluga-remora/config.yaml
```

## Architecture

```
┌─────────────┐    gRPC stream    ┌──────────────┐
│  remora     │ ◄───────────────► │   Beluga     │
│  (remote    │                   │   ext_host   │
│   host)     │                   │   + remora   │
└─────────────┘                   │   extension  │
                                  └──────────────┘
```

The daemon connects to Beluga's gRPC server (provided by ext_host), registers itself, and receives commands. All commands are whitelisted and sandboxed — only read-only operations are permitted.
