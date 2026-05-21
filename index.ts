// ── Remora Extension ──────────────────────────────────────────
// Ported from Go beluga-ext-remora (server-side only).
// The daemon binary (cmd/remora/, internal/) stays in Go.
//
// Manages connections from remora daemons on remote hosts.
// Registers RemoraService on ext_host's gRPC server.
// Provides 7 host tools for remote command execution.

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import { randomUUID } from "crypto";
import type {
	Extension,
	ExtensionContext,
	Tool,
	ToolDef,
	ToolContext,
} from "@beluga/sdk";
import type { GRPCProvider } from "@beluga/ext-host";

// ── Proto loading ──────────────────────────────────────────────

const PROTO_PATH = resolve(dirname(fileURLToPath(import.meta.url)), "proto", "remora.proto");

const packageDef = protoLoader.loadSync(PROTO_PATH, {
	keepCase: true,
	longs: String,
	enums: String,
	defaults: true,
	oneofs: true,
});

const belugaProto = grpc.loadPackageDefinition(packageDef).beluga as unknown as {
	v1: {
		RemoraService: grpc.ServiceClientConstructor;
	};
};

// ── Types ──────────────────────────────────────────────────────

interface DaemonConnection {
	hostId: string;
	hostname: string;
	allowedDirs: string[];
	stream: grpc.ServerDuplexStream<unknown, unknown>;
}

interface DaemonInfo {
	host_id: string;
	hostname: string;
	allowed_dirs: string[];
}

interface PendingCommand {
	response: {
		resolve: (output: Record<string, unknown>) => void;
		reject: (err: Error) => void;
	};
}

// ── Manager ────────────────────────────────────────────────────

class RemoraManager {
	private daemons = new Map<string, DaemonConnection>();
	private pending = new Map<string, PendingCommand>();
	private logger: import("pino").Logger;

	constructor(logger: import("pino").Logger) {
		this.logger = logger;
	}

	registerDaemon(conn: DaemonConnection): void {
		const old = this.daemons.get(conn.hostId);
		if (old) {
			this.logger.warn({ hostId: conn.hostId }, "replacing existing daemon connection");
		}
		this.daemons.set(conn.hostId, conn);
		this.logger.info({ hostId: conn.hostId, hostname: conn.hostname }, "daemon registered");
	}

	unregisterDaemon(hostId: string): void {
		if (this.daemons.delete(hostId)) {
			this.logger.info({ hostId }, "daemon unregistered");
		}
	}

	getDaemon(hostId: string): DaemonConnection {
		const conn = this.daemons.get(hostId);
		if (!conn) throw new Error(`daemon "${hostId}" is not connected`);
		return conn;
	}

	listDaemons(): DaemonInfo[] {
		return Array.from(this.daemons.values()).map((c) => ({
			host_id: c.hostId,
			hostname: c.hostname,
			allowed_dirs: c.allowedDirs,
		}));
	}

	async sendCommandAndWait(
		hostId: string,
		commandType: number,
		args: string[],
		workingDir?: string,
	): Promise<Record<string, unknown>> {
		if (!hostId) throw new Error("host is required: no default daemon host");

		const requestId = randomUUID();
		const conn = this.getDaemon(hostId);

		return new Promise((resolve, reject) => {
			this.pending.set(requestId, { response: { resolve, reject } });

			// Send command through the stream
			conn.stream.write({
				payload: {
					execute_command: {
						request_id: requestId,
						command: commandType,
						args,
						working_dir: workingDir || "",
						timeout_seconds: 30,
					},
				},
			});

			// Timeout
			setTimeout(() => {
				if (this.pending.delete(requestId)) {
					reject(new Error(`command to ${hostId} timed out`));
				}
			}, 60_000);
		});
	}

	handleCommandOutput(output: Record<string, unknown>): void {
		const requestId = output.request_id as string;
		const pending = this.pending.get(requestId);
		if (!pending) {
			this.logger.warn({ requestId }, "received output for unknown request");
			return;
		}
		this.pending.delete(requestId);
		pending.response.resolve({
			stdout: output.stdout ?? "",
			stderr: output.stderr ?? "",
			exit_code: output.exit_code ?? 1,
			timeout: output.timeout ?? false,
		});
	}
}

// ── gRPC Service ───────────────────────────────────────────────

class RemoraServiceServer {
	private manager: RemoraManager;
	private logger: import("pino").Logger;

	constructor(manager: RemoraManager, logger: import("pino").Logger) {
		this.manager = manager;
		this.logger = logger;
	}

	/** RemoraService.Connect — bidirectional stream from daemon */
	connect(call: grpc.ServerDuplexStream<unknown, unknown>): void {
		let hostId = "";

		const cleanup = () => {
			if (hostId) this.manager.unregisterDaemon(hostId);
		};

		call.on("data", (msg: unknown) => {
			const m = msg as Record<string, unknown>;
			const payload = (m.payload ?? m) as Record<string, unknown>;

			if (payload.registration) {
				const reg = payload.registration as Record<string, unknown>;
				hostId = reg.host_id as string;

				this.manager.registerDaemon({
					hostId,
					hostname: reg.hostname as string,
					allowedDirs: (reg.allowed_directories as string[]) ?? [],
					stream: call,
				});

				this.logger.info({ hostId, hostname: reg.hostname }, "remora daemon connected");
			}

			if (payload.heartbeat) {
				this.logger.debug({ hostId }, "heartbeat from remora daemon");
			}

			if (payload.command_output) {
				this.manager.handleCommandOutput(payload.command_output as Record<string, unknown>);
			}

			if (payload.directory_chunk) {
				this.logger.debug({ hostId }, "directory chunk from remora daemon");
			}
		});

		call.on("end", cleanup);
		call.on("error", cleanup);
	}
}

// ── Tools ──────────────────────────────────────────────────────

function dryRun(): boolean {
	return process.env.BELUGA_DRY_RUN === "true";
}

const HOST_TOOL_PARAMS = {
	type: "object",
	properties: {
		host: {
			type: "string",
			description: "Host ID of the remote daemon (use host_list_daemons to discover)",
		},
		args: {
			type: "array",
			items: { type: "string" },
			description: "Command and arguments",
		},
		working_dir: {
			type: "string",
			description: "Working directory on the remote host",
		},
	},
	required: ["host", "args"],
};

class HostTool implements Tool {
	private toolName: string;
	private desc: string;
	private commandType: number;
	private manager: RemoraManager;

	constructor(name: string, description: string, commandType: number, manager: RemoraManager) {
		this.toolName = name;
		this.desc = description;
		this.commandType = commandType;
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: this.toolName,
			description: this.desc,
			parameters: HOST_TOOL_PARAMS,
		};
	}

	async execute(args: Record<string, unknown>, _ctx: ToolContext): Promise<Record<string, unknown>> {
		if (dryRun()) {
			return {
				stdout: "dry-run: command would execute on remote host",
				stderr: "",
				exit_code: 0,
			};
		}

		const host = args.host as string;
		const cmdArgs = args.args as string[];
		const workingDir = args.working_dir as string | undefined;

		if (!host) throw new Error("host is required — specify which daemon to route to");

		return this.manager.sendCommandAndWait(host, this.commandType, cmdArgs, workingDir);
	}
}

class ListDaemonsTool implements Tool {
	private manager: RemoraManager;

	constructor(manager: RemoraManager) {
		this.manager = manager;
	}

	definition(): ToolDef {
		return {
			name: "host_list_daemons",
			description:
				"List all connected remora daemons on remote hosts. Returns host ID, hostname, and allowed directories for each daemon.",
			parameters: { type: "object", properties: {}, required: [] },
		};
	}

	async execute(_args: Record<string, unknown>, _ctx: ToolContext): Promise<Record<string, unknown>> {
		if (dryRun()) {
			return {
				daemons: [
					{
						host_id: "dry-run-host",
						hostname: "dry-run-host.example.com",
						allowed_dirs: ["/var/log"],
					},
				],
				count: 1,
			};
		}
		const daemons = this.manager.listDaemons();
		return { daemons, count: daemons.length };
	}
}

// ── Extension ──────────────────────────────────────────────────

class RemoraExtension implements Extension {
	name = "remora";
	private manager?: RemoraManager;

	async init(ctx: ExtensionContext): Promise<void> {
		// Require ext_host
		const provider = ctx.shared.grpcProvider as GRPCProvider | undefined;
		if (!provider) {
			throw new Error("ext_host extension is required for remora — enable ext_host first");
		}

		this.manager = new RemoraManager(ctx.logger);

		// Register RemoraService on ext_host's shared gRPC server
		const server = new RemoraServiceServer(this.manager, ctx.logger);
		provider.registerService(belugaProto.v1.RemoraService, {
			connect: server.connect.bind(server),
		});

		// Register host tools
		// CommandType enum: GREP=1, AWK=2, FIND=3, CAT=4, READ_FILE=5, TAIL=6, SYSTEMCTL_STATUS=7, JOURNALCTL=8
		const hostTools: Array<[string, string, number]> = [
			["host_exec", "Execute a whitelisted command on a remote host via its remora daemon. Commands restricted to read-only operations.", 1],
			["host_grep", "Run grep on a remote host. Searches file contents for matching patterns.", 1],
			["host_cat", "Read a file on a remote host. Returns the full file contents.", 4],
			["host_tail", "Tail a file on a remote host. Returns the last N lines.", 6],
			["host_find", "Find files on a remote host in allowed directories.", 3],
			["host_journalctl", "Read systemd journal logs on a remote host. Always read-only.", 8],
		];

		for (const [name, desc, cmdType] of hostTools) {
			ctx.registry.register(new HostTool(name, desc, cmdType, this.manager));
		}

		ctx.registry.register(new ListDaemonsTool(this.manager));

		ctx.logger.info("remora extension initialized");
	}

	async start(_signal: AbortSignal): Promise<void> {
		// gRPC service handled by ext_host's server
	}

	async stop(): Promise<void> {
		// Daemon connections cleaned up by gRPC stream lifecycle
	}
}

export default new RemoraExtension();
