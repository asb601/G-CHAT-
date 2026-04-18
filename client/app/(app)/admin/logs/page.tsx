"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import {
  FileText,
  Search,
  RefreshCw,
  AlertCircle,
  Info,
  AlertTriangle,
  ChevronDown,
  Clock,
  Upload,
  Zap,
  CheckCircle,
  XCircle,
  Loader2,
} from "lucide-react";
import { apiFetch } from "@/lib/auth";
import { useAuth } from "@/components/auth-provider";
import { cn } from "@/lib/utils";

/* ── types ───────────────────────────────────────────────────────────────── */

interface LogFile {
  name: string;
  size_kb: number;
}

interface LogEntry {
  [key: string]: unknown;
  event?: string;
  level?: string;
  timestamp?: string;
  raw?: string;
}

interface LogResponse {
  file: string;
  total_lines: number;
  returned: number;
  lines: LogEntry[];
}

/* ── helpers ─────────────────────────────────────────────────────────────── */

const LEVEL_COLORS: Record<string, string> = {
  error: "bg-red-500/10 text-red-400 border-red-500/20",
  warning: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20",
  info: "bg-blue-500/10 text-blue-300 border-blue-500/20",
  debug: "bg-zinc-500/10 text-zinc-400 border-zinc-500/20",
};

const LEVEL_ICONS: Record<string, typeof Info> = {
  error: AlertCircle,
  warning: AlertTriangle,
  info: Info,
  debug: Info,
};

function formatTimestamp(ts: string | undefined): string {
  if (!ts) return "";
  try {
    const d = new Date(ts);
    return d.toLocaleTimeString("en-US", {
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  } catch {
    return ts;
  }
}

function formatDuration(ms: number | undefined): string {
  if (ms === undefined || ms === null) return "";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

/* ── log line component ──────────────────────────────────────────────────── */

function LogLine({ entry }: { entry: LogEntry }) {
  const [expanded, setExpanded] = useState(false);
  const level = (entry.level || "info") as string;
  const colors = LEVEL_COLORS[level] || LEVEL_COLORS.info;
  const Icon = LEVEL_ICONS[level] || Info;

  // Raw text line
  if (entry.raw) {
    return (
      <div className="px-3 py-1.5 font-mono text-xs text-muted-foreground border-b border-border/50">
        {entry.raw}
      </div>
    );
  }

  const event = entry.event || "";
  const timestamp = formatTimestamp(entry.timestamp as string);
  const durationMs = entry.duration_ms as number | undefined;
  const step = entry.step as string | undefined;
  const status = entry.status as string | undefined;
  const traceId = entry.trace_id as string | undefined;

  // Keys to hide from detail view
  const hideKeys = new Set([
    "event",
    "level",
    "timestamp",
    "duration_ms",
    "step",
    "status",
    "trace_id",
    "pipeline",
  ]);
  const extraKeys = Object.keys(entry).filter(
    (k) => !hideKeys.has(k) && entry[k] !== undefined && entry[k] !== null
  );

  return (
    <div
      className={cn(
        "border-b border-border/50 hover:bg-surface-raised/50 transition-colors cursor-pointer",
        expanded && "bg-surface-raised/30"
      )}
      onClick={() => extraKeys.length > 0 && setExpanded(!expanded)}
    >
      <div className="flex items-center gap-2 px-3 py-1.5">
        {/* Time */}
        <span className="text-[10px] text-muted-foreground font-mono w-16 shrink-0">
          {timestamp}
        </span>

        {/* Level badge */}
        <span
          className={cn(
            "inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium border shrink-0",
            colors
          )}
        >
          <Icon className="w-3 h-3" />
          {level}
        </span>

        {/* Step badge */}
        {step && (
          <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-primary/10 text-primary border border-primary/20 shrink-0">
            {step}
          </span>
        )}

        {/* Event */}
        <span className="text-xs font-medium text-foreground truncate">
          {event}
        </span>

        {/* Status */}
        {status && (
          <span
            className={cn(
              "px-1.5 py-0.5 rounded text-[10px] font-medium shrink-0",
              status === "done" || status === "started"
                ? "bg-green-500/10 text-green-400"
                : status === "failed"
                ? "bg-red-500/10 text-red-400"
                : "bg-zinc-500/10 text-zinc-400"
            )}
          >
            {status}
          </span>
        )}

        {/* Duration */}
        {durationMs !== undefined && (
          <span className="flex items-center gap-0.5 text-[10px] text-muted-foreground shrink-0 ml-auto">
            <Clock className="w-3 h-3" />
            {formatDuration(durationMs)}
          </span>
        )}

        {/* Trace ID */}
        {traceId && (
          <span className="text-[10px] text-muted-foreground font-mono shrink-0 hidden lg:block">
            {traceId}
          </span>
        )}

        {/* Expand indicator */}
        {extraKeys.length > 0 && (
          <ChevronDown
            className={cn(
              "w-3 h-3 text-muted-foreground transition-transform shrink-0",
              expanded && "rotate-180"
            )}
          />
        )}
      </div>

      {/* Expanded details */}
      {expanded && extraKeys.length > 0 && (
        <div className="px-3 pb-2 pl-[7.5rem]">
          <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 text-[11px]">
            {extraKeys.map((key) => (
              <div key={key} className="contents">
                <span className="text-muted-foreground font-mono">{key}</span>
                <span className="text-foreground font-mono break-all">
                  {typeof entry[key] === "object"
                    ? JSON.stringify(entry[key])
                    : String(entry[key])}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/* ── main page ───────────────────────────────────────────────────────────── */

type PageView = "logs" | "performance";

interface FileTiming {
  file_id: string;
  name: string;
  size: number;
  ingest_status: string;
  uploaded_at: string | null;
  upload_secs: number | null;
  ingested_at: string | null;
  ingestion_secs: number | null;
  total_secs: number | null;
  parquet_status: string | null;
  parquet_started_at: string | null;
  parquet_completed_at: string | null;
  parquet_secs: number | null;
  parquet_error: string | null;
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatSecs(secs: number | null): string {
  if (secs === null || secs === undefined) return "—";
  if (secs < 1) return `${Math.round(secs * 1000)}ms`;
  if (secs < 60) return `${secs.toFixed(1)}s`;
  const m = Math.floor(secs / 60);
  const s = Math.round(secs % 60);
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

function formatDateTime(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("en-US", {
      month: "short", day: "numeric",
      hour: "2-digit", minute: "2-digit", second: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}

function StatusBadge({ status }: { status: string }) {
  const map: Record<string, { color: string; icon: typeof CheckCircle }> = {
    ingested: { color: "bg-green-500/10 text-green-400 border-green-500/20", icon: CheckCircle },
    done: { color: "bg-green-500/10 text-green-400 border-green-500/20", icon: CheckCircle },
    failed: { color: "bg-red-500/10 text-red-400 border-red-500/20", icon: XCircle },
    running: { color: "bg-blue-500/10 text-blue-400 border-blue-500/20", icon: Loader2 },
    pending: { color: "bg-yellow-500/10 text-yellow-400 border-yellow-500/20", icon: Clock },
    not_ingested: { color: "bg-zinc-500/10 text-zinc-400 border-zinc-500/20", icon: Clock },
  };
  const conf = map[status] || map.not_ingested;
  const Icon = conf.icon;
  return (
    <span className={cn("inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-medium border", conf.color)}>
      <Icon className={cn("w-3 h-3", status === "running" && "animate-spin")} />
      {status}
    </span>
  );
}

function PerformancePanel() {
  const [timings, setTimings] = useState<FileTiming[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchTimings = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiFetch("/api/logs/file-timings?limit=50");
      if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
      const data = await res.json();
      setTimings(data.files);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to fetch timings");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchTimings(); }, [fetchTimings]);

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="border-b border-border px-4 py-2 flex items-center gap-2 shrink-0">
        <span className="text-xs text-muted-foreground">
          {timings.length} file{timings.length !== 1 && "s"} — most recent first
        </span>
        <button
          onClick={fetchTimings}
          disabled={loading}
          className="ml-auto p-1.5 rounded bg-surface-raised text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
        >
          <RefreshCw className={cn("w-3.5 h-3.5", loading && "animate-spin")} />
        </button>
      </div>

      {error && (
        <div className="mx-4 mt-2 px-3 py-2 rounded bg-red-500/10 border border-red-500/20 text-red-400 text-xs flex items-center gap-2">
          <AlertCircle className="w-3.5 h-3.5 shrink-0" />
          {error}
        </div>
      )}

      {/* Table */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-surface z-10">
            <tr className="border-b border-border text-left text-muted-foreground">
              <th className="px-4 py-2 font-medium">File</th>
              <th className="px-3 py-2 font-medium">Size</th>
              <th className="px-3 py-2 font-medium">Uploaded</th>
              <th className="px-3 py-2 font-medium text-center">Upload Time</th>
              <th className="px-3 py-2 font-medium">Status</th>
              <th className="px-3 py-2 font-medium text-center">Ingestion Time</th>
              <th className="px-3 py-2 font-medium text-center">Total Time</th>
              <th className="px-3 py-2 font-medium">Parquet</th>
              <th className="px-3 py-2 font-medium text-center">Parquet Time</th>
            </tr>
          </thead>
          <tbody>
            {timings.map((t) => (
              <tr key={t.file_id} className="border-b border-border/50 hover:bg-surface-raised/50 transition-colors">
                <td className="px-4 py-2 text-foreground font-medium truncate max-w-[200px]" title={t.name}>
                  {t.name}
                </td>
                <td className="px-3 py-2 text-muted-foreground whitespace-nowrap">
                  {formatBytes(t.size)}
                </td>
                <td className="px-3 py-2 text-muted-foreground whitespace-nowrap">
                  {formatDateTime(t.uploaded_at)}
                </td>
                <td className="px-3 py-2 text-center">
                  {t.upload_secs !== null ? (
                    <span className="flex items-center justify-center gap-1 text-foreground font-mono">
                      <Upload className="w-3 h-3 text-blue-400" />
                      {formatSecs(t.upload_secs)}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-3 py-2">
                  <StatusBadge status={t.ingest_status} />
                </td>
                <td className="px-3 py-2 text-center">
                  {t.ingestion_secs !== null ? (
                    <span className="flex items-center justify-center gap-1 text-foreground font-mono">
                      <Zap className="w-3 h-3 text-yellow-400" />
                      {formatSecs(t.ingestion_secs)}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-3 py-2 text-center">
                  {t.total_secs !== null ? (
                    <span className="flex items-center justify-center gap-1 text-foreground font-mono font-semibold">
                      <Clock className="w-3 h-3 text-green-400" />
                      {formatSecs(t.total_secs)}
                    </span>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-3 py-2">
                  {t.parquet_status ? (
                    <StatusBadge status={t.parquet_status} />
                  ) : (
                    <span className="text-muted-foreground text-[10px]">—</span>
                  )}
                </td>
                <td className="px-3 py-2 text-center">
                  {t.parquet_secs !== null ? (
                    <span className="flex items-center justify-center gap-1 text-foreground font-mono">
                      <Clock className="w-3 h-3 text-muted-foreground" />
                      {formatSecs(t.parquet_secs)}
                    </span>
                  ) : t.parquet_error ? (
                    <span className="text-red-400 text-[10px] truncate max-w-[120px]" title={t.parquet_error}>Error</span>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
              </tr>
            ))}
            {timings.length === 0 && !loading && (
              <tr>
                <td colSpan={9} className="px-4 py-8 text-center text-muted-foreground">
                  No files found
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const LOG_FILES = [
  { name: "ai_pipeline.log", label: "AI Pipeline", description: "Ingestion & chat" },
  { name: "system.log", label: "System", description: "Upload, auth, blob" },
  { name: "llm_calls.log", label: "LLM Calls", description: "Token usage & timing" },
  { name: "costs.log", label: "Costs", description: "Billing events" },
];

export default function AdminLogsPage() {
  const { user } = useAuth();
  const [pageView, setPageView] = useState<PageView>("logs");
  const [activeFile, setActiveFile] = useState("ai_pipeline.log");
  const [lines, setLines] = useState<LogEntry[]>([]);
  const [totalLines, setTotalLines] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [lineCount, setLineCount] = useState(200);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const logContainerRef = useRef<HTMLDivElement>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchLogs = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      let url: string;
      if (searchQuery.trim()) {
        url = `/api/logs/${activeFile}/search?q=${encodeURIComponent(searchQuery)}&lines=${lineCount}`;
      } else {
        url = `/api/logs/${activeFile}?lines=${lineCount}`;
      }
      const res = await apiFetch(url);
      if (!res.ok) {
        const body = await res.text();
        throw new Error(`${res.status}: ${body}`);
      }
      const data: LogResponse = await res.json();
      setLines(searchQuery.trim() ? data.lines.map((l: any) => l.data || l) : data.lines);
      setTotalLines(data.total_lines);

      // Scroll to bottom
      requestAnimationFrame(() => {
        if (logContainerRef.current) {
          logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
        }
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to fetch logs");
    } finally {
      setLoading(false);
    }
  }, [activeFile, lineCount, searchQuery]);

  // Fetch on file/filter change
  useEffect(() => {
    fetchLogs();
  }, [fetchLogs]);

  // Auto-refresh
  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(fetchLogs, 5000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoRefresh, fetchLogs]);

  if (!user?.is_admin) {
    return (
      <div className="flex items-center justify-center h-full">
        <p className="text-muted-foreground">Admin access required</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="border-b border-border px-4 py-3 shrink-0 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-foreground">Server Logs</h1>
          <p className="text-xs text-muted-foreground mt-0.5">
            Real-time server logs — ingestion pipeline, LLM calls, system events
          </p>
        </div>
        <div className="flex gap-1">
          <button
            onClick={() => setPageView("logs")}
            className={cn(
              "px-3 py-1.5 rounded text-xs font-medium transition-colors",
              pageView === "logs"
                ? "bg-primary text-primary-foreground"
                : "bg-surface-raised text-muted-foreground hover:text-foreground"
            )}
          >
            <FileText className="w-3.5 h-3.5 inline mr-1" />
            Logs
          </button>
          <button
            onClick={() => setPageView("performance")}
            className={cn(
              "px-3 py-1.5 rounded text-xs font-medium transition-colors",
              pageView === "performance"
                ? "bg-primary text-primary-foreground"
                : "bg-surface-raised text-muted-foreground hover:text-foreground"
            )}
          >
            <Zap className="w-3.5 h-3.5 inline mr-1" />
            Performance
          </button>
        </div>
      </div>

      {pageView === "performance" ? (
        <PerformancePanel />
      ) : (
      <>

      {/* Toolbar */}
      <div className="border-b border-border px-4 py-2 flex flex-wrap items-center gap-2 shrink-0">
        {/* Log file tabs */}
        <div className="flex gap-1">
          {LOG_FILES.map((f) => (
            <button
              key={f.name}
              onClick={() => {
                setActiveFile(f.name);
                setSearchQuery("");
              }}
              className={cn(
                "px-2.5 py-1 rounded text-xs font-medium transition-colors",
                activeFile === f.name
                  ? "bg-primary text-primary-foreground"
                  : "bg-surface-raised text-muted-foreground hover:text-foreground"
              )}
              title={f.description}
            >
              {f.label}
            </button>
          ))}
        </div>

        <div className="w-px h-5 bg-border mx-1 hidden sm:block" />

        {/* Search */}
        <div className="relative flex-1 min-w-[180px] max-w-xs">
          <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search logs..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && fetchLogs()}
            className="w-full pl-7 pr-2 py-1 rounded bg-surface-raised border border-border text-xs text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
          />
        </div>

        {/* Lines selector */}
        <select
          value={lineCount}
          onChange={(e) => setLineCount(Number(e.target.value))}
          className="px-2 py-1 rounded bg-surface-raised border border-border text-xs text-foreground"
        >
          <option value={50}>50 lines</option>
          <option value={200}>200 lines</option>
          <option value={500}>500 lines</option>
          <option value={1000}>1000 lines</option>
        </select>

        {/* Auto refresh toggle */}
        <button
          onClick={() => setAutoRefresh(!autoRefresh)}
          className={cn(
            "px-2 py-1 rounded text-xs font-medium transition-colors flex items-center gap-1",
            autoRefresh
              ? "bg-green-500/15 text-green-400 border border-green-500/30"
              : "bg-surface-raised text-muted-foreground hover:text-foreground"
          )}
        >
          <RefreshCw className={cn("w-3 h-3", autoRefresh && "animate-spin")} />
          {autoRefresh ? "Live" : "Auto"}
        </button>

        {/* Manual refresh */}
        <button
          onClick={fetchLogs}
          disabled={loading}
          className="p-1.5 rounded bg-surface-raised text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
        >
          <RefreshCw className={cn("w-3.5 h-3.5", loading && "animate-spin")} />
        </button>

        {/* Line count */}
        <span className="text-[10px] text-muted-foreground ml-auto hidden sm:block">
          {lines.length} / {totalLines} lines
        </span>
      </div>

      {/* Error */}
      {error && (
        <div className="mx-4 mt-2 px-3 py-2 rounded bg-red-500/10 border border-red-500/20 text-red-400 text-xs flex items-center gap-2">
          <AlertCircle className="w-3.5 h-3.5 shrink-0" />
          {error}
        </div>
      )}

      {/* Log entries */}
      <div
        ref={logContainerRef}
        className="flex-1 overflow-y-auto bg-[#0d1117] font-mono"
      >
        {lines.length === 0 && !loading && (
          <div className="flex flex-col items-center justify-center h-full gap-2 text-muted-foreground">
            <FileText className="w-8 h-8" />
            <p className="text-sm">No log entries found</p>
          </div>
        )}
        {lines.map((entry, i) => (
          <LogLine key={i} entry={entry} />
        ))}
      </div>
      </>
      )}
    </div>
  );
}
