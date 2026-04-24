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
  Activity,
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

type PageView = "logs" | "performance" | "pipeline";

interface FileTiming {
  file_id: string;
  name: string;
  size: number;
  ingest_status: string;
  uploaded_at: string | null;
  upload_secs: number | null;
  ingested_at: string | null;
  ingestion_secs: number | null;
  parquet_status: string | null;
  parquet_secs: number | null;
  processing_secs: number | null;
  total_secs: number | null;
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

/* ── AI Pipeline panel ───────────────────────────────────────────────────── */

/**
 * Map a pipeline event name to a UI step (number, label, colour).
 * Order in the UI mirrors the actual flow of a query.
 */
type StepConfig = {
  num: string;
  label: string;
  color: string; // tailwind text + bg combo for the badge
  short: string; // one-line summary builder key
};

const PIPELINE_STEPS: Record<string, StepConfig> = {
  query_received:        { num: "1", label: "Query Received",   color: "bg-cyan-500/15 text-cyan-300 border-cyan-500/30",       short: "query" },
  catalog_loaded:        { num: "2", label: "Catalog Loaded",   color: "bg-blue-500/15 text-blue-300 border-blue-500/30",       short: "catalog" },
  catalog_empty:         { num: "2", label: "Catalog Empty",    color: "bg-red-500/15 text-red-300 border-red-500/30",          short: "catalog_empty" },
  system_prompt_built:   { num: "3", label: "System Prompt",    color: "bg-violet-500/15 text-violet-300 border-violet-500/30", short: "prompt" },
  search_catalog:        { num: "3", label: "Catalog Search",   color: "bg-violet-500/15 text-violet-300 border-violet-500/30", short: "search" },
  get_file_schema:       { num: "3", label: "File Schema",      color: "bg-violet-500/15 text-violet-300 border-violet-500/30", short: "schema" },
  llm_input:             { num: "4", label: "LLM Input",        color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "llm_in" },
  llm_stream_input:      { num: "4", label: "LLM Input",        color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "llm_in" },
  llm_output:            { num: "4", label: "LLM Decision",     color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "llm_out" },
  llm_stream_output:     { num: "4", label: "LLM Decision",     color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "llm_out" },
  tool_call_start:       { num: "4", label: "Tool Start",       color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "tool_start" },
  tool_call_end:         { num: "4", label: "Tool End",         color: "bg-fuchsia-500/15 text-fuchsia-300 border-fuchsia-500/30", short: "tool_end" },
  sql_execute_start:     { num: "5", label: "SQL Executing",    color: "bg-amber-500/15 text-amber-300 border-amber-500/30",    short: "sql_start" },
  sql_execute_done:      { num: "6", label: "SQL Result",       color: "bg-orange-500/15 text-orange-300 border-orange-500/30", short: "sql_done" },
  sql_execute_error:     { num: "6", label: "SQL Error",        color: "bg-red-500/15 text-red-300 border-red-500/30",          short: "sql_error" },
  inspect_data_format:   { num: "5", label: "Data Sample",      color: "bg-amber-500/15 text-amber-300 border-amber-500/30",    short: "sample" },
  summarise_dataframe_done: { num: "5", label: "Stats",         color: "bg-amber-500/15 text-amber-300 border-amber-500/30",    short: "stats" },
  ingest_llm_prompt:     { num: "i", label: "Ingest Prompt",    color: "bg-zinc-500/15 text-zinc-300 border-zinc-500/30",       short: "ingest_p" },
  ingest_llm_response:   { num: "i", label: "Ingest Reply",     color: "bg-zinc-500/15 text-zinc-300 border-zinc-500/30",       short: "ingest_r" },
  final_answer:          { num: "✓", label: "Final Answer",     color: "bg-emerald-500/15 text-emerald-300 border-emerald-500/40", short: "answer" },
};

function pipelineSummary(ev: LogEntry): string {
  const e = ev.event as string;
  switch (e) {
    case "query_received":      return String(ev.query ?? "");
    case "catalog_loaded":      return `${ev.file_count} files in '${ev.container}' (${ev.parquet_count} parquet, ${ev.relationship_count} relationships)`;
    case "catalog_empty":       return "No files ingested yet — agent cannot answer";
    case "system_prompt_built": return `${ev.catalog_file_count} files | ${ev.parquet_file_count} parquet | rels:${ev.has_relationships ? "yes" : "no"}`;
    case "search_catalog":      return `query="${ev.query}" → ${ev.files_found ?? (Array.isArray(ev.matched_files) ? (ev.matched_files as unknown[]).length : 0)} matches`;
    case "get_file_schema":     return `${ev.blob_path}${ev.found ? "" : " (NOT FOUND)"}`;
    case "llm_input":
    case "llm_stream_input":    return `iteration ${ev.iteration} · ${ev.message_count ?? (Array.isArray(ev.messages) ? (ev.messages as unknown[]).length : 0)} messages`;
    case "llm_output":
    case "llm_stream_output": {
      const tcs = (ev.tool_calls as unknown[]) || [];
      const toks = `${ev.prompt_tokens ?? "?"}+${ev.completion_tokens ?? "?"} tok`;
      if (tcs.length > 0) {
        const names = tcs.map((tc) => (tc as { name?: string }).name ?? "?").join(", ");
        return `iter ${ev.iteration} · ${toks} · → ${names}`;
      }
      return `iter ${ev.iteration} · ${toks} · final answer`;
    }
    case "tool_call_start":     return `${ev.tool} (iter ${ev.iteration})`;
    case "tool_call_end":       return `${ev.tool} done`;
    case "sql_execute_start":   return String(ev.sql ?? "").replace(/\s+/g, " ").slice(0, 120);
    case "sql_execute_done":    return `${ev.rows_returned}/${ev.total_rows} rows · ${ev.duration_ms} ms`;
    case "sql_execute_error":   return String(ev.error ?? "").slice(0, 120);
    case "inspect_data_format": return `${Array.isArray(ev.columns) ? (ev.columns as unknown[]).length : 0} cols · ${Array.isArray(ev.rows) ? (ev.rows as unknown[]).length : 0} sample rows`;
    case "summarise_dataframe_done": return `${ev.row_count} rows · ${ev.column_count} cols · focus=${ev.focus}`;
    case "ingest_llm_prompt":   return `${ev.filename} (~${ev.estimated_prompt_tokens} tok)`;
    case "ingest_llm_response": return `${ev.filename} · ${ev.duration_ms} ms`;
    case "final_answer":        return `${ev.tool_calls} tool calls · ${ev.row_count} rows · ${ev.total_duration_ms} ms`;
    default:                    return "";
  }
}

/* ─ small renderers used inside the expanded card ─ */

function PipelineCodeBlock({ children, lang }: { children: string; lang?: string }) {
  return (
    <pre className="bg-[#0d1117] border border-border rounded p-2 overflow-x-auto text-[11px] leading-relaxed text-zinc-200 font-mono whitespace-pre-wrap break-all">
      {lang && <div className="text-[9px] uppercase text-muted-foreground mb-1">{lang}</div>}
      <code>{children}</code>
    </pre>
  );
}

function PipelineRowsTable({ rows }: { rows: Record<string, unknown>[] }) {
  if (!rows || rows.length === 0) return <div className="text-[11px] text-muted-foreground">(no rows)</div>;
  const cols = Object.keys(rows[0]);
  return (
    <div className="overflow-x-auto border border-border rounded">
      <table className="w-full text-[10px] font-mono">
        <thead className="bg-surface-raised text-muted-foreground">
          <tr>{cols.map((c) => <th key={c} className="px-2 py-1 text-left font-medium">{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.slice(0, 20).map((r, i) => (
            <tr key={i} className="border-t border-border/50">
              {cols.map((c) => <td key={c} className="px-2 py-1 text-foreground">{String(r[c] ?? "")}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 20 && (
        <div className="px-2 py-1 text-[10px] text-muted-foreground bg-surface-raised">
          … {rows.length - 20} more rows
        </div>
      )}
    </div>
  );
}

function PipelineEventDetail({ ev }: { ev: LogEntry }) {
  const e = ev.event as string;

  if (e === "query_received") {
    return (
      <div className="space-y-2">
        <div className="text-[11px] text-muted-foreground">User query:</div>
        <PipelineCodeBlock>{String(ev.query ?? "")}</PipelineCodeBlock>
        {ev.has_conversation_context ? (
          <>
            <div className="text-[11px] text-muted-foreground">Conversation context (preview):</div>
            <PipelineCodeBlock>{String(ev.conversation_context_preview ?? "")}</PipelineCodeBlock>
          </>
        ) : (
          <div className="text-[11px] text-muted-foreground">No conversation context</div>
        )}
      </div>
    );
  }

  if (e === "catalog_loaded") {
    const files = (ev.files as string[]) || [];
    return (
      <div className="space-y-2">
        <div className="grid grid-cols-2 gap-2 text-[11px]">
          <div><span className="text-muted-foreground">Container:</span> <span className="text-foreground font-mono">{String(ev.container)}</span></div>
          <div><span className="text-muted-foreground">Files:</span> <span className="text-foreground font-mono">{String(ev.file_count)}</span></div>
          <div><span className="text-muted-foreground">Parquet:</span> <span className="text-foreground font-mono">{String(ev.parquet_count)}</span></div>
          <div><span className="text-muted-foreground">Relationships:</span> <span className="text-foreground font-mono">{String(ev.relationship_count)}</span></div>
        </div>
        <div className="text-[11px] text-muted-foreground">Files available to the agent:</div>
        <div className="border border-border rounded p-2 bg-[#0d1117] max-h-48 overflow-auto">
          {files.map((f, i) => (
            <div key={i} className="text-[11px] font-mono text-zinc-200">• {f}</div>
          ))}
        </div>
      </div>
    );
  }

  if (e === "system_prompt_built") {
    return (
      <div className="space-y-2">
        <div className="grid grid-cols-2 gap-2 text-[11px]">
          <div><span className="text-muted-foreground">Container:</span> <span className="text-foreground font-mono">{String(ev.container)}</span></div>
          <div><span className="text-muted-foreground">Files in catalog:</span> <span className="text-foreground font-mono">{String(ev.catalog_file_count)}</span></div>
          <div><span className="text-muted-foreground">Parquet-ready:</span> <span className="text-foreground font-mono">{String(ev.parquet_file_count)}</span></div>
          <div><span className="text-muted-foreground">Has relationships:</span> <span className="text-foreground font-mono">{ev.has_relationships ? "yes" : "no"}</span></div>
        </div>
        <div className="text-[11px] text-muted-foreground">Full prompt sent to LLM:</div>
        <div className="max-h-72 overflow-auto">
          <PipelineCodeBlock>{String(ev.system_prompt ?? "")}</PipelineCodeBlock>
        </div>
      </div>
    );
  }

  if (e === "search_catalog") {
    const matched = (ev.matched_files as string[]) || [];
    return (
      <div className="space-y-2">
        <div className="text-[11px]"><span className="text-muted-foreground">Query:</span> <span className="font-mono text-foreground">{String(ev.query)}</span></div>
        <div className="text-[11px] text-muted-foreground">Matched files (names only):</div>
        <div className="border border-border rounded p-2 bg-[#0d1117]">
          {matched.length === 0 ? <div className="text-[11px] text-muted-foreground">(no matches)</div> :
            matched.map((f, i) => <div key={i} className="text-[11px] font-mono text-zinc-200">• {f}</div>)}
        </div>
      </div>
    );
  }

  if (e === "get_file_schema") {
    const cols = (ev.columns as string[]) || [];
    const types = (ev.column_types as Record<string, string>) || {};
    const samples = (ev.sample_values as Record<string, unknown[]>) || {};
    return (
      <div className="space-y-2">
        <div className="text-[11px]">
          <span className="text-muted-foreground">File:</span>{" "}
          <span className="font-mono text-foreground">{String(ev.blob_path)}</span>{" "}
          <span className={cn("ml-2 px-1.5 py-0.5 rounded text-[10px] border",
            ev.found ? "bg-green-500/10 text-green-400 border-green-500/30"
                     : "bg-red-500/10 text-red-400 border-red-500/30")}>
            {ev.found ? "found" : "not found"}
          </span>
        </div>
        {cols.length > 0 && (
          <div className="border border-border rounded overflow-x-auto">
            <table className="w-full text-[10px] font-mono">
              <thead className="bg-surface-raised text-muted-foreground">
                <tr><th className="px-2 py-1 text-left">column</th><th className="px-2 py-1 text-left">type</th><th className="px-2 py-1 text-left">sample</th></tr>
              </thead>
              <tbody>
                {cols.map((c) => (
                  <tr key={c} className="border-t border-border/50">
                    <td className="px-2 py-1 text-foreground">{c}</td>
                    <td className="px-2 py-1 text-cyan-300">{types[c] ?? ""}</td>
                    <td className="px-2 py-1 text-muted-foreground">{(samples[c] || []).slice(0,3).map(String).join(", ")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    );
  }

  if (e === "llm_output" || e === "llm_stream_output") {
    const tcs = (ev.tool_calls as { name?: string; args?: Record<string, unknown> }[]) || [];
    return (
      <div className="space-y-2">
        <div className="grid grid-cols-3 gap-2 text-[11px]">
          <div><span className="text-muted-foreground">Iteration:</span> <span className="font-mono">{String(ev.iteration)}</span></div>
          <div><span className="text-muted-foreground">Tokens:</span> <span className="font-mono">{String(ev.prompt_tokens)} + {String(ev.completion_tokens)}</span></div>
          <div><span className="text-muted-foreground">Duration:</span> <span className="font-mono">{String(ev.duration_ms ?? "?")} ms</span></div>
        </div>
        {tcs.length > 0 ? (
          <>
            <div className="text-[11px] text-muted-foreground">LLM decided to call {tcs.length} tool(s):</div>
            {tcs.map((tc, i) => (
              <div key={i} className="border border-border rounded p-2 bg-[#0d1117]">
                <div className="text-[11px] text-fuchsia-300 font-mono mb-1">→ {tc.name}</div>
                <PipelineCodeBlock lang="args">{JSON.stringify(tc.args ?? {}, null, 2)}</PipelineCodeBlock>
              </div>
            ))}
          </>
        ) : (
          <>
            <div className="text-[11px] text-muted-foreground">LLM decided: generate final answer</div>
            <PipelineCodeBlock>{String(ev.content ?? "")}</PipelineCodeBlock>
          </>
        )}
      </div>
    );
  }

  if (e === "llm_input" || e === "llm_stream_input") {
    const msgs = (ev.messages as { type: string; content: string; tool_calls?: unknown[] }[]) || [];
    return (
      <div className="space-y-2">
        <div className="text-[11px] text-muted-foreground">{msgs.length} messages going into the LLM:</div>
        <div className="max-h-72 overflow-auto space-y-1">
          {msgs.map((m, i) => (
            <div key={i} className="border border-border rounded p-2 bg-[#0d1117]">
              <div className="text-[10px] text-cyan-300 font-mono mb-1">[{i + 1}] {m.type}</div>
              <pre className="text-[11px] font-mono text-zinc-200 whitespace-pre-wrap break-all">{m.content?.slice(0, 800)}{(m.content?.length ?? 0) > 800 ? " …" : ""}</pre>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (e === "tool_call_start") {
    return (
      <div className="space-y-2">
        <div className="text-[11px]"><span className="text-muted-foreground">Tool:</span> <span className="font-mono text-fuchsia-300">{String(ev.tool)}</span></div>
        <PipelineCodeBlock lang="input">{JSON.stringify(ev.input ?? {}, null, 2)}</PipelineCodeBlock>
      </div>
    );
  }

  if (e === "tool_call_end") {
    const out = String(ev.output ?? "");
    return (
      <div className="space-y-2">
        <div className="text-[11px]"><span className="text-muted-foreground">Tool:</span> <span className="font-mono text-fuchsia-300">{String(ev.tool)}</span></div>
        <PipelineCodeBlock lang="output">{out.length > 4000 ? out.slice(0, 4000) + "\n… (truncated)" : out}</PipelineCodeBlock>
      </div>
    );
  }

  if (e === "sql_execute_start") {
    return (
      <div className="space-y-2">
        <div className="text-[11px] text-muted-foreground">SQL being executed:</div>
        <PipelineCodeBlock lang="sql">{String(ev.sql ?? "")}</PipelineCodeBlock>
      </div>
    );
  }

  if (e === "sql_execute_done") {
    const rows = (ev.preview_rows as Record<string, unknown>[]) || [];
    return (
      <div className="space-y-2">
        <div className="grid grid-cols-3 gap-2 text-[11px]">
          <div><span className="text-muted-foreground">Returned:</span> <span className="font-mono text-emerald-300">{String(ev.rows_returned)}/{String(ev.total_rows)}</span></div>
          <div><span className="text-muted-foreground">Duration:</span> <span className="font-mono">{String(ev.duration_ms)} ms</span></div>
          <div><span className="text-muted-foreground">Columns:</span> <span className="font-mono">{Array.isArray(ev.columns) ? (ev.columns as unknown[]).length : 0}</span></div>
        </div>
        <PipelineRowsTable rows={rows} />
      </div>
    );
  }

  if (e === "sql_execute_error") {
    return (
      <div className="space-y-2">
        <div className="text-[11px] text-muted-foreground">SQL that failed:</div>
        <PipelineCodeBlock lang="sql">{String(ev.sql ?? "")}</PipelineCodeBlock>
        <div className="text-[11px] text-muted-foreground">Error:</div>
        <div className="border border-red-500/30 bg-red-500/5 rounded p-2 text-[11px] font-mono text-red-300 whitespace-pre-wrap break-all">
          {String(ev.error ?? "")}
        </div>
      </div>
    );
  }

  if (e === "final_answer") {
    return (
      <div className="space-y-2">
        <div className="grid grid-cols-3 gap-2 text-[11px]">
          <div><span className="text-muted-foreground">Tool calls:</span> <span className="font-mono">{String(ev.tool_calls)}</span></div>
          <div><span className="text-muted-foreground">Rows:</span> <span className="font-mono">{String(ev.row_count)}</span></div>
          <div><span className="text-muted-foreground">Total time:</span> <span className="font-mono text-emerald-300">{String(ev.total_duration_ms)} ms</span></div>
        </div>
        <div className="text-[11px] text-muted-foreground">Answer delivered to user:</div>
        <PipelineCodeBlock>{String(ev.answer ?? "")}</PipelineCodeBlock>
      </div>
    );
  }

  // Fallback: dump the JSON
  return <PipelineCodeBlock>{JSON.stringify(ev, null, 2)}</PipelineCodeBlock>;
}

function PipelineEventRow({ ev }: { ev: LogEntry }) {
  const [expanded, setExpanded] = useState(false);
  const eventName = (ev.event as string) || "";
  const cfg = PIPELINE_STEPS[eventName];
  const ts = formatTimestamp(ev.timestamp as string);
  const summary = pipelineSummary(ev);

  // Unknown / non-pipeline events get a muted row
  if (!cfg) {
    return (
      <div className="px-3 py-1.5 border-b border-border/40 text-[11px] font-mono text-muted-foreground">
        <span className="mr-2">{ts}</span>{eventName} <span className="opacity-60">{JSON.stringify(ev).slice(0, 120)}</span>
      </div>
    );
  }

  const isFinal = eventName === "final_answer";
  const isError = eventName === "sql_execute_error" || eventName === "catalog_empty";

  return (
    <div
      className={cn(
        "border-b border-border/50 transition-colors cursor-pointer hover:bg-surface-raised/40",
        expanded && "bg-surface-raised/30",
        isFinal && "bg-emerald-500/[0.04]",
        isError && "bg-red-500/[0.04]",
      )}
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-center gap-2 px-3 py-1.5">
        {/* Time */}
        <span className="text-[10px] text-muted-foreground font-mono w-16 shrink-0">{ts}</span>

        {/* Step badge */}
        <span className={cn(
          "inline-flex items-center justify-center w-6 h-5 rounded text-[10px] font-bold border shrink-0",
          cfg.color,
        )}>
          {cfg.num}
        </span>

        {/* Step label */}
        <span className={cn("px-1.5 py-0.5 rounded text-[10px] font-medium border shrink-0", cfg.color)}>
          {cfg.label}
        </span>

        {/* Summary */}
        <span className="text-xs text-foreground truncate font-mono">{summary}</span>

        {/* Expand */}
        <ChevronDown className={cn("w-3 h-3 text-muted-foreground transition-transform shrink-0 ml-auto", expanded && "rotate-180")} />
      </div>

      {expanded && (
        <div className="px-3 pb-3 pt-1 pl-[6.5rem]">
          <PipelineEventDetail ev={ev} />
        </div>
      )}
    </div>
  );
}

function PipelinePanel() {
  const [events, setEvents] = useState<LogEntry[]>([]);
  const [lines, setLines] = useState<number>(300);
  const [stepFilter, setStepFilter] = useState<string>("all");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [totalLines, setTotalLines] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchPipeline = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await apiFetch(`/api/logs/pipeline.log?lines=${lines}`);
      if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
      const data: LogResponse = await res.json();
      setEvents(data.lines || []);
      setTotalLines(data.total_lines || 0);
      requestAnimationFrame(() => {
        if (containerRef.current) containerRef.current.scrollTop = containerRef.current.scrollHeight;
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Failed to fetch pipeline log");
    } finally {
      setLoading(false);
    }
  }, [lines]);

  useEffect(() => { fetchPipeline(); }, [fetchPipeline]);

  useEffect(() => {
    if (autoRefresh) {
      intervalRef.current = setInterval(fetchPipeline, 3000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [autoRefresh, fetchPipeline]);

  const filtered = stepFilter === "all"
    ? events
    : events.filter((ev) => {
        const cfg = PIPELINE_STEPS[(ev.event as string) || ""];
        return cfg?.num === stepFilter;
      });

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="border-b border-border px-4 py-2 flex flex-wrap items-center gap-2 shrink-0">
        {/* Step filter chips */}
        <div className="flex items-center gap-1">
          {[
            { v: "all", label: "All" },
            { v: "1",   label: "1 · Query" },
            { v: "2",   label: "2 · Catalog" },
            { v: "3",   label: "3 · Prompt" },
            { v: "4",   label: "4 · LLM" },
            { v: "5",   label: "5 · SQL" },
            { v: "6",   label: "6 · Result" },
            { v: "✓",   label: "✓ Final" },
          ].map((s) => (
            <button
              key={s.v}
              onClick={() => setStepFilter(s.v)}
              className={cn(
                "px-2 py-1 rounded text-[11px] font-medium transition-colors",
                stepFilter === s.v
                  ? "bg-primary text-primary-foreground"
                  : "bg-surface-raised text-muted-foreground hover:text-foreground"
              )}
            >
              {s.label}
            </button>
          ))}
        </div>

        <div className="w-px h-5 bg-border mx-1 hidden sm:block" />

        <select
          value={lines}
          onChange={(e) => setLines(Number(e.target.value))}
          className="px-2 py-1 rounded bg-surface-raised border border-border text-xs text-foreground"
        >
          <option value={100}>100 events</option>
          <option value={300}>300 events</option>
          <option value={500}>500 events</option>
          <option value={1000}>1000 events</option>
        </select>

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

        <button
          onClick={fetchPipeline}
          disabled={loading}
          className="p-1.5 rounded bg-surface-raised text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
        >
          <RefreshCw className={cn("w-3.5 h-3.5", loading && "animate-spin")} />
        </button>

        <span className="text-[10px] text-muted-foreground ml-auto hidden sm:block">
          {filtered.length} / {totalLines} events
        </span>
      </div>

      {error && (
        <div className="mx-4 mt-2 px-3 py-2 rounded bg-red-500/10 border border-red-500/20 text-red-400 text-xs flex items-center gap-2">
          <AlertCircle className="w-3.5 h-3.5 shrink-0" />
          {error}
        </div>
      )}

      <div ref={containerRef} className="flex-1 overflow-y-auto bg-[#0d1117]">
        {filtered.length === 0 && !loading && (
          <div className="flex flex-col items-center justify-center h-full gap-2 text-muted-foreground">
            <Activity className="w-8 h-8" />
            <p className="text-sm">No pipeline events — send a chat query to see the trace.</p>
          </div>
        )}
        {filtered.map((ev, i) => (
          <PipelineEventRow key={i} ev={ev} />
        ))}
      </div>
    </div>
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
              <th className="px-3 py-2 font-medium text-center">Processing Time</th>
              <th className="px-3 py-2 font-medium text-center">Total Time</th>
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
                  {t.parquet_status && t.parquet_status !== t.ingest_status && (
                    <span className="ml-1"><StatusBadge status={t.parquet_status} /></span>
                  )}
                </td>
                <td className="px-3 py-2 text-center">
                  {t.processing_secs !== null ? (
                    <span className="flex items-center justify-center gap-1 text-foreground font-mono">
                      <Zap className="w-3 h-3 text-yellow-400" />
                      {formatSecs(t.processing_secs)}
                      {t.ingestion_secs !== null && t.parquet_secs !== null && (
                        <span className="text-[9px] text-muted-foreground ml-0.5">
                          ({formatSecs(t.ingestion_secs)} + {formatSecs(t.parquet_secs)})
                        </span>
                      )}
                    </span>
                  ) : t.parquet_error ? (
                    <span className="text-red-400 text-[10px]" title={t.parquet_error}>Error</span>
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
              </tr>
            ))}
            {timings.length === 0 && !loading && (
              <tr>
                <td colSpan={7} className="px-4 py-8 text-center text-muted-foreground">
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
  const [pageView, setPageView] = useState<PageView>("pipeline");
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
            onClick={() => setPageView("pipeline")}
            className={cn(
              "px-3 py-1.5 rounded text-xs font-medium transition-colors",
              pageView === "pipeline"
                ? "bg-primary text-primary-foreground"
                : "bg-surface-raised text-muted-foreground hover:text-foreground"
            )}
          >
            <Activity className="w-3.5 h-3.5 inline mr-1" />
            AI Pipeline
          </button>
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

      {pageView === "pipeline" ? (
        <PipelinePanel />
      ) : pageView === "performance" ? (
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
