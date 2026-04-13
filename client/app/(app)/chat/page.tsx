"use client";

import { useState, useRef, useEffect } from "react";
import {
  Send,
  AlertCircle,
  RefreshCw,
  ChevronDown,
  BarChart2,
  Table2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { apiFetch } from "@/lib/auth";

// ── Types ──────────────────────────────────────────────────────────────────────

interface ChartMeta {
  type: string;
  title?: string;
  x_column?: string;
  y_column?: string;
}

interface AssistantPayload {
  answer: string;
  data: Record<string, unknown>[];
  chart: ChartMeta | null;
  row_count?: number;
  suggested_rephrase?: string | null;
  tool_calls?: number;
  files_used?: string[];
}

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  payload?: AssistantPayload;
  error?: boolean;
}

// ── Analytics helpers ─────────────────────────────────────────────────────────

interface NumericStat {
  kind: "numeric";
  min: number;
  max: number;
  mean: number;
  sum: number;
  nulls: number;
}

interface CategoricalStat {
  kind: "categorical";
  topValues: { value: string; count: number; pct: number }[];
  unique: number;
  nulls: number;
}

type ColStat = NumericStat | CategoricalStat;

function computeAnalytics(data: Record<string, unknown>[]): Record<string, ColStat> {
  if (!data.length) return {};
  const cols = Object.keys(data[0]);
  const result: Record<string, ColStat> = {};

  for (const col of cols) {
    const vals = data.map((r) => r[col]);
    const nulls = vals.filter((v) => v === null || v === undefined || v === "").length;
    const nonNull = vals.filter((v) => v !== null && v !== undefined && v !== "");
    const numericVals = nonNull
      .map((v) => parseFloat(String(v)))
      .filter((n) => !isNaN(n));

    if (numericVals.length > nonNull.length * 0.7 && numericVals.length > 0) {
      const sum = numericVals.reduce((a, b) => a + b, 0);
      result[col] = {
        kind: "numeric",
        min: Math.min(...numericVals),
        max: Math.max(...numericVals),
        mean: sum / numericVals.length,
        sum,
        nulls,
      };
    } else {
      const freq: Record<string, number> = {};
      for (const v of nonNull) {
        const k = String(v);
        freq[k] = (freq[k] || 0) + 1;
      }
      const sorted = Object.entries(freq)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 8);
      result[col] = {
        kind: "categorical",
        topValues: sorted.map(([value, count]) => ({
          value,
          count,
          pct: Math.round((count / data.length) * 100),
        })),
        unique: Object.keys(freq).length,
        nulls,
      };
    }
  }
  return result;
}

function fmtNum(n: number): string {
  if (Math.abs(n) >= 1_000_000) return (n / 1_000_000).toFixed(2) + "M";
  if (Math.abs(n) >= 1_000) return (n / 1_000).toFixed(1) + "K";
  return n % 1 === 0 ? String(n) : n.toFixed(2);
}

// ── Analytics grid ────────────────────────────────────────────────────────────

function AnalyticsGrid({ data }: { data: Record<string, unknown>[] }) {
  const stats = computeAnalytics(data);
  const entries = Object.entries(stats);
  if (!entries.length) return null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-3">
      {entries.map(([col, stat]) => (
        <div key={col} className="bg-surface border border-border rounded-lg p-3">
          <p className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide mb-2 truncate">
            {col}
          </p>
          {stat.kind === "numeric" ? (
            <div className="space-y-1.5">
              <div className="flex justify-between items-baseline">
                <span className="text-xs text-muted-foreground">Sum</span>
                <span className="text-sm font-semibold text-foreground">{fmtNum(stat.sum)}</span>
              </div>
              <div className="flex justify-between items-baseline">
                <span className="text-xs text-muted-foreground">Avg</span>
                <span className="text-xs text-foreground">{fmtNum(stat.mean)}</span>
              </div>
              <div className="flex justify-between items-baseline">
                <span className="text-xs text-muted-foreground">Min / Max</span>
                <span className="text-xs text-foreground">
                  {fmtNum(stat.min)} / {fmtNum(stat.max)}
                </span>
              </div>
              {stat.nulls > 0 && (
                <p className="text-[11px] text-amber-500/80 mt-1">{stat.nulls} nulls</p>
              )}
            </div>
          ) : (
            <div className="space-y-2">
              <p className="text-[11px] text-muted-foreground">{stat.unique} unique values</p>
              {stat.topValues.map(({ value, count, pct }) => (
                <div key={value} className="space-y-0.5">
                  <div className="flex justify-between text-[11px]">
                    <span className="text-foreground truncate max-w-[120px]" title={value}>
                      {value}
                    </span>
                    <span className="text-muted-foreground shrink-0 ml-2">
                      {count} ({pct}%)
                    </span>
                  </div>
                  <div className="w-full bg-border rounded-full h-1 overflow-hidden">
                    <div
                      className="bg-primary h-full rounded-full transition-all"
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

// ── Data table ────────────────────────────────────────────────────────────────

function DataTable({ data }: { data: Record<string, unknown>[] }) {
  if (!data.length)
    return (
      <p className="text-xs text-muted-foreground py-6 text-center">No rows returned.</p>
    );

  const cols = Object.keys(data[0]);
  const rows = data.slice(0, 100);

  return (
    <div className="overflow-x-auto rounded-md border border-border text-xs">
      <table className="min-w-full divide-y divide-border">
        <thead className="bg-surface-raised">
          <tr>
            {cols.map((c) => (
              <th
                key={c}
                className="px-3 py-2 text-left font-medium text-muted-foreground whitespace-nowrap"
              >
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border bg-surface">
          {rows.map((row, i) => (
            <tr key={i} className="hover:bg-surface-raised/60 transition-colors">
              {cols.map((c) => (
                <td
                  key={c}
                  className="px-3 py-1.5 whitespace-nowrap text-foreground max-w-[180px] truncate"
                  title={String(row[c] ?? "")}
                >
                  {String(row[c] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 100 && (
        <p className="px-3 py-1.5 text-xs text-muted-foreground border-t border-border bg-surface-raised">
          Showing 100 of {data.length} rows
        </p>
      )}
    </div>
  );
}

// ── Results accordion ─────────────────────────────────────────────────────────

type TabId = "table" | "analytics";

function ResultsAccordion({
  payload,
  isOpen,
  onToggle,
}: {
  payload: AssistantPayload;
  isOpen: boolean;
  onToggle: () => void;
}) {
  const [tab, setTab] = useState<TabId>("table");
  const hasData = payload.data && payload.data.length > 0;
  if (!hasData) return null;

  const rowCount = payload.row_count ?? payload.data.length;

  return (
    <div className="mt-3 border border-border rounded-lg overflow-hidden">
      {/* Accordion header */}
      <button
        onClick={onToggle}
        className="w-full flex items-center justify-between px-3 py-2.5 bg-surface-raised hover:bg-surface-raised/70 transition-colors text-left select-none"
      >
        <div className="flex items-center gap-2">
          <Table2 className="w-3.5 h-3.5 text-muted-foreground" />
          <span className="text-xs font-medium text-foreground">Results</span>
          <span className="bg-primary/10 text-primary text-[11px] font-mono rounded px-1.5 py-0.5">
            {rowCount} row{rowCount !== 1 ? "s" : ""}
          </span>
          {payload.files_used && payload.files_used.length > 0 && (
            <span className="text-[11px] text-muted-foreground hidden sm:inline">
              · {payload.files_used.length} file{payload.files_used.length !== 1 ? "s" : ""}
            </span>
          )}
        </div>
        <ChevronDown
          className={cn(
            "w-4 h-4 text-muted-foreground transition-transform duration-200",
            isOpen && "rotate-180"
          )}
        />
      </button>

      {/* Accordion body */}
      {isOpen && (
        <div>
          {/* Tabs */}
          <div className="flex border-b border-border bg-surface">
            <button
              onClick={() => setTab("table")}
              className={cn(
                "flex items-center gap-1.5 px-4 py-2 text-xs font-medium border-b-2 -mb-px transition-colors",
                tab === "table"
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              <Table2 className="w-3.5 h-3.5" />
              Table
            </button>
            <button
              onClick={() => setTab("analytics")}
              className={cn(
                "flex items-center gap-1.5 px-4 py-2 text-xs font-medium border-b-2 -mb-px transition-colors",
                tab === "analytics"
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              )}
            >
              <BarChart2 className="w-3.5 h-3.5" />
              Analytics
            </button>
          </div>

          {/* Tab content */}
          <div className="p-3">
            {tab === "table" ? (
              <DataTable data={payload.data} />
            ) : (
              <AnalyticsGrid data={payload.data} />
            )}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Answer text ───────────────────────────────────────────────────────────────

function AnswerText({ text }: { text: string }) {
  const lines = text.split("\n").filter((l, i, a) => !(l === "" && a[i - 1] === ""));
  return (
    <div className="space-y-1 text-sm leading-relaxed">
      {lines.map((line, i) => {
        if (!line.trim()) return <div key={i} className="h-1" />;
        const parts = line.split(/\*\*(.+?)\*\*/g);
        return (
          <p
            key={i}
            className={cn(
              line.trimStart().startsWith("- ") || line.trimStart().startsWith("• ")
                ? "pl-3 before:content-['•'] before:-ml-3 before:mr-1 before:text-muted-foreground"
                : ""
            )}
          >
            {parts.map((part, j) =>
              j % 2 === 1 ? (
                <strong key={j} className="font-semibold text-foreground">
                  {part}
                </strong>
              ) : (
                <span key={j}>{part}</span>
              )
            )}
          </p>
        );
      })}
    </div>
  );
}

// ── Assistant message bubble ───────────────────────────────────────────────────

function AssistantMessage({
  msg,
  isExpanded,
  onToggle,
}: {
  msg: Message;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="bg-surface border border-border rounded-xl px-4 py-3 max-w-[85%]">
      {msg.error ? (
        <span className="flex items-center gap-2 text-destructive text-sm">
          <AlertCircle className="w-4 h-4 shrink-0" />
          {msg.content}
        </span>
      ) : (
        <>
          <AnswerText text={msg.content} />
          {msg.payload?.suggested_rephrase && (
            <p className="mt-2 text-xs text-muted-foreground italic border-t border-border pt-2">
              Try: &ldquo;{msg.payload.suggested_rephrase}&rdquo;
            </p>
          )}
          {msg.payload && msg.payload.data && msg.payload.data.length > 0 && (
            <ResultsAccordion
              payload={msg.payload}
              isOpen={isExpanded}
              onToggle={onToggle}
            />
          )}
        </>
      )}
    </div>
  );
}

// ── Page ───────────────────────────────────────────────────────────────────────

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [expandedMsgId, setExpandedMsgId] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: "smooth" });
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed || isLoading) return;

    const userMsg: Message = { id: crypto.randomUUID(), role: "user", content: trimmed };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setIsLoading(true);
    setExpandedMsgId(null); // collapse all previous on new question

    try {
      const res = await apiFetch("/api/chat/message", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: trimmed }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: "Unknown error" }));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }

      const data: AssistantPayload = await res.json();
      const newMsgId = crypto.randomUUID();

      setMessages((prev) => [
        ...prev,
        { id: newMsgId, role: "assistant", content: data.answer, payload: data },
      ]);

      // Auto-open accordion when data is present
      if (data.data && data.data.length > 0) {
        setExpandedMsgId(newMsgId);
      }
    } catch (err: unknown) {
      const errMsg = err instanceof Error ? err.message : "Something went wrong.";
      setMessages((prev) => [
        ...prev,
        { id: crypto.randomUUID(), role: "assistant", content: errMsg, error: true },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Messages */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full px-4 text-center">
            <div className="w-12 h-12 rounded-xl bg-primary/10 flex items-center justify-center mb-4">
              <Send className="w-5 h-5 text-primary" />
            </div>
            <h2 className="text-lg font-semibold text-foreground mb-1">Start a conversation</h2>
            <p className="text-sm text-muted-foreground max-w-sm">
              Ask anything about your data. The AI will search, query, and analyse it for you.
            </p>
          </div>
        ) : (
          <div className="max-w-3xl mx-auto px-4 py-6 space-y-6">
            {messages.map((msg) => (
              <div
                key={msg.id}
                className={cn(
                  "flex gap-3",
                  msg.role === "user" ? "justify-end" : "justify-start"
                )}
              >
                {msg.role === "user" ? (
                  <div className="max-w-[80%] rounded-xl px-4 py-3 text-sm leading-relaxed bg-primary text-primary-foreground">
                    {msg.content}
                  </div>
                ) : (
                  <AssistantMessage
                    msg={msg}
                    isExpanded={expandedMsgId === msg.id}
                    onToggle={() =>
                      setExpandedMsgId((prev) => (prev === msg.id ? null : msg.id))
                    }
                  />
                )}
              </div>
            ))}
            {isLoading && (
              <div className="flex justify-start">
                <div className="bg-surface border border-border rounded-xl px-4 py-3">
                  <div className="flex gap-1.5 items-center">
                    <span className="w-2 h-2 rounded-full bg-muted-foreground animate-pulse" />
                    <span className="w-2 h-2 rounded-full bg-muted-foreground animate-pulse [animation-delay:150ms]" />
                    <span className="w-2 h-2 rounded-full bg-muted-foreground animate-pulse [animation-delay:300ms]" />
                  </div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Input */}
      <div className="border-t border-border bg-surface p-4">
        <form onSubmit={handleSubmit} className="max-w-3xl mx-auto flex items-end gap-2">
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask a question about your data..."
            rows={1}
            className="flex-1 resize-none bg-surface-raised border border-border rounded-lg px-4 py-2.5 text-sm text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50 focus:border-primary"
          />
          <button
            type="submit"
            disabled={!input.trim() || isLoading}
            className="shrink-0 h-10 w-10 flex items-center justify-center rounded-lg bg-primary text-primary-foreground disabled:opacity-40 transition-opacity hover:opacity-90"
          >
            {isLoading ? (
              <RefreshCw className="w-4 h-4 animate-spin" />
            ) : (
              <Send className="w-4 h-4" />
            )}
          </button>
        </form>
      </div>
    </div>
  );
}
