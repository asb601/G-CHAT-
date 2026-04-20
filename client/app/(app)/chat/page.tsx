"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import {
  Send,
  AlertCircle,
  RefreshCw,
  ChevronDown,
  BarChart2,
  Table2,
  Plus,
  MessageSquare,
  Trash2,
  Pencil,
  Check,
  X,
  Clock,
  PanelLeftClose,
  PanelLeft,
  Search,
} from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";
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

interface ConversationSummary {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  message_count: number;
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

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  const s = String(value);
  // ISO datetime → readable format
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(s)) {
    try {
      const d = new Date(s);
      return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
    } catch {
      return s;
    }
  }
  // Large numbers → comma-separated
  const num = Number(s);
  if (!isNaN(num) && s.trim() !== "" && Math.abs(num) >= 1000) {
    return num % 1 === 0
      ? num.toLocaleString("en-US")
      : num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }
  return s;
}

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
            <th className="px-2 py-2 text-center font-medium text-muted-foreground w-10">#</th>
            {cols.map((c) => (
              <th
                key={c}
                className="px-3 py-2 text-left font-medium text-muted-foreground whitespace-nowrap"
              >
                {c.replace(/_/g, " ")}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-border bg-surface">
          {rows.map((row, i) => (
            <tr
              key={i}
              className={cn(
                "hover:bg-surface-raised/60 transition-colors",
                i % 2 === 1 && "bg-surface-raised/30"
              )}
            >
              <td className="px-2 py-1.5 text-center text-muted-foreground tabular-nums">{i + 1}</td>
              {cols.map((c) => {
                const formatted = formatCell(row[c]);
                const isNum = !isNaN(Number(row[c])) && String(row[c]).trim() !== "";
                return (
                  <td
                    key={c}
                    className={cn(
                      "px-3 py-1.5 text-foreground max-w-[280px] truncate",
                      isNum ? "tabular-nums text-right" : "text-left"
                    )}
                    title={String(row[c] ?? "")}
                  >
                    {formatted}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 100 && (
        <p className="px-3 py-2 text-xs text-muted-foreground border-t border-border bg-surface-raised text-center">
          Showing 100 of {data.length.toLocaleString()} rows
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

// ── Answer text (full markdown) ───────────────────────────────────────────────

function AnswerText({ text }: { text: string }) {
  return (
    <div className="prose prose-sm dark:prose-invert max-w-none prose-p:my-1 prose-li:my-0.5 prose-headings:mt-3 prose-headings:mb-1 prose-hr:my-2 prose-pre:bg-surface-raised prose-pre:border prose-pre:border-border prose-code:text-primary prose-code:before:content-none prose-code:after:content-none prose-table:text-xs prose-th:px-2 prose-th:py-1 prose-td:px-2 prose-td:py-1">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{text}</ReactMarkdown>
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

// ── Relative time ─────────────────────────────────────────────────────────────

function relativeTime(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 7) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}

// ── Conversation sidebar ──────────────────────────────────────────────────────

function ConversationSidebar({
  conversations,
  activeId,
  onSelect,
  onNew,
  onDelete,
  onRename,
  isOpen,
  onToggle,
  searchQuery,
  onSearchChange,
}: {
  conversations: ConversationSummary[];
  activeId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => void;
  isOpen: boolean;
  onToggle: () => void;
  searchQuery: string;
  onSearchChange: (q: string) => void;
}) {
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editTitle, setEditTitle] = useState("");
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null);

  const startRename = (conv: ConversationSummary) => {
    setEditingId(conv.id);
    setEditTitle(conv.title);
  };

  const commitRename = () => {
    if (editingId && editTitle.trim()) {
      onRename(editingId, editTitle.trim());
    }
    setEditingId(null);
  };

  if (!isOpen) return null;

  return (
    <div className="w-[260px] shrink-0 border-r border-border bg-surface flex flex-col h-full">
      {/* Header */}
      <div className="px-3 py-3 border-b border-border flex items-center justify-between">
        <h2 className="text-sm font-semibold text-foreground">Conversations</h2>
        <div className="flex items-center gap-1">
          <button
            onClick={onNew}
            className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-surface-raised transition-colors"
            title="New chat"
          >
            <Plus className="w-4 h-4" />
          </button>
          <button
            onClick={onToggle}
            className="p-1.5 rounded-md text-muted-foreground hover:text-foreground hover:bg-surface-raised transition-colors"
            title="Close sidebar"
          >
            <PanelLeftClose className="w-4 h-4" />
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="px-3 py-2 border-b border-border">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search conversations..."
            className="w-full pl-8 pr-3 py-1.5 text-xs bg-surface-raised border border-border rounded-md text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-1 focus:ring-primary"
          />
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto py-2">
        {conversations.length === 0 ? (
          <div className="px-4 py-8 text-center">
            <MessageSquare className="w-8 h-8 text-muted-foreground/40 mx-auto mb-2" />
            <p className="text-xs text-muted-foreground">
              {searchQuery ? "No matching conversations" : "No conversations yet"}
            </p>
          </div>
        ) : (
          conversations.map((conv) => (
            <div
              key={conv.id}
              className={cn(
                "group relative px-3 py-2.5 mx-2 rounded-lg cursor-pointer transition-colors",
                activeId === conv.id
                  ? "bg-primary/10 text-foreground"
                  : "text-muted-foreground hover:bg-surface-raised hover:text-foreground"
              )}
              onClick={() => {
                if (editingId !== conv.id) onSelect(conv.id);
              }}
            >
              {editingId === conv.id ? (
                <div className="flex items-center gap-1">
                  <input
                    autoFocus
                    value={editTitle}
                    onChange={(e) => setEditTitle(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") commitRename();
                      if (e.key === "Escape") setEditingId(null);
                    }}
                    className="flex-1 text-xs bg-surface border border-border rounded px-2 py-1 text-foreground focus:outline-none focus:ring-1 focus:ring-primary"
                    onClick={(e) => e.stopPropagation()}
                  />
                  <button
                    onClick={(e) => { e.stopPropagation(); commitRename(); }}
                    className="p-0.5 rounded text-green-500 hover:bg-green-500/10"
                  >
                    <Check className="w-3.5 h-3.5" />
                  </button>
                  <button
                    onClick={(e) => { e.stopPropagation(); setEditingId(null); }}
                    className="p-0.5 rounded text-muted-foreground hover:bg-surface-raised"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              ) : confirmDeleteId === conv.id ? (
                /* Delete confirmation inline */
                <div className="flex items-center gap-2" onClick={(e) => e.stopPropagation()}>
                  <span className="text-xs text-destructive truncate flex-1">Delete this chat?</span>
                  <button
                    onClick={() => { onDelete(conv.id); setConfirmDeleteId(null); }}
                    className="px-2 py-0.5 text-[11px] font-medium rounded bg-destructive text-destructive-foreground hover:opacity-90"
                  >
                    Yes
                  </button>
                  <button
                    onClick={() => setConfirmDeleteId(null)}
                    className="px-2 py-0.5 text-[11px] font-medium rounded border border-border text-muted-foreground hover:text-foreground"
                  >
                    No
                  </button>
                </div>
              ) : (
                <>
                  <p className="text-xs font-medium truncate pr-12">{conv.title}</p>
                  <div className="flex items-center gap-2 mt-0.5">
                    <p className="text-[11px] text-muted-foreground flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      {relativeTime(conv.updated_at)}
                    </p>
                    {conv.message_count > 0 && (
                      <p className="text-[11px] text-muted-foreground">
                        {conv.message_count} msg{conv.message_count !== 1 ? "s" : ""}
                      </p>
                    )}
                  </div>
                  {/* Action buttons on hover */}
                  <div className="absolute right-2 top-1/2 -translate-y-1/2 hidden group-hover:flex items-center gap-0.5">
                    <button
                      onClick={(e) => { e.stopPropagation(); startRename(conv); }}
                      className="p-1 rounded text-muted-foreground hover:text-foreground hover:bg-surface transition-colors"
                      title="Rename"
                    >
                      <Pencil className="w-3 h-3" />
                    </button>
                    <button
                      onClick={(e) => { e.stopPropagation(); setConfirmDeleteId(conv.id); }}
                      className="p-1 rounded text-muted-foreground hover:text-destructive hover:bg-destructive/10 transition-colors"
                      title="Delete"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                </>
              )}
            </div>
          ))
        )}
      </div>
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

  // Conversation state
  const [conversations, setConversations] = useState<ConversationSummary[]>([]);
  const [activeConvId, setActiveConvId] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [loadingConv, setLoadingConv] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  // ── Load conversation list on mount ──
  const fetchConversations = useCallback(async (search = "") => {
    try {
      const params = new URLSearchParams({ limit: "100" });
      if (search.trim()) params.set("search", search.trim());
      const res = await apiFetch(`/api/chat/conversations?${params}`);
      if (res.ok) {
        const data = await res.json();
        setConversations(data.conversations || []);
      }
    } catch {
      // silent — sidebar just won't load
    }
  }, []);

  useEffect(() => {
    fetchConversations();
  }, [fetchConversations]);

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => fetchConversations(searchQuery), 300);
    return () => clearTimeout(timer);
  }, [searchQuery, fetchConversations]);

  // ── Load a conversation's messages ──
  const loadConversation = useCallback(async (convId: string) => {
    setLoadingConv(true);
    setActiveConvId(convId);
    setMessages([]);
    setExpandedMsgId(null);

    try {
      const res = await apiFetch(`/api/chat/conversations/${convId}`);
      if (!res.ok) return;
      const data = await res.json();

      const loaded: Message[] = (data.messages || []).map(
        (m: { id: string; role: string; content: string; payload?: AssistantPayload }) => ({
          id: m.id,
          role: m.role as "user" | "assistant",
          content: m.content,
          payload: m.role === "assistant" ? m.payload : undefined,
        })
      );
      setMessages(loaded);
    } catch {
      // silent
    } finally {
      setLoadingConv(false);
    }
  }, []);

  // ── Start new conversation ──
  const startNewChat = useCallback(() => {
    setActiveConvId(null);
    setMessages([]);
    setExpandedMsgId(null);
    setInput("");
  }, []);

  // ── Delete conversation ──
  const deleteConversation = useCallback(async (convId: string) => {
    try {
      await apiFetch(`/api/chat/conversations/${convId}`, { method: "DELETE" });
      setConversations((prev) => prev.filter((c) => c.id !== convId));
      if (activeConvId === convId) {
        startNewChat();
      }
    } catch {
      // silent
    }
  }, [activeConvId, startNewChat]);

  // ── Rename conversation ──
  const renameConversation = useCallback(async (convId: string, title: string) => {
    try {
      await apiFetch(`/api/chat/conversations/${convId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title }),
      });
      setConversations((prev) =>
        prev.map((c) => (c.id === convId ? { ...c, title } : c))
      );
    } catch {
      // silent
    }
  }, []);

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
    setExpandedMsgId(null);

    try {
      const res = await apiFetch("/api/chat/message/stream", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: trimmed,
          conversation_id: activeConvId,
        }),
      });

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: "Unknown error" }));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }

      // Parse SSE stream
      const reader = res.body?.getReader();
      if (!reader) throw new Error("No response stream");

      const decoder = new TextDecoder();
      let streamedContent = "";
      let streamMsgId: string | null = null;
      let finalResult: (AssistantPayload & { conversation_id?: string; warning?: string }) | null = null;
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || ""; // keep incomplete line in buffer

        for (const line of lines) {
          if (!line.startsWith("data: ")) continue;
          try {
            const event = JSON.parse(line.slice(6));

            if (event.event === "started" && event.conversation_id) {
              if (!activeConvId || activeConvId !== event.conversation_id) {
                setActiveConvId(event.conversation_id);
              }
            } else if (event.event === "thinking") {
              // Show which tool is running (e.g. "Running run_sql...")
              const toolName = event.tool || "tools";
              if (!streamMsgId) {
                streamMsgId = crypto.randomUUID();
                setMessages((prev) => [
                  ...prev,
                  { id: streamMsgId!, role: "assistant", content: `Running ${toolName}...` },
                ]);
              } else {
                const currentId = streamMsgId;
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === currentId ? { ...m, content: `Running ${toolName}...` } : m
                  )
                );
              }
            } else if (event.event === "token") {
              streamedContent += event.content;
              if (!streamMsgId) {
                streamMsgId = crypto.randomUUID();
                setMessages((prev) => [
                  ...prev,
                  { id: streamMsgId!, role: "assistant", content: streamedContent },
                ]);
              } else {
                const currentContent = streamedContent;
                const currentId = streamMsgId;
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === currentId ? { ...m, content: currentContent } : m
                  )
                );
              }
            } else if (event.event === "done") {
              finalResult = event.result;
              if (streamMsgId && finalResult) {
                const fResult = finalResult;
                const sId = streamMsgId;
                setMessages((prev) =>
                  prev.map((m) =>
                    m.id === sId
                      ? { ...m, content: fResult.answer, payload: fResult }
                      : m
                  )
                );
                if (fResult.data && fResult.data.length > 0) {
                  setExpandedMsgId(sId);
                }
              }
              if (finalResult?.warning) {
                const warnMsg = finalResult.warning;
                setMessages((prev) => [
                  ...prev,
                  { id: crypto.randomUUID(), role: "assistant", content: warnMsg },
                ]);
              }
            } else if (event.event === "error") {
              throw new Error(event.detail || "Stream error");
            }
          } catch (parseErr) {
            if (parseErr instanceof Error && parseErr.message.includes("Stream error")) throw parseErr;
          }
        }
      }

      // Optimistic sidebar update — no full refetch
      const resultConvId = finalResult?.conversation_id || activeConvId;
      if (resultConvId) {
        setConversations((prev) => {
          const exists = prev.some((c) => c.id === resultConvId);
          if (exists) {
            return prev.map((c) =>
              c.id === resultConvId
                ? { ...c, message_count: (c.message_count || 0) + 2, updated_at: new Date().toISOString() }
                : c
            );
          }
          // New conversation — add it at the top, then do one background refresh
          // to get the server-generated title
          fetchConversations(searchQuery);
          return prev;
        });
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
    <div className="flex h-full">
      {/* Conversation sidebar */}
      <ConversationSidebar
        conversations={conversations}
        activeId={activeConvId}
        onSelect={loadConversation}
        onNew={startNewChat}
        onDelete={deleteConversation}
        onRename={renameConversation}
        isOpen={sidebarOpen}
        onToggle={() => setSidebarOpen(false)}
        searchQuery={searchQuery}
        onSearchChange={setSearchQuery}
      />

      {/* Main chat area */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Sidebar toggle when collapsed */}
        {!sidebarOpen && (
          <div className="px-3 py-2 border-b border-border">
            <button
              onClick={() => setSidebarOpen(true)}
              className="p-1.5 rounded-md bg-surface border border-border text-muted-foreground hover:text-foreground transition-colors"
              title="Open sidebar"
            >
              <PanelLeft className="w-4 h-4" />
            </button>
          </div>
        )}

        {/* Messages */}
        <div ref={scrollRef} className="flex-1 overflow-y-auto">
          {loadingConv ? (
            <div className="flex items-center justify-center h-full">
              <RefreshCw className="w-5 h-5 text-muted-foreground animate-spin" />
            </div>
          ) : messages.length === 0 ? (
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
    </div>
  );
}
