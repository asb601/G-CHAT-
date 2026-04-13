"use client";

import {
  useState,
  useCallback,
  useRef,
  useEffect,
  type DragEvent,
  type MouseEvent,
} from "react";
import {
  ChevronLeft,
  LayoutGrid,
  List,
  Upload,
  FolderOpen,
  FolderPlus,
  Folder,
  FileText,
  FileSpreadsheet,
  Table,
  File,
  Pencil,
  Star,
  Trash2,
  Zap,
  Check,
  X,
  Cloud,
  ExternalLink,
} from "lucide-react";
import { cn } from "@/lib/utils";

/* ━━━ Types ━━━ */
export type FileStatus = "indexed" | "failed" | "pending" | "not_ingested";
export type FileType = "csv" | "xlsx" | "txt" | "pdf" | "folder";

export interface FileItem {
  id: string;
  name: string;
  type: FileType;
  size: number;
  status: FileStatus;
  lastModified: Date;
  synced?: boolean;
  uploadedBy?: string;
}

export interface UploadProgressItem {
  fileName: string;
  percent: number;
  speedMBps: number;
  remainingMins: number;
  phase: "uploading" | "confirming" | "done" | "error";
}

interface ContainerOption {
  id: string;
  name: string;
}

interface FileManagerViewProps {
  files?: FileItem[];
  folderName?: string;
  loading?: boolean;
  readOnly?: boolean;
  uploadProgress?: UploadProgressItem[];
  containers?: ContainerOption[];
  selectedContainerId?: string;
  onContainerChange?: (id: string) => void;
  onUpload: (files: File[]) => void;
  onIngest: (id: string) => void;
  onDelete: (id: string) => void;
  onRename?: (id: string, newName: string) => void;
  onCreateFolder?: (name: string) => void;
  onFolderOpen: (id: string) => void;
  onOpenFile?: (id: string) => void;
  onFolderHover?: (id: string) => void;
  onBack?: () => void;
}

type ViewMode = "grid" | "list";



/* ━━━ Helpers ━━━ */
function formatSize(bytes: number): string {
  if (bytes === 0) return "—";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDate(d: Date): string {
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function getIcon(type: FileType) {
  switch (type) {
    case "csv": return FileSpreadsheet;
    case "xlsx": return Table;
    case "txt": return FileText;
    case "pdf": return FileText;
    case "folder": return Folder;
  }
}

function getIconColor(type: FileType): string {
  switch (type) {
    case "folder": return "text-foreground";
    default: return "text-muted-foreground";
  }
}

function getTypeBadge(type: FileType): string {
  return type === "folder" ? "Folder" : type.toUpperCase();
}

/* ━━━ StatusBadge ━━━ */
function StatusBadge({ status }: { status: FileStatus }) {
  const config = {
    indexed: { color: "text-emerald-400", bg: "bg-emerald-400", label: "indexed" },
    failed: { color: "text-red-400", bg: "bg-red-400", label: "failed" },
    pending: { color: "text-amber-400", bg: "bg-amber-400", label: "ingesting" },
    not_ingested: { color: "text-muted-foreground", bg: "bg-muted-foreground", label: "not ingested" },
  } as const;

  const c = config[status];

  return (
    <span className={cn("inline-flex items-center gap-1 text-[10px]", c.color)}>
      <span
        className={cn(
          "w-1.5 h-1.5 rounded-full shrink-0",
          c.bg,
          status === "pending" && "animate-pulse"
        )}
      />
      {c.label}
    </span>
  );
}

/* ━━━ ContextMenu ━━━ */
function ContextMenu({
  x,
  y,
  isFolder,
  readOnly,
  onOpen,
  onOpenFile,
  onIngest,
  onRename,
  onDelete,
  onClose,
}: {
  x: number;
  y: number;
  isFolder: boolean;
  readOnly?: boolean;
  onOpen: () => void;
  onOpenFile: () => void;
  onIngest: () => void;
  onRename: () => void;
  onDelete: () => void;
  onClose: () => void;
}) {
  useEffect(() => {
    const handleClick = () => onClose();
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("click", handleClick);
    window.addEventListener("keydown", handleKey);
    return () => {
      window.removeEventListener("click", handleClick);
      window.removeEventListener("keydown", handleKey);
    };
  }, [onClose]);

  // Clamp to viewport
  const style = {
    left: Math.min(x, window.innerWidth - 180),
    top: Math.min(y, window.innerHeight - 200),
  };

  return (
    <div
      className="fixed z-[200] min-w-[10rem] bg-surface border border-border rounded-lg shadow-md py-1"
      style={style}
    >
      {isFolder && (
        <>
          <button
            onClick={(e) => { e.stopPropagation(); onOpen(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <FolderOpen className="w-3.5 h-3.5 text-muted-foreground" />
            Open
          </button>
          <div className="h-px bg-border mx-2 my-0.5" />
        </>
      )}
      {!isFolder && (
        <>
          <button
            onClick={(e) => { e.stopPropagation(); onOpenFile(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <ExternalLink className="w-3.5 h-3.5 text-muted-foreground" />
            Open
          </button>
          <div className="h-px bg-border mx-2 my-0.5" />
        </>
      )}
      {!isFolder && !readOnly && (
        <>
          <button
            onClick={(e) => { e.stopPropagation(); onIngest(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <Zap className="w-3.5 h-3.5 text-muted-foreground" />
            Ingest
          </button>
          <div className="h-px bg-border mx-2 my-0.5" />
        </>
      )}
      {!readOnly && (
        <>
          <button
            onClick={(e) => { e.stopPropagation(); onRename(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <Pencil className="w-3.5 h-3.5 text-muted-foreground" />
            Rename
          </button>
          <button
            onClick={(e) => { e.stopPropagation(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <Star className="w-3.5 h-3.5 text-muted-foreground" />
            Star
          </button>
          <div className="h-px bg-border mx-2 my-0.5" />
          <button
            onClick={(e) => { e.stopPropagation(); onDelete(); onClose(); }}
            className="w-full h-8 px-3 text-sm flex items-center gap-2 text-foreground hover:bg-surface-raised transition-colors"
          >
            <Trash2 className="w-3.5 h-3.5" />
            Delete
          </button>
        </>
      )}
    </div>
  );
}

/* ━━━ GridCard ━━━ */
function GridCard({
  item,
  selected,
  onClick,
  onDoubleClick,
  onContextMenu,
  onMouseEnter,
}: {
  item: FileItem;
  selected: boolean;
  onClick: () => void;
  onDoubleClick: () => void;
  onContextMenu: (e: MouseEvent) => void;
  onMouseEnter?: () => void;
}) {
  const Icon = item.type === "folder" && selected ? FolderOpen : getIcon(item.type);
  const iconColor = getIconColor(item.type);

  return (
    <div
      onClick={onClick}
      onDoubleClick={onDoubleClick}
      onContextMenu={onContextMenu}
      onMouseEnter={onMouseEnter}
      className={cn(
        "flex flex-col items-center justify-center rounded-xl p-3 cursor-pointer select-none transition-all border",
        selected
          ? "border-primary/50 bg-primary/5"
          : "border-transparent hover:border-border hover:bg-surface-raised"
      )}
    >
      <div className="flex items-center justify-center h-[52px] relative">
        <Icon className={cn("w-9 h-9", iconColor)} strokeWidth={1.5} />
        {item.synced && item.type !== "folder" && (
          <Cloud className="w-3 h-3 text-muted-foreground absolute top-0 right-0" strokeWidth={1.5} />
        )}
      </div>
      <p className="text-[11px] text-foreground text-center truncate w-full mt-1 leading-tight">
        {item.name}
      </p>
      {item.type !== "folder" && (
        <div className="mt-0.5">
          <StatusBadge status={item.status} />
        </div>
      )}
    </div>
  );
}

/* ━━━ ListRow ━━━ */
function ListRow({
  item,
  selected,
  onClick,
  onDoubleClick,
  onContextMenu,
  onMouseEnter,
}: {
  item: FileItem;
  selected: boolean;
  onClick: () => void;
  onDoubleClick: () => void;
  onContextMenu: (e: MouseEvent) => void;
  onMouseEnter?: () => void;
}) {
  const Icon = getIcon(item.type);
  const iconColor = getIconColor(item.type);

  return (
    <div
      onClick={onClick}
      onDoubleClick={onDoubleClick}
      onContextMenu={onContextMenu}
      onMouseEnter={onMouseEnter}
      className={cn(
        "h-10 flex items-center px-3 gap-4 border-b border-border text-sm cursor-pointer select-none transition-colors",
        selected ? "bg-primary/5" : "hover:bg-surface-raised"
      )}
    >
      <Icon className={cn("w-4 h-4 shrink-0", iconColor)} strokeWidth={1.5} />
      {item.synced && item.type !== "folder" && (
        <Cloud className="w-3 h-3 shrink-0 text-muted-foreground" strokeWidth={1.5} />
      )}
      <span className="flex-1 truncate text-foreground">{item.name}</span>
      <span className="text-[10px] px-1.5 py-0.5 bg-surface-raised rounded text-muted-foreground shrink-0">
        {getTypeBadge(item.type)}
      </span>
      <span className="text-xs text-muted-foreground w-16 text-right shrink-0">
        {item.type === "folder" ? "—" : formatSize(item.size)}
      </span>
      <span className="w-16 shrink-0 text-center">
        {item.type !== "folder" ? <StatusBadge status={item.status} /> : null}
      </span>
      <span className="text-xs text-muted-foreground w-24 text-right shrink-0">
        {formatDate(item.lastModified)}
      </span>
    </div>
  );
}

/* ━━━ Main Component ━━━ */
export default function FileManagerView({
  files,
  folderName,
  loading,
  readOnly,
  uploadProgress,
  containers,
  selectedContainerId,
  onContainerChange,
  onUpload,
  onIngest,
  onDelete,
  onRename,
  onCreateFolder,
  onFolderOpen,
  onOpenFile,
  onFolderHover,
  onBack,
}: FileManagerViewProps) {
  const items = files ?? [];
  const [view, setView] = useState<ViewMode>("grid");
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [draggingOver, setDraggingOver] = useState(false);
  const [ctxMenu, setCtxMenu] = useState<{ x: number; y: number; id: string } | null>(null);
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState("Untitled Folder");
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);
  const newFolderInputRef = useRef<HTMLInputElement>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  const dragCounter = useRef(0);

  /* ── Auto-focus new folder input ── */
  useEffect(() => {
    if (creatingFolder && newFolderInputRef.current) {
      newFolderInputRef.current.focus();
      newFolderInputRef.current.select();
    }
  }, [creatingFolder]);

  /* ── Auto-focus rename input ── */
  useEffect(() => {
    if (renamingId && renameInputRef.current) {
      renameInputRef.current.focus();
      renameInputRef.current.select();
    }
  }, [renamingId]);

  const submitNewFolder = useCallback(() => {
    const name = newFolderName.trim();
    if (name && onCreateFolder) {
      onCreateFolder(name);
    }
    setCreatingFolder(false);
    setNewFolderName("Untitled Folder");
  }, [newFolderName, onCreateFolder]);

  const submitRename = useCallback(() => {
    const name = renameValue.trim();
    if (name && renamingId && onRename) {
      onRename(renamingId, name);
    }
    setRenamingId(null);
    setRenameValue("");
  }, [renameValue, renamingId, onRename]);

  const startRename = useCallback((id: string) => {
    const item = items.find((i) => i.id === id);
    if (item) {
      setRenamingId(id);
      setRenameValue(item.name);
    }
  }, [items]);

  /* ── Click outside to deselect ── */
  const handleBgClick = useCallback(() => {
    setSelectedId(null);
  }, []);

  /* ── File input change ── */
  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const fileList = e.target.files;
      if (fileList?.length) {
        onUpload(Array.from(fileList) as unknown as File[]);
      }
      e.target.value = "";
    },
    [onUpload]
  );

  /* ── Drag & drop zone ── */
  const handleDragEnter = useCallback((e: DragEvent) => {
    e.preventDefault();
    dragCounter.current++;
    if (e.dataTransfer.types.includes("Files")) setDraggingOver(true);
  }, []);

  const handleDragLeave = useCallback((e: DragEvent) => {
    e.preventDefault();
    dragCounter.current--;
    if (dragCounter.current === 0) setDraggingOver(false);
  }, []);

  const handleDragOver = useCallback((e: DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "copy";
  }, []);

  const handleDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      dragCounter.current = 0;
      setDraggingOver(false);
      if (readOnly) return;
      const droppedFiles = Array.from(e.dataTransfer.files) as unknown as File[];
      if (droppedFiles.length) onUpload(droppedFiles);
    },
    [onUpload, readOnly]
  );

  /* ── Context menu ── */
  const openCtx = useCallback((e: MouseEvent, id: string) => {
    e.preventDefault();
    e.stopPropagation();
    setSelectedId(id);
    setCtxMenu({ x: e.clientX, y: e.clientY, id });
  }, []);

  /* ── Double click ── */
  const handleDoubleClick = useCallback(
    (item: FileItem) => {
      if (item.type === "folder") onFolderOpen(item.id);
      else onOpenFile?.(item.id);
    },
    [onFolderOpen, onOpenFile]
  );

  const ctxItem = ctxMenu ? items.find((i) => i.id === ctxMenu.id) : null;

  // Sort: folders first, then alphabetical
  const sorted = [...items].sort((a, b) => {
    if (a.type === "folder" && b.type !== "folder") return -1;
    if (a.type !== "folder" && b.type === "folder") return 1;
    return a.name.localeCompare(b.name);
  });

  return (
    <div
      className="flex flex-col h-full relative"
      onDragEnter={handleDragEnter}
      onDragLeave={handleDragLeave}
      onDragOver={handleDragOver}
      onDrop={handleDrop}
    >
      {/* ── Toolbar ── */}
      <div className="shrink-0 h-11 flex items-center justify-between px-4 bg-surface/80 backdrop-blur border-b border-border">
        <div className="flex items-center gap-2 min-w-0">
          {onBack && (
            <button
              onClick={onBack}
              className="w-7 h-7 flex items-center justify-center rounded-md text-muted-foreground hover:text-foreground hover:bg-surface-raised transition-colors"
            >
              <ChevronLeft className="w-4 h-4" />
            </button>
          )}
          <span className="text-sm font-medium text-foreground truncate">
            {folderName ?? "All Files"}
          </span>
        </div>

        <div className="flex items-center gap-1">
          <button
            onClick={() => setView("grid")}
            className={cn(
              "w-7 h-7 flex items-center justify-center rounded-md transition-colors",
              view === "grid"
                ? "bg-surface-raised text-foreground"
                : "text-muted-foreground hover:text-foreground"
            )}
          >
            <LayoutGrid className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => setView("list")}
            className={cn(
              "w-7 h-7 flex items-center justify-center rounded-md transition-colors",
              view === "list"
                ? "bg-surface-raised text-foreground"
                : "text-muted-foreground hover:text-foreground"
            )}
          >
            <List className="w-3.5 h-3.5" />
          </button>
          <div className="w-px h-4 bg-border mx-1" />
          {!readOnly && (
            <>
              {containers && containers.length > 0 && (
                <select
                  value={selectedContainerId ?? ""}
                  onChange={(e) => onContainerChange?.(e.target.value)}
                  className="h-7 px-2 rounded-md text-xs bg-surface border border-border text-foreground outline-none focus:border-primary"
                >
                  <option value="">Select container…</option>
                  {containers.map((c) => (
                    <option key={c.id} value={c.id}>{c.name}</option>
                  ))}
                </select>
              )}
              <button
                onClick={() => {
                  setCreatingFolder(true);
                  setNewFolderName("Untitled Folder");
                }}
                className="h-7 px-2.5 flex items-center gap-1.5 rounded-md text-sm text-muted-foreground hover:text-foreground hover:bg-surface-raised transition-colors"
              >
                <FolderPlus className="w-3.5 h-3.5" />
                <span className="text-xs">New Folder</span>
              </button>
              <button
                onClick={() => {
                  if (!selectedContainerId && containers && containers.length > 0) return;
                  fileInputRef.current?.click();
                }}
                disabled={!selectedContainerId && (containers?.length ?? 0) > 0}
                className={cn(
                  "h-7 px-2.5 flex items-center gap-1.5 rounded-md text-sm transition-colors",
                  !selectedContainerId && (containers?.length ?? 0) > 0
                    ? "text-muted-foreground/50 cursor-not-allowed"
                    : "text-muted-foreground hover:text-foreground hover:bg-surface-raised"
                )}
              >
                <Upload className="w-3.5 h-3.5" />
                <span className="text-xs">Upload</span>
              </button>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                onChange={handleFileInput}
              />
            </>
          )}
        </div>
      </div>

      {/* ── Content ── */}
      <div className="flex-1 overflow-y-auto" onClick={handleBgClick}>
        {loading ? (
          /* ── Skeleton loading ── */
          <div className="p-4 grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="flex flex-col items-center justify-center rounded-xl p-3">
                <div className="h-[52px] w-9 rounded bg-surface-raised animate-pulse" />
                <div className="mt-2 h-3 w-16 rounded bg-surface-raised animate-pulse" />
              </div>
            ))}
          </div>
        ) : sorted.length === 0 && !creatingFolder ? (
          /* ── Empty state ── */
          <div className="flex-1 flex flex-col items-center justify-center h-full min-h-[300px]">
            <FolderOpen className="w-12 h-12 text-muted-foreground" strokeWidth={1} />
            <p className="text-sm text-foreground mt-3">No files yet</p>
            <p className="text-xs text-muted-foreground mt-1">
              Drop files here, click Upload, or create a New Folder
            </p>
          </div>
        ) : view === "grid" ? (
          /* ── Grid view ── */
          <div className="p-4 grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 lg:grid-cols-8 gap-2">
            {creatingFolder && (
              <div className="flex flex-col items-center justify-center rounded-xl p-3 border border-primary/50 bg-primary/5">
                <div className="flex items-center justify-center h-[52px]">
                  <Folder className="w-9 h-9 text-foreground" strokeWidth={1.5} />
                </div>
                <input
                  ref={newFolderInputRef}
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") submitNewFolder();
                    if (e.key === "Escape") { setCreatingFolder(false); setNewFolderName("Untitled Folder"); }
                  }}
                  onBlur={submitNewFolder}
                  className="mt-1 w-full text-[11px] text-center bg-transparent border border-border rounded px-1 py-0.5 text-foreground outline-none focus:border-primary"
                />
              </div>
            )}
            {sorted.map((item) => (
              renamingId === item.id ? (
                <div key={item.id} className="flex flex-col items-center justify-center rounded-xl p-3 border border-primary/50 bg-primary/5">
                  <div className="flex items-center justify-center h-[52px]">
                    {(() => { const Icon = getIcon(item.type); return <Icon className={cn("w-9 h-9", getIconColor(item.type))} strokeWidth={1.5} />; })()}
                  </div>
                  <input
                    ref={renameInputRef}
                    value={renameValue}
                    onChange={(e) => setRenameValue(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") submitRename();
                      if (e.key === "Escape") { setRenamingId(null); setRenameValue(""); }
                    }}
                    onBlur={submitRename}
                    className="mt-1 w-full text-[11px] text-center bg-transparent border border-border rounded px-1 py-0.5 text-foreground outline-none focus:border-primary"
                  />
                </div>
              ) : (
                <GridCard
                  key={item.id}
                  item={item}
                  selected={selectedId === item.id}
                  onClick={() => item.type === "folder" ? onFolderOpen(item.id) : setSelectedId(item.id)}
                  onDoubleClick={() => handleDoubleClick(item)}
                  onContextMenu={(e) => openCtx(e, item.id)}
                  onMouseEnter={item.type === "folder" && onFolderHover ? () => onFolderHover(item.id) : undefined}
                />
              )
            ))}
          </div>
        ) : (
          /* ── List view ── */
          <div className="w-full">
            <div className="h-8 flex items-center px-3 gap-4 text-[11px] text-muted-foreground uppercase tracking-wide border-b border-border">
              <span className="w-4 shrink-0" />
              <span className="flex-1">Name</span>
              <span className="w-12 shrink-0 text-center">Type</span>
              <span className="w-16 text-right shrink-0">Size</span>
              <span className="w-16 text-center shrink-0">Status</span>
              <span className="w-24 text-right shrink-0">Modified</span>
            </div>
            {creatingFolder && (
              <div className="h-10 flex items-center px-3 gap-4 border-b border-border bg-primary/5">
                <Folder className="w-4 h-4 shrink-0 text-foreground" strokeWidth={1.5} />
                <input
                  ref={!renamingId ? newFolderInputRef : undefined}
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") submitNewFolder();
                    if (e.key === "Escape") { setCreatingFolder(false); setNewFolderName("Untitled Folder"); }
                  }}
                  onBlur={submitNewFolder}
                  className="flex-1 text-sm bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground outline-none focus:border-primary"
                />
              </div>
            )}
            {sorted.map((item) => (
              renamingId === item.id ? (
                <div key={item.id} className="h-10 flex items-center px-3 gap-4 border-b border-border bg-primary/5">
                  {(() => { const Icon = getIcon(item.type); return <Icon className={cn("w-4 h-4 shrink-0", getIconColor(item.type))} strokeWidth={1.5} />; })()}
                  <input
                    ref={renameInputRef}
                    value={renameValue}
                    onChange={(e) => setRenameValue(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") submitRename();
                      if (e.key === "Escape") { setRenamingId(null); setRenameValue(""); }
                    }}
                    onBlur={submitRename}
                    className="flex-1 text-sm bg-transparent border border-border rounded px-1.5 py-0.5 text-foreground outline-none focus:border-primary"
                  />
                </div>
              ) : (
                <ListRow
                  key={item.id}
                  item={item}
                  selected={selectedId === item.id}
                  onClick={() => item.type === "folder" ? onFolderOpen(item.id) : setSelectedId(item.id)}
                  onDoubleClick={() => handleDoubleClick(item)}
                  onContextMenu={(e) => openCtx(e, item.id)}
                  onMouseEnter={item.type === "folder" && onFolderHover ? () => onFolderHover(item.id) : undefined}
                />
              )
            ))}
          </div>
        )}
      </div>

      {/* ── Drag & drop overlay ── */}
      {draggingOver && (
        <div className="absolute inset-0 z-50 flex items-center justify-center pointer-events-none">
          <div className="absolute inset-2 border-2 border-dashed border-primary rounded-xl bg-primary/5" />
          <div className="relative flex flex-col items-center gap-2">
            <Upload className="w-8 h-8 text-foreground" />
            <span className="text-sm text-foreground font-medium">Drop to upload</span>
          </div>
        </div>
      )}

      {/* ── Context menu ── */}
      {ctxMenu && ctxItem && (
        <ContextMenu
          x={ctxMenu.x}
          y={ctxMenu.y}
          isFolder={ctxItem.type === "folder"}
          readOnly={readOnly}
          onOpen={() => onFolderOpen(ctxItem.id)}
          onOpenFile={() => onOpenFile?.(ctxItem.id)}
          onIngest={() => onIngest(ctxItem.id)}
          onRename={() => startRename(ctxItem.id)}
          onDelete={() => onDelete(ctxItem.id)}
          onClose={() => setCtxMenu(null)}
        />
      )}

      {/* ── Upload progress panel ── */}
      {uploadProgress && uploadProgress.length > 0 && (
        <div className="shrink-0 border-t border-border bg-surface/80 backdrop-blur">
          <div className="px-4 py-2 text-[11px] text-muted-foreground uppercase tracking-wide">
            Uploading {uploadProgress.filter((p) => p.phase !== "done" && p.phase !== "error").length} of {uploadProgress.length} files
          </div>
          <div className="max-h-32 overflow-y-auto px-4 pb-2 space-y-1.5">
            {uploadProgress.map((p, i) => (
              <div key={i} className="flex items-center gap-3">
                <span className="text-xs text-foreground truncate flex-1 min-w-0">{p.fileName}</span>
                <div className="w-32 h-1.5 rounded-full bg-surface-raised overflow-hidden shrink-0">
                  <div
                    className={cn(
                      "h-full rounded-full transition-all duration-300",
                      p.phase === "error" ? "bg-red-500" : p.phase === "done" ? "bg-green-500" : "bg-primary"
                    )}
                    style={{ width: `${p.percent}%` }}
                  />
                </div>
                <span className="text-[11px] text-muted-foreground text-right shrink-0 whitespace-nowrap">
                  {p.phase === "error"
                    ? "Error"
                    : p.phase === "done"
                      ? <Check className="w-3 h-3 text-green-500 inline" />
                      : p.phase === "confirming"
                        ? "Confirming…"
                        : `${p.percent}%${p.speedMBps > 0 ? ` — ${p.speedMBps} MB/s` : ""}${p.remainingMins > 0 ? ` — ~${p.remainingMins} min` : ""}`}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* ── Status bar ── */}
      <div className="shrink-0 h-6 px-4 flex items-center border-t border-border bg-surface/60 text-[11px] text-muted-foreground">
        {sorted.length} item{sorted.length !== 1 && "s"}
      </div>
    </div>
  );
}
