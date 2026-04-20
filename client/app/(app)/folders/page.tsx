"use client";

import { useState, useCallback } from "react";
import useSWR, { mutate as globalMutate } from "swr";
import FileManagerView, {
  type FileItem,
  type FileStatus,
  type FileType,
  type UploadProgressItem,
} from "@/components/file-manager/FileManagerView";
import { apiFetch } from "@/lib/auth";
import { useAuth } from "@/components/auth-provider";
import { uploadFileQueue, UploadController, type UploadProgress } from "@/lib/upload";

/* ── types for container picker ──────────────────────────────────────────── */

interface Container {
  id: string;
  name: string;
}

const containersFetcher = async (): Promise<Container[]> => {
  const res = await apiFetch("/api/containers");
  if (!res.ok) return [];
  return res.json();
};

function mapFolder(f: { id: string; name: string; created_at: string }): FileItem {
  return {
    id: f.id,
    name: f.name,
    type: "folder",
    size: 0,
    status: "not_ingested",
    lastModified: new Date(f.created_at),
  };
}

function mapFile(f: {
  id: string;
  name: string;
  content_type: string;
  size: number;
  created_at: string;
  container_id?: string | null;
  ingest_status?: string;
  uploaded_by_name?: string | null;
}): FileItem {
  const ext = f.name.split(".").pop()?.toLowerCase() ?? "";
  let type: FileType = "txt";
  if (ext === "csv") type = "csv";
  else if (ext === "xlsx" || ext === "xls") type = "xlsx";
  else if (ext === "pdf") type = "pdf";

  return {
    id: f.id,
    name: f.name,
    type,
    size: f.size,
    status: (
      f.ingest_status === "ingested" ? "indexed" : (f.ingest_status ?? "not_ingested")
    ) as FileStatus,
    lastModified: new Date(f.created_at),
    synced: !!f.container_id,
    uploadedBy: f.uploaded_by_name || undefined,
  };
}

const contentsFetcher = async (key: string): Promise<FileItem[]> => {
  const folderId = key.replace("contents:", "");
  const url = `/api/folders/${folderId}/contents`;
  const res = await apiFetch(url);
  if (!res.ok) return [];
  const data = await res.json();
  return [
    ...(data.folders ?? []).map(mapFolder),
    ...(data.files ?? [])
      .filter((f: { name: string }) => !f.name.toLowerCase().endsWith(".parquet"))
      .map(mapFile),
  ];
};

interface FolderBreadcrumb {
  id: string;
  name: string;
}

export default function FoldersPage() {
  const { user } = useAuth();
  const isAdmin = user?.is_admin ?? false;

  const [folderStack, setFolderStack] = useState<FolderBreadcrumb[]>([]);
  const [selectedContainerId, setSelectedContainerId] = useState<string>("");

  const currentFolderId =
    folderStack.length > 0 ? folderStack[folderStack.length - 1].id : null;
  const folderName =
    folderStack.length > 0 ? folderStack[folderStack.length - 1].name : undefined;
  const swrKey = `contents:${currentFolderId ?? "root"}`;

  const { data: items, isLoading, mutate } = useSWR(swrKey, contentsFetcher, {
    revalidateOnFocus: false,
  });

  // Fetch containers for the picker (admin only)
  const { data: containers } = useSWR(
    isAdmin ? "containers-list" : null,
    containersFetcher,
    { revalidateOnFocus: false }
  );

  const handleFolderOpen = useCallback(
    (id: string) => {
      const folder = items?.find((i) => i.id === id && i.type === "folder");
      if (folder) {
        setFolderStack((prev) => [...prev, { id: folder.id, name: folder.name }]);
      }
    },
    [items]
  );

  const handleFolderHover = useCallback((id: string) => {
    const prefetchKey = `contents:${id}`;
    globalMutate(prefetchKey, contentsFetcher(prefetchKey), false);
  }, []);

  const handleBack = useCallback(() => {
    setFolderStack((prev) => prev.slice(0, -1));
  }, []);

  const [uploadProgress, setUploadProgress] = useState<UploadProgressItem[]>([]);
  const [uploadCtrl, setUploadCtrl] = useState<UploadController | null>(null);

  const handleUpload = useCallback(
    async (files: File[]) => {
      if (!selectedContainerId) return;

      const ctrl = new UploadController();
      setUploadCtrl(ctrl);

      // Init progress state — all queued
      setUploadProgress(
        files.map((f) => ({ fileName: f.name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "queued" as const }))
      );

      const onProgress = (p: UploadProgress) => {
        setUploadProgress((prev) =>
          prev.map((item, i) =>
            i === p.fileIndex
              ? { fileName: p.fileName, percent: p.percent, speedMBps: p.speedMBps, remainingSecs: p.remainingSecs, phase: p.phase }
              : item
          )
        );
      };

      // Queue-based upload: 2 files at a time
      await uploadFileQueue(files, currentFolderId, selectedContainerId, onProgress, ctrl, 2);

      await mutate();
      ctrl.cleanup();
      setUploadCtrl(null);

      // Clear progress after a short delay
      setTimeout(() => setUploadProgress([]), 3000);
    },
    [currentFolderId, mutate, selectedContainerId]
  );

  const handleCancelUpload = useCallback((fileIndex: number) => {
    uploadCtrl?.cancel(fileIndex);
  }, [uploadCtrl]);

  const handleCancelAll = useCallback(() => {
    uploadCtrl?.cancelAll();
  }, [uploadCtrl]);

  const handleDelete = useCallback(
    async (id: string) => {
      const item = items?.find((i) => i.id === id);
      if (!item) return;
      const endpoint =
        item.type === "folder" ? "/api/folders/" + id : "/api/files/" + id;
      try {
        await apiFetch(endpoint, { method: "DELETE" });
        await mutate();
      } catch (err) {
        await mutate();
      }
    },
    [items, swrKey, mutate]
  );

  const handleIngest = useCallback(async (id: string) => {
    try {
      const res = await apiFetch("/api/chat/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ file_ids: [id] }),
      });
      if (!res.ok) return;
      // Optimistic: mark as pending in local data
      await mutate();
      // Poll until status changes from pending
      const poll = setInterval(async () => {
        const statusRes = await apiFetch(`/api/chat/ingest-status/${id}`);
        if (statusRes.ok) {
          const data = await statusRes.json();
          if (data.ingest_status !== "pending") {
            clearInterval(poll);
            await mutate();
          }
        }
      }, 3000);
      // Safety: stop polling after 2 minutes
      setTimeout(() => { clearInterval(poll); mutate(); }, 120_000);
    } catch {
      // silently ignore
    }
  }, [mutate]);

  const handleOpenFile = useCallback(async (id: string) => {
    try {
      const res = await apiFetch(`/api/files/${id}/signed-url`);
      if (!res.ok) return;
      const { signed_url } = await res.json();
      window.open(signed_url, "_blank");
    } catch {
      // signed-url fetch failed — silently ignore
    }
  }, []);

  const [reingestLoading, setReingestLoading] = useState(false);

  const handleReingestAll = useCallback(async () => {
    setReingestLoading(true);
    try {
      const res = await apiFetch("/api/admin/reingest-all", { method: "POST" });
      if (!res.ok) {
        setReingestLoading(false);
        return;
      }
      // Poll until all files are done re-ingesting
      const poll = setInterval(async () => {
        await mutate();
        const freshItems = await contentsFetcher(swrKey);
        const stillPending = freshItems.some(
          (i) => i.type !== "folder" && (i.status === "pending" || i.status === "not_ingested")
        );
        if (!stillPending) {
          clearInterval(poll);
          setReingestLoading(false);
          await mutate();
        }
      }, 5000);
      // Safety: stop after 5 minutes
      setTimeout(() => { clearInterval(poll); setReingestLoading(false); mutate(); }, 300_000);
    } catch {
      setReingestLoading(false);
    }
  }, [mutate, swrKey]);

  const handleCreateFolder = useCallback(
    async (name: string) => {
      const payload = {
        name,
        parent_id: currentFolderId ?? undefined,
      };
      try {
        await apiFetch("/api/folders", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
        });
        await mutate();
      } catch {
        await mutate();
      }
    },
    [currentFolderId, swrKey, mutate]
  );

  const handleRename = useCallback(
    async (id: string, newName: string) => {
      const item = items?.find((i) => i.id === id);
      if (!item) return;
      const endpoint =
        item.type === "folder" ? "/api/folders/" + id : "/api/files/" + id;
      try {
        await apiFetch(endpoint, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: newName }),
        });
        await mutate();
      } catch {
        await mutate();
      }
    },
    [items, swrKey, mutate]
  );

  return (
    <FileManagerView
      files={items ?? []}
      folderName={folderName}
      loading={isLoading}
      readOnly={!isAdmin}
      uploadProgress={uploadProgress}
      containers={isAdmin ? containers ?? [] : []}
      selectedContainerId={selectedContainerId}
      onContainerChange={setSelectedContainerId}
      onUpload={handleUpload}
      onIngest={handleIngest}
      onDelete={handleDelete}
      onRename={handleRename}
      onCreateFolder={handleCreateFolder}
      onFolderOpen={handleFolderOpen}
      onOpenFile={handleOpenFile}
      onFolderHover={handleFolderHover}
      onBack={folderStack.length > 0 ? handleBack : undefined}
      onCancelUpload={handleCancelUpload}
      onCancelAllUploads={handleCancelAll}
      onReingestAll={isAdmin ? handleReingestAll : undefined}
      reingestLoading={reingestLoading}
    />
  );
}