import { BlockBlobClient } from "@azure/storage-blob";
import { apiFetch } from "./auth";

export interface UploadProgress {
  fileIndex: number;
  fileName: string;
  percent: number; // 0-100
  speedMBps: number; // current upload speed in MB/s
  remainingMins: number; // estimated minutes remaining
  phase: "uploading" | "confirming" | "done" | "error";
}

export type OnUploadProgress = (progress: UploadProgress) => void;

/**
 * Upload files directly to Azure Blob via SAS URL (supports 10 GB+).
 * Flow: get SAS URL → upload via BlockBlobClient (64MB parallel chunks) → confirm with server.
 */
export async function uploadFileDirect(
  file: File,
  fileIndex: number,
  folderId: string | null,
  onProgress: OnUploadProgress,
  containerId: string
): Promise<void> {
  const filename = file.name;

  // 1. Get SAS URL from backend
  const sasRes = await apiFetch("/api/files/upload-url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filename,
      content_type: file.type || undefined,
      folder_id: folderId,
      container_id: containerId,
    }),
  });

  if (!sasRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingMins: 0, phase: "error" });
    throw new Error(`Failed to get upload URL: ${sasRes.status}`);
  }

  const { file_id, sas_url, blob_name } = await sasRes.json();

  // 2. Upload directly to Azure via SAS URL — 64MB parallel chunks
  onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingMins: 0, phase: "uploading" });

  let lastLoaded = 0;
  let lastTime = Date.now();

  const blockBlobClient = new BlockBlobClient(sas_url);
  await blockBlobClient.uploadData(file, {
    blockSize: 64 * 1024 * 1024, // 64 MB blocks — optimal for Azure Blob
    concurrency: 4,               // 4 parallel uploads — sweet spot before throttling
    onProgress: (ev) => {
      const now = Date.now();
      const elapsedSec = (now - lastTime) / 1000;
      const bytesDelta = ev.loadedBytes - lastLoaded;

      let speedMBps = 0;
      let remainingMins = 0;

      if (elapsedSec > 0 && bytesDelta > 0) {
        const speedBps = bytesDelta / elapsedSec;
        speedMBps = parseFloat((speedBps / (1024 * 1024)).toFixed(1));
        const remainingBytes = file.size - ev.loadedBytes;
        const remainingSec = remainingBytes / speedBps;
        remainingMins = Math.max(0, Math.ceil(remainingSec / 60));
      }

      const percent = Math.min(99, Math.round((ev.loadedBytes / file.size) * 100));

      lastLoaded = ev.loadedBytes;
      lastTime = now;

      onProgress({ fileIndex, fileName: filename, percent, speedMBps, remainingMins, phase: "uploading" });
    },
    blobHTTPHeaders: {
      blobContentType: file.type || "application/octet-stream",
    },
  });

  // 3. Confirm upload with backend
  onProgress({ fileIndex, fileName: filename, percent: 99, speedMBps: 0, remainingMins: 0, phase: "confirming" });

  const confirmRes = await apiFetch("/api/files/confirm-upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      file_id,
      blob_name,
      filename,
      content_type: file.type || undefined,
      size: file.size,
      folder_id: folderId,
      container_id: containerId,
    }),
  });

  if (!confirmRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingMins: 0, phase: "error" });
    throw new Error(`Failed to confirm upload: ${confirmRes.status}`);
  }

  onProgress({ fileIndex, fileName: filename, percent: 100, speedMBps: 0, remainingMins: 0, phase: "done" });
}
