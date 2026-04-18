import { BlockBlobClient } from "@azure/storage-blob";
import { apiFetch } from "./auth";

export interface UploadProgress {
  fileIndex: number;
  fileName: string;
  percent: number;
  speedMBps: number;
  remainingSecs: number;
  phase: "queued" | "uploading" | "confirming" | "done" | "paused" | "cancelled" | "error";
  errorMessage?: string;
}

export type OnUploadProgress = (progress: UploadProgress) => void;

/* ── AbortController-based cancel/pause ─────────────────────────────────── */

export class UploadController {
  private _abortControllers: Map<number, AbortController> = new Map();
  private _paused: Set<number> = new Set();
  private _cancelled: Set<number> = new Set();

  /** Get or create an AbortController for a file index */
  getSignal(fileIndex: number): AbortSignal {
    if (!this._abortControllers.has(fileIndex)) {
      this._abortControllers.set(fileIndex, new AbortController());
    }
    return this._abortControllers.get(fileIndex)!.signal;
  }

  cancel(fileIndex: number) {
    this._cancelled.add(fileIndex);
    this._abortControllers.get(fileIndex)?.abort();
  }

  cancelAll() {
    this._abortControllers.forEach((ctrl, idx) => {
      this._cancelled.add(idx);
      ctrl.abort();
    });
  }

  pause(fileIndex: number) {
    this._paused.add(fileIndex);
    this._abortControllers.get(fileIndex)?.abort();
  }

  resume(fileIndex: number) {
    this._paused.delete(fileIndex);
    // Reset the controller so a new upload attempt gets a fresh signal
    this._abortControllers.set(fileIndex, new AbortController());
  }

  isPaused(fileIndex: number): boolean {
    return this._paused.has(fileIndex);
  }

  isCancelled(fileIndex: number): boolean {
    return this._cancelled.has(fileIndex);
  }

  cleanup() {
    this._abortControllers.clear();
    this._paused.clear();
    this._cancelled.clear();
  }
}

/* ── Speed tracker with smoothing ───────────────────────────────────────── */

class SpeedTracker {
  private samples: { time: number; bytes: number }[] = [];
  private windowMs = 5000; // 5-second rolling window

  update(loadedBytes: number): { speedMBps: number; remainingSecs: number; } {
    const now = Date.now();
    this.samples.push({ time: now, bytes: loadedBytes });

    // Trim old samples outside window
    const cutoff = now - this.windowMs;
    this.samples = this.samples.filter((s) => s.time >= cutoff);

    return { speedMBps: 0, remainingSecs: 0 };
  }

  calc(totalSize: number, loadedBytes: number): { speedMBps: number; remainingSecs: number } {
    if (this.samples.length < 2) return { speedMBps: 0, remainingSecs: 0 };

    const oldest = this.samples[0];
    const newest = this.samples[this.samples.length - 1];
    const elapsedSec = (newest.time - oldest.time) / 1000;
    const bytesDelta = newest.bytes - oldest.bytes;

    if (elapsedSec <= 0 || bytesDelta <= 0) return { speedMBps: 0, remainingSecs: 0 };

    const speedBps = bytesDelta / elapsedSec;
    const speedMBps = parseFloat((speedBps / (1024 * 1024)).toFixed(1));
    const remainingBytes = totalSize - loadedBytes;
    const remainingSecs = Math.max(0, Math.round(remainingBytes / speedBps));

    return { speedMBps, remainingSecs };
  }
}

/* ── Single file upload ─────────────────────────────────────────────────── */

export async function uploadFileDirect(
  file: File,
  fileIndex: number,
  folderId: string | null,
  onProgress: OnUploadProgress,
  containerId: string,
  abortSignal?: AbortSignal,
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
    signal: abortSignal,
  });

  if (!sasRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "error", errorMessage: `SAS URL failed: ${sasRes.status}` });
    throw new Error(`Failed to get upload URL: ${sasRes.status}`);
  }

  const { file_id, sas_url, blob_name } = await sasRes.json();

  // 2. Upload directly to Azure Blob — 4MB blocks, 6 parallel
  onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "uploading" });

  const uploadStartTime = Date.now();
  const tracker = new SpeedTracker();
  const fileSize = file.size;

  const blockBlobClient = new BlockBlobClient(sas_url);
  await blockBlobClient.uploadData(file, {
    blockSize: 4 * 1024 * 1024,    // 4 MB blocks
    concurrency: 6,                // 6 parallel block uploads
    abortSignal,
    onProgress: (ev) => {
      tracker.update(ev.loadedBytes);
      const { speedMBps, remainingSecs } = tracker.calc(fileSize, ev.loadedBytes);
      const percent = Math.min(99, Math.round((ev.loadedBytes / fileSize) * 100));

      onProgress({ fileIndex, fileName: filename, percent, speedMBps, remainingSecs, phase: "uploading" });
    },
    blobHTTPHeaders: {
      blobContentType: file.type || "application/octet-stream",
    },
  });

  // 3. Confirm upload with backend
  const uploadDurationSecs = parseFloat(((Date.now() - uploadStartTime) / 1000).toFixed(1));
  onProgress({ fileIndex, fileName: filename, percent: 99, speedMBps: 0, remainingSecs: 0, phase: "confirming" });

  const confirmRes = await apiFetch("/api/files/confirm-upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      file_id,
      blob_name,
      filename,
      content_type: file.type || undefined,
      size: file.size,
      upload_duration_secs: uploadDurationSecs,
      folder_id: folderId,
      container_id: containerId,
    }),
    signal: abortSignal,
  });

  if (!confirmRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "error", errorMessage: `Confirm failed: ${confirmRes.status}` });
    throw new Error(`Failed to confirm upload: ${confirmRes.status}`);
  }

  onProgress({ fileIndex, fileName: filename, percent: 100, speedMBps: 0, remainingSecs: 0, phase: "done" });
}

/* ── Queue-based multi-file uploader ────────────────────────────────────── */

export async function uploadFileQueue(
  files: File[],
  folderId: string | null,
  containerId: string,
  onProgress: OnUploadProgress,
  controller: UploadController,
  maxConcurrent: number = 2,
): Promise<void> {
  // Mark all as queued
  files.forEach((f, i) => {
    onProgress({ fileIndex: i, fileName: f.name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "queued" });
  });

  let nextIndex = 0;
  const running = new Set<Promise<void>>();

  const startOne = (idx: number): Promise<void> => {
    if (controller.isCancelled(idx)) {
      onProgress({ fileIndex: idx, fileName: files[idx].name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "cancelled" });
      return Promise.resolve();
    }

    const signal = controller.getSignal(idx);

    return uploadFileDirect(files[idx], idx, folderId, onProgress, containerId, signal)
      .catch((err) => {
        if (controller.isCancelled(idx)) {
          onProgress({ fileIndex: idx, fileName: files[idx].name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "cancelled" });
        } else if (controller.isPaused(idx)) {
          onProgress({ fileIndex: idx, fileName: files[idx].name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "paused" });
        } else {
          onProgress({ fileIndex: idx, fileName: files[idx].name, percent: 0, speedMBps: 0, remainingSecs: 0, phase: "error", errorMessage: err?.message || "Upload failed" });
        }
      });
  };

  // Process queue with concurrency limit
  while (nextIndex < files.length || running.size > 0) {
    // Fill up to maxConcurrent
    while (nextIndex < files.length && running.size < maxConcurrent) {
      const idx = nextIndex++;
      const promise = startOne(idx).then(() => {
        running.delete(promise);
      });
      running.add(promise);
    }

    // Wait for at least one to finish
    if (running.size > 0) {
      await Promise.race(running);
    }
  }
}
