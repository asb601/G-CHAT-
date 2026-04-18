import { BlockBlobClient } from "@azure/storage-blob";
import { apiFetch } from "./auth";

export interface UploadProgress {
  fileIndex: number;
  fileName: string;
  percent: number; // 0-100
  speedMBps: number; // current upload speed in MB/s
  remainingMins: number; // estimated minutes remaining
  phase: "compressing" | "uploading" | "confirming" | "done" | "error";
  compressionRatio?: number; // e.g. 5.2 means 5.2x smaller
}

export type OnUploadProgress = (progress: UploadProgress) => void;

/** Check if file is a compressible text type (CSV, TSV, TXT) */
function isCompressible(filename: string): boolean {
  const ext = filename.split(".").pop()?.toLowerCase() || "";
  return ["csv", "tsv", "txt"].includes(ext);
}

/**
 * Compress a File using gzip via the native CompressionStream API.
 * CSVs compress 5-10x (6GB → 600MB-1.2GB).
 */
async function compressFile(
  file: File,
  onProgress: (percent: number) => void
): Promise<Blob> {
  const stream = file.stream();
  const gzipStream = stream.pipeThrough(new CompressionStream("gzip"));

  // Collect compressed chunks and report progress
  const reader = gzipStream.getReader();
  const chunks: BlobPart[] = [];
  let compressedSize = 0;
  // Estimate progress based on read position vs file size
  let readSoFar = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    compressedSize += value.byteLength;
    // Estimate: compressed output is roughly proportional to input read
    // Use compressed size as a rough proxy (imperfect but shows movement)
    readSoFar = Math.min(file.size, readSoFar + value.byteLength * 6); // rough estimate
    onProgress(Math.min(95, Math.round((readSoFar / file.size) * 100)));
  }

  onProgress(100);
  return new Blob(chunks, { type: "application/gzip" });
}

/**
 * Upload files directly to Azure Blob via SAS URL (supports 10 GB+).
 * For CSV/TSV/TXT: compresses with gzip first (5-10x smaller = 5-10x faster upload).
 * Flow: [compress] → get SAS URL → upload via BlockBlobClient → confirm with server.
 */
export async function uploadFileDirect(
  file: File,
  fileIndex: number,
  folderId: string | null,
  onProgress: OnUploadProgress,
  containerId: string
): Promise<void> {
  const filename = file.name;
  const shouldCompress = isCompressible(filename);
  let uploadBlob: Blob = file;
  let uploadFilename = filename;
  let compressionRatio: number | undefined;

  // Compress CSV/TSV/TXT files before upload (5-10x smaller)
  if (shouldCompress) {
    onProgress({
      fileIndex, fileName: filename, percent: 0, speedMBps: 0,
      remainingMins: 0, phase: "compressing",
    });

    const compressed = await compressFile(file, (pct) => {
      onProgress({
        fileIndex, fileName: filename, percent: pct, speedMBps: 0,
        remainingMins: 0, phase: "compressing",
      });
    });

    compressionRatio = parseFloat((file.size / compressed.size).toFixed(1));
    uploadBlob = compressed;
    uploadFilename = filename + ".gz";
  }

  // 1. Get SAS URL from backend (use .gz filename so backend knows it's compressed)
  const sasRes = await apiFetch("/api/files/upload-url", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      filename: uploadFilename,
      content_type: shouldCompress ? "application/gzip" : (file.type || undefined),
      folder_id: folderId,
      container_id: containerId,
    }),
  });

  if (!sasRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingMins: 0, phase: "error" });
    throw new Error(`Failed to get upload URL: ${sasRes.status}`);
  }

  const { file_id, sas_url, blob_name } = await sasRes.json();

  // 2. Upload to Azure via SAS URL
  onProgress({
    fileIndex, fileName: filename, percent: 0, speedMBps: 0,
    remainingMins: 0, phase: "uploading", compressionRatio,
  });

  let lastLoaded = 0;
  let lastTime = Date.now();
  const uploadSize = uploadBlob.size;

  const blockBlobClient = new BlockBlobClient(sas_url);
  await blockBlobClient.uploadData(uploadBlob, {
    blockSize: 8 * 1024 * 1024,   // 8 MB blocks
    concurrency: 8,               // 8 parallel uploads
    onProgress: (ev) => {
      const now = Date.now();
      const elapsedSec = (now - lastTime) / 1000;
      const bytesDelta = ev.loadedBytes - lastLoaded;

      let speedMBps = 0;
      let remainingMins = 0;

      if (elapsedSec > 0 && bytesDelta > 0) {
        const speedBps = bytesDelta / elapsedSec;
        speedMBps = parseFloat((speedBps / (1024 * 1024)).toFixed(1));
        const remainingBytes = uploadSize - ev.loadedBytes;
        const remainingSec = remainingBytes / speedBps;
        remainingMins = Math.max(0, Math.ceil(remainingSec / 60));
      }

      const percent = Math.min(99, Math.round((ev.loadedBytes / uploadSize) * 100));

      lastLoaded = ev.loadedBytes;
      lastTime = now;

      onProgress({
        fileIndex, fileName: filename, percent, speedMBps,
        remainingMins, phase: "uploading", compressionRatio,
      });
    },
    blobHTTPHeaders: {
      blobContentType: shouldCompress ? "application/gzip" : (file.type || "application/octet-stream"),
    },
  });

  // 3. Confirm upload with backend — send original filename + original size
  //    Backend sees .gz extension and knows to decompress before processing
  onProgress({ fileIndex, fileName: filename, percent: 99, speedMBps: 0, remainingMins: 0, phase: "confirming" });

  const confirmRes = await apiFetch("/api/files/confirm-upload", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      file_id,
      blob_name,
      filename: uploadFilename,
      original_filename: filename,
      content_type: shouldCompress ? "application/gzip" : (file.type || undefined),
      size: file.size,            // original size for display
      compressed_size: shouldCompress ? uploadBlob.size : undefined,
      folder_id: folderId,
      container_id: containerId,
    }),
  });

  if (!confirmRes.ok) {
    onProgress({ fileIndex, fileName: filename, percent: 0, speedMBps: 0, remainingMins: 0, phase: "error" });
    throw new Error(`Failed to confirm upload: ${confirmRes.status}`);
  }

  onProgress({
    fileIndex, fileName: filename, percent: 100, speedMBps: 0,
    remainingMins: 0, phase: "done", compressionRatio,
  });
}
