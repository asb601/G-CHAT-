"""
Backfill search_text + description_embedding for all existing FileMetadata rows.

Runs as a standalone async script — safe to re-run, idempotent.
Rows that already have a non-null description_embedding are skipped unless
--force is passed.

Usage (from server/):
    uv run python -m app.migrations.backfill_embeddings
    uv run python -m app.migrations.backfill_embeddings --force
    uv run python -m app.migrations.backfill_embeddings --batch-size 50 --concurrency 5

Strategy
--------
- Read rows in pages of `--batch-size` (default 20)  → avoids OOM on large catalogs
- `--concurrency` (default 5) rows embedded in parallel per page  → 5× throughput
- Each row: build_search_text() → embed_text() → write both columns → commit
- Azure rate-limit (429): linear back-off up to 60 s, then skip + warn
- Progress logged to stdout in the same structlog JSON format as ingest_logger
- Summary at end: total / updated / skipped / failed
"""
from __future__ import annotations

import argparse
import asyncio
import time

import structlog
from sqlalchemy import func, select

from app.core.database import async_session
from app.core.logger import ingest_logger
from app.models.file_metadata import FileMetadata
from app.retrieval.embeddings import build_search_text, embed_text

logger = structlog.get_logger("backfill_embeddings")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _embed_row(
    metadata: FileMetadata,
    *,
    semaphore: asyncio.Semaphore,
    retries: int = 3,
) -> tuple[str, list[float] | None, str | None]:
    """
    Returns (file_id, embedding_or_None, error_or_None).
    Acquires semaphore to cap in-flight concurrent API calls.
    """
    search_text = build_search_text(metadata)
    async with semaphore:
        for attempt in range(retries):
            try:
                embedding = await embed_text(search_text)
                # If we got a zero vector (deployment degraded), treat as failure
                if not any(x != 0.0 for x in embedding):
                    return metadata.id, None, "zero_vector (deployment not live?)"
                return metadata.id, embedding, search_text
            except Exception as exc:
                err = str(exc)
                if "429" in err or "RateLimitError" in err:
                    wait = (attempt + 1) * 15
                    logger.warning("rate_limit_backoff", id=metadata.id, attempt=attempt + 1, wait_s=wait)
                    await asyncio.sleep(wait)
                else:
                    return metadata.id, None, err[:200]
        return metadata.id, None, f"max_retries={retries} exhausted"


# ---------------------------------------------------------------------------
# Main backfill coroutine
# ---------------------------------------------------------------------------

async def run_backfill(*, batch_size: int = 20, concurrency: int = 5, force: bool = False) -> None:
    semaphore = asyncio.Semaphore(concurrency)
    t_start = time.perf_counter()

    total = updated = skipped = failed = 0

    async with async_session() as db:
        # Count
        count_q = select(func.count()).select_from(FileMetadata)
        if not force:
            count_q = count_q.where(FileMetadata.description_embedding.is_(None))
        total_rows: int = (await db.execute(count_q)).scalar_one()
        logger.info("backfill_start",
                    total_rows=total_rows,
                    batch_size=batch_size,
                    concurrency=concurrency,
                    force=force)

        # When filtering (normal mode): always fetch from offset=0.
        # After each commit, processed rows are excluded by IS NULL → no double processing.
        # When --force: rows are updated in-place so we must advance offset normally.
        offset = 0
        while True:
            q = select(FileMetadata).order_by(FileMetadata.id)
            if not force:
                q = q.where(FileMetadata.description_embedding.is_(None))
                q = q.limit(batch_size)
            else:
                q = q.offset(offset).limit(batch_size)

            rows: list[FileMetadata] = list((await db.execute(q)).scalars().all())
            if not rows:
                break

            total += len(rows)
            logger.info("backfill_page", offset=offset, batch=len(rows))

            # Embed all rows in this page concurrently (bounded by semaphore)
            tasks = [_embed_row(row, semaphore=semaphore) for row in rows]
            results = await asyncio.gather(*tasks)

            # Write results back
            for row, (row_id, embedding, payload) in zip(rows, results):
                if embedding is None:
                    failed += 1
                    logger.warning("backfill_row_failed", id=row_id, error=payload)
                    continue

                row.search_text = payload      # payload = search_text on success
                row.description_embedding = embedding
                updated += 1

            await db.commit()

            pct = round(total / total_rows * 100, 1) if total_rows else 100
            elapsed = round(time.perf_counter() - t_start, 1)
            logger.info("backfill_progress",
                        processed=total,
                        of=total_rows,
                        pct=pct,
                        updated=updated,
                        skipped=skipped,
                        failed=failed,
                        elapsed_s=elapsed)

            offset += batch_size  # only matters in --force mode

    elapsed_total = round(time.perf_counter() - t_start, 1)
    logger.info("backfill_complete",
                total=total,
                updated=updated,
                skipped=skipped,
                failed=failed,
                elapsed_s=elapsed_total)
    print(
        f"\n✓ Backfill done in {elapsed_total}s — "
        f"updated={updated}  skipped={skipped}  failed={failed}"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill embeddings for FileMetadata rows")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Rows per DB page (default: 20)")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="Max parallel embed_text calls (default: 5)")
    parser.add_argument("--force", action="store_true",
                        help="Re-embed rows that already have an embedding")
    args = parser.parse_args()
    asyncio.run(run_backfill(
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        force=args.force,
    ))


if __name__ == "__main__":
    main()
