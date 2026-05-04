"""
Conversation context service — handles token counting, rolling summaries,
context window building, and auto-title generation.

Three-layer context strategy:
  Layer 1: Rolling summary (compressed history of older messages)
  Layer 2: Last N recent messages (verbatim, within token budget)
  Layer 3: Current user query

This keeps the agent contextually aware without blowing token limits.
"""
from __future__ import annotations

import tiktoken
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.agent.llm import get_llm
from app.core.logger import chat_logger
from app.models.conversation import Conversation, Message

# ── Constants ──

CONTEXT_TOKEN_BUDGET = 1500  # max tokens for conversation history (halved from 3000 to cut bloat)
RECENT_MESSAGE_COUNT = 6     # last N messages verbatim (down from 10)
PER_MESSAGE_TOKEN_CAP = 200  # cap any single prior message at this many tokens
SUMMARY_REGEN_INTERVAL = 10  # regenerate summary every N messages
TITLE_MODEL_MAX_TOKENS = 60  # cheap call for title generation
SUMMARY_MODEL_MAX_TOKENS = 300

# tiktoken encoder — cl100k_base covers GPT-4 / GPT-4o
_encoder: tiktoken.Encoding | None = None


def _get_encoder() -> tiktoken.Encoding:
    global _encoder
    if _encoder is None:
        _encoder = tiktoken.get_encoding("cl100k_base")
    return _encoder


# ── Token counting ──


def count_tokens(text: str) -> int:
    """Count tokens for a string using cl100k_base (GPT-4 family)."""
    if not text:
        return 0
    return len(_get_encoder().encode(text))


def _trim_history_message(content: str, role: str) -> str:
    """Compress a prior message before re-sending to the LLM.

    - Drop markdown pipe-table rows (LLM already saw them once; UI re-renders).
    - Drop tab-separated rows (TSV blocks).
    - Cap to PER_MESSAGE_TOKEN_CAP tokens.
    Applied to both user and assistant messages.
    """
    if not content:
        return ""
    lines = []
    for ln in content.splitlines():
        s = ln.strip()
        if s.startswith("|") and s.endswith("|"):
            continue
        if s.count("\t") >= 2:
            continue
        lines.append(ln)
    trimmed = "\n".join(lines).strip()
    if not trimmed:
        return "[prior tabular response — omitted]"
    enc = _get_encoder()
    toks = enc.encode(trimmed)
    if len(toks) > PER_MESSAGE_TOKEN_CAP:
        trimmed = enc.decode(toks[:PER_MESSAGE_TOKEN_CAP]) + " …[truncated]"
    return trimmed


# ── Context building ──


async def build_conversation_context(
    conv: Conversation,
    db: AsyncSession,
) -> str:
    """
    Build a context string to inject into the agent's system prompt.

    Returns a formatted string containing:
      - Rolling summary of older messages (if available)
      - Last N messages verbatim (within token budget)

    Returns empty string for brand-new conversations.
    """
    # Fetch recent messages ordered by created_at DESC
    q = (
        select(Message)
        .where(Message.conversation_id == conv.id)
        .order_by(Message.created_at.desc())
        .limit(RECENT_MESSAGE_COUNT)
    )
    recent = list(reversed((await db.execute(q)).scalars().all()))

    if not recent:
        return ""

    # Build context parts within token budget
    parts: list[str] = []
    tokens_used = 0

    # Layer 1: Rolling summary
    if conv.summary:
        summary_tokens = count_tokens(conv.summary)
        if summary_tokens < CONTEXT_TOKEN_BUDGET * 0.4:  # cap summary at 40% of budget
            parts.append(f"[Conversation summary so far]\n{conv.summary}")
            tokens_used += summary_tokens

    # Layer 2: Recent messages (most recent first priority)
    msg_parts: list[str] = []
    for msg in recent:
        role_label = "User" if msg.role == "user" else "Assistant"
        # Strip data tables / TSV / long bullet lists from prior assistant messages —
        # the data is stored separately and re-sending wastes tokens. Keep prose only.
        body = _trim_history_message(msg.content, msg.role)
        msg_text = f"{role_label}: {body}"
        msg_tokens = count_tokens(msg_text)

        if tokens_used + msg_tokens > CONTEXT_TOKEN_BUDGET:
            break
        msg_parts.append(msg_text)
        tokens_used += msg_tokens

    if msg_parts:
        parts.append("[Recent conversation]\n" + "\n".join(msg_parts))

    if not parts:
        return ""

    return "\n\n".join(parts)


# ── Rolling summary generation ──


async def maybe_regenerate_summary(
    conv_id: str,
    db: AsyncSession,
) -> None:
    """
    Check if the conversation has crossed a summary threshold and
    regenerate the rolling summary if needed.

    Called as a background task after each message pair is saved.
    """
    # Count total messages
    count_q = select(func.count(Message.id)).where(Message.conversation_id == conv_id)
    total = (await db.execute(count_q)).scalar() or 0

    # Only regenerate at multiples of the interval, and only if > threshold
    if total < SUMMARY_REGEN_INTERVAL or total % SUMMARY_REGEN_INTERVAL != 0:
        return

    conv = await db.get(Conversation, conv_id)
    if not conv:
        return

    # Fetch ALL messages for summarization
    all_q = (
        select(Message)
        .where(Message.conversation_id == conv_id)
        .order_by(Message.created_at.asc())
    )
    all_msgs = list((await db.execute(all_q)).scalars().all())

    # Messages to summarize = everything except the last RECENT_MESSAGE_COUNT
    # (those will be included verbatim in context building)
    to_summarize = all_msgs[:-RECENT_MESSAGE_COUNT] if len(all_msgs) > RECENT_MESSAGE_COUNT else []

    if not to_summarize:
        return

    # Build the text to summarize
    prev_summary = conv.summary or ""
    conversation_text = "\n".join(
        f"{'User' if m.role == 'user' else 'Assistant'}: {m.content[:500]}"
        for m in to_summarize
    )

    prompt = (
        "You are a conversation summarizer. Summarize the following conversation history "
        "into a concise paragraph that captures: key questions asked, data/files discussed, "
        "important findings, and any ongoing analysis threads. Keep it under 200 words.\n\n"
    )
    if prev_summary:
        prompt += f"Previous summary:\n{prev_summary}\n\n"
    prompt += f"Messages to incorporate:\n{conversation_text}"

    try:
        llm = get_llm()
        response = await _call_llm_simple(llm, prompt, SUMMARY_MODEL_MAX_TOKENS)
        conv.summary = response.strip()
        conv.token_count = sum(m.token_count for m in all_msgs)
        await db.commit()
        chat_logger.info("summary_regenerated", conversation_id=conv_id, total_messages=total)
    except Exception as exc:
        chat_logger.warning("summary_regen_failed", conversation_id=conv_id, error=str(exc)[:200])
        await db.rollback()


# ── Auto title generation ──


async def maybe_generate_title(
    conv_id: str,
    db: AsyncSession,
) -> None:
    """
    Generate a descriptive title after the first user+assistant exchange.
    Uses a cheap LLM call. Only runs once per conversation.
    """
    conv = await db.get(Conversation, conv_id)
    if not conv or conv.title_generated:
        return

    # Fetch first two messages (user + assistant)
    q = (
        select(Message)
        .where(Message.conversation_id == conv_id)
        .order_by(Message.created_at.asc())
        .limit(2)
    )
    msgs = list((await db.execute(q)).scalars().all())
    if len(msgs) < 2:
        return

    user_text = msgs[0].content[:300]
    assistant_text = msgs[1].content[:300]

    prompt = (
        "Generate a short, descriptive title (4-6 words) for this conversation. "
        "Return ONLY the title, no quotes, no explanation.\n\n"
        f"User: {user_text}\n"
        f"Assistant: {assistant_text}"
    )

    try:
        llm = get_llm()
        title = await _call_llm_simple(llm, prompt, TITLE_MODEL_MAX_TOKENS)
        title = title.strip().strip('"').strip("'")[:200]
        if title:
            conv.title = title
        conv.title_generated = True
        await db.commit()
        chat_logger.info("title_generated", conversation_id=conv_id, title=title)
    except Exception as exc:
        chat_logger.warning("title_gen_failed", conversation_id=conv_id, error=str(exc)[:200])
        await db.rollback()


# ── Helpers ──

async def _call_llm_simple(llm, prompt: str, max_tokens: int) -> str:
    """Make a simple LLM call (no tools) for summarization/title tasks."""
    import asyncio
    from langchain_core.messages import HumanMessage as LCHumanMessage

    # Use a low-cost call with reduced token budget
    bound = llm.bind(max_completion_tokens=max_tokens, temperature=0.3)
    response = await asyncio.to_thread(bound.invoke, [LCHumanMessage(content=prompt)])
    return response.content if isinstance(response.content, str) else str(response.content)
