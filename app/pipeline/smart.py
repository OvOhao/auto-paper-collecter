"""LLM-assisted fetching helpers.

Two jobs the raw keyword can't do well on its own:
  1. expand_queries  — turn one user keyword into several effective search strings
     (synonyms / full forms / common spellings), so e.g. "C2Rust" also hits
     "C-to-Rust translation". Fixes poor recall & stale results.
  2. filter_relevant — drop papers that are off-domain (not computer science) or
     not actually about the keyword. Fixes cross-domain junk (medical, etc.).

Both fail OPEN: if the AI gateway is unavailable they degrade to "keep the raw
behaviour" instead of throwing or wiping the feed.
"""
import json
import re
import asyncio
from ..services.ai import chat

_ARR = re.compile(r"\[.*\]", re.S)

EXPAND_SYS = (
    "你是计算机科学文献检索助手。用户给出一个研究关键词。"
    "请做“联想式扩展”：生成 5-7 个与之高度相关的英文学术检索式。"
    "不仅包括同义词、全称与常见写法，还要包括紧邻的子方向、关键技术、方法与典型应用，"
    "以便把该主题最新、最相关的论文尽量召回。"
    "所有检索式都必须仍属于计算机科学领域、且与原关键词强相关，避免过度泛化跑题。"
    "严格只输出 JSON 字符串数组，例如对“C2Rust”可输出 "
    '["C2Rust","C-to-Rust translation","automatic C to Rust transpilation",'
    '"memory-safe systems programming in Rust","translating legacy C code to Rust",'
    '"unsafe code elimination Rust","LLM-based code transpilation"]，不要任何其它文字。'
)

FILTER_SYS = (
    "你是计算机科学文献筛选助手。给定一个研究关键词和若干论文（标题 + 来源/期刊）。"
    "逐篇判断是否同时满足：①属于计算机科学/计算机工程领域；且 ②与该关键词主题直接相关。"
    "严格只输出 JSON 数组，每项形如 {\"i\":序号,\"keep\":true 或 false}，不要任何其它文字。"
)


# Cache expansions for the process lifetime — the same keyword/domain expands to
# the same queries every refresh, so there's no need to re-call the LLM each time.
_expand_cache = {}


async def expand_queries(keyword: str, domain: str = "") -> list:
    """Return the original keyword plus LLM associative queries (cached)."""
    kw = (keyword or "").strip()
    if not kw:
        return []
    ck = (kw.lower(), (domain or "").lower())
    if ck in _expand_cache:
        return _expand_cache[ck]
    user = f"关键词：{kw}" + (f"\n领域：{domain}" if domain else "")
    raw = await chat(
        [{"role": "system", "content": EXPAND_SYS}, {"role": "user", "content": user}],
        temperature=0.2, max_tokens=300,
    )
    queries = [kw]
    if raw:
        m = _ARR.search(raw)
        if m:
            try:
                for q in json.loads(m.group(0)):
                    q = str(q).strip()
                    if q and q.lower() not in [x.lower() for x in queries]:
                        queries.append(q)
            except Exception:
                pass
    queries = queries[:6]
    # only cache a genuinely expanded result, so a one-off AI failure that
    # returns just [kw] doesn't get stuck in the cache.
    if len(queries) > 1:
        _expand_cache[ck] = queries
    return queries


async def filter_relevant(items: list, keyword: str, domain: str = "",
                          batch_size: int = 25, negatives: list = None) -> list:
    """Keep only items the LLM judges to be CS-domain AND on-topic for `keyword`.

    Judged in small concurrent BATCHES — one big call would run past the
    gateway's ~100s Cloudflare limit and 524, which (fail-open) would silently
    let every off-topic paper through. Per-item default is keep=True, so a
    failed batch degrades to 'keep' rather than dropping good papers.

    `negatives`: titles the user marked 👎 — passed as negative examples so the
    filter learns to drop similar papers over time."""
    if not items:
        return items
    keep = [True] * len(items)
    neg_block = ""
    if negatives:
        neg_block = ("\n\n用户此前明确标记“不感兴趣”的论文（请据此排除主题/风格类似的）：\n"
                     + "\n".join(f"- {t}" for t in negatives[:15]))

    async def _filter_batch(start, chunk):
        listing = "\n".join(
            f'{j}. {it.title}　[{it.source}/{it.venue or "?"}]' for j, it in enumerate(chunk))
        user = (f"关键词：{keyword}" + (f"（领域：{domain}）" if domain else "")
                + neg_block + "\n\n论文列表：\n" + listing)
        raw = await chat(
            [{"role": "system", "content": FILTER_SYS}, {"role": "user", "content": user}],
            temperature=0.0, max_tokens=1200, timeout=90,
        )
        if not raw:
            return
        m = _ARR.search(raw)
        if not m:
            return
        try:
            for o in json.loads(m.group(0)):
                j = int(o["i"])
                if 0 <= j < len(chunk):
                    keep[start + j] = bool(o.get("keep"))
        except Exception:
            pass

    await asyncio.gather(*[
        _filter_batch(start, items[start:start + batch_size])
        for start in range(0, len(items), batch_size)
    ])
    kept = [it for i, it in enumerate(items) if keep[i]]
    return kept or items  # if everything got rejected, fall back to raw
