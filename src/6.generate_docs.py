#!/usr/bin/env python
# Step 6：根据推荐结果生成 Docs（精读区 / 速读区），并更新侧边栏。

import argparse
import html
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import fitz  # PyMuPDF
import requests
from llm import BltClient

SCRIPT_DIR = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
CONFIG_FILE = os.path.join(ROOT_DIR, "config.yaml")
TODAY_STR = datetime.now(timezone.utc).strftime("%Y%m%d")

# LLM 配置（使用 llm.py 内的 BLT 客户端）
BLT_API_KEY = os.getenv("BLT_API_KEY")
BLT_MODEL = os.getenv("BLT_SUMMARY_MODEL", "gemini-3-flash-preview")
LLM_CLIENT = None
if BLT_API_KEY:
    LLM_CLIENT = BltClient(api_key=BLT_API_KEY, model=BLT_MODEL)


def call_blt_text(
    client: BltClient,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: int,
) -> str:
    client.kwargs.update(
        {
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }
    )
    resp = client.chat(messages=messages)
    return (resp.get("content") or "").strip()


def log(message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}", flush=True)

def log_substep(code: str, name: str, phase: str) -> None:
    """
    用于前端解析的子步骤标记。
    格式： [SUBSTEP] 6.1 - xxx START/END
    """
    phase = str(phase or "").strip().upper()
    if phase not in ("START", "END"):
        phase = "INFO"
    log(f"[SUBSTEP] {code} - {name} {phase}")


def load_config() -> dict:
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        import yaml  # type: ignore
    except Exception:
        log("[WARN] 未安装 PyYAML，无法解析 config.yaml。")
        return {}
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception as e:
        log(f"[WARN] 读取 config.yaml 失败：{e}")
        return {}


def resolve_docs_dir() -> str:
    docs_dir = os.getenv("DOCS_DIR")
    config = load_config()
    paper_setting = (config or {}).get("arxiv_paper_setting") or {}
    crawler_setting = (config or {}).get("crawler") or {}
    cfg_docs = paper_setting.get("docs_dir") or crawler_setting.get("docs_dir")
    if not docs_dir and cfg_docs:
        if os.path.isabs(cfg_docs):
            docs_dir = cfg_docs
        else:
            docs_dir = os.path.join(ROOT_DIR, cfg_docs)
    if not docs_dir:
        docs_dir = os.path.join(ROOT_DIR, "docs")
    return docs_dir


def slugify(title: str) -> str:
    s = (title or "").strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    return s or "paper"


def extract_pdf_text(pdf_path: str) -> str:
    doc = fitz.open(pdf_path)
    texts = []
    try:
        for page in doc:
            texts.append(page.get_text("text"))
    finally:
        doc.close()
    return "\n\n".join(texts)


def fetch_paper_markdown_via_jina(pdf_url: str, max_retries: int = 3) -> str | None:
    if not pdf_url:
        return None
    base = "https://r.jina.ai/"
    full_url = base + pdf_url
    for attempt in range(1, max_retries + 1):
        try:
            log(f"[JINA] 第 {attempt} 次请求：{full_url}")
            resp = requests.get(full_url, timeout=60)
            if resp.status_code != 200:
                log(f"[JINA][WARN] 状态码 {resp.status_code}，响应前 100 字符：{(resp.text or '')[:100]}")
            else:
                text = (resp.text or "").strip()
                if text:
                    log("[JINA] 获取到结构化 Markdown 文本，将直接用作 .txt 内容。")
                    return text
        except Exception as e:
            log(f"[JINA][WARN] 请求失败（第 {attempt} 次）：{e}")
        time.sleep(2 * attempt)
    log("[JINA][ERROR] 多次请求失败，将回退到 PyMuPDF 抽取。")
    return None


def translate_title_and_abstract_to_zh(title: str, abstract: str) -> Tuple[str, str]:
    if LLM_CLIENT is None:
        return "", ""
    title = title.strip() if title else ""
    abstract = abstract.strip() if abstract else ""
    if not title and not abstract:
        return "", ""

    system_prompt = (
        "你是一名熟悉机器学习与自然科学论文的专业翻译，请将英文标题和摘要翻译为自然、准确的中文。"
        "保持学术风格，尽量保留专有名词，不要额外添加评论。"
    )
    user_parts: list[str] = []
    if title:
        user_parts.append(f"【标题 Title】\n{title}")
    if abstract:
        user_parts.append(f"【摘要 Abstract】\n{abstract}")
    user_text = "\n\n".join(user_parts)

    user_prompt = (
        "请将上面的英文标题和摘要分别翻译成中文，并严格使用下面的输出格式：\n"
        "【标题（中文）】\n<中文标题>\n\n"
        "【摘要（中文）】\n<中文摘要>\n"
        "不要输出任何其它说明文字。"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_text},
        {"role": "user", "content": user_prompt},
    ]
    try:
        content = call_blt_text(LLM_CLIENT, messages, temperature=0.2, max_tokens=1024)
    except Exception:
        return "", ""

    zh_title = ""
    zh_abstract = ""
    try:
        parts = content.split("【摘要（中文）】", 1)
        title_block = parts[0]
        abstract_block = parts[1] if len(parts) > 1 else ""
        t = title_block.split("【标题（中文）】", 1)
        if len(t) == 2:
            zh_title = t[1].strip()
        zh_abstract = abstract_block.strip()
    except Exception:
        return "", ""
    return zh_title, zh_abstract


def looks_truncated(text: str) -> bool:
    """
    经验性判断：模型输出是否像“被截断/未写完”。
    典型现象：以逗号/冒号/顿号/左括号等结尾。
    """
    s = (text or "").strip()
    if not s:
        return False
    if s.endswith(("，", ",", "：", ":", "；", ";", "、", "（", "(", "—", "-", "…")):
        return True
    return False


def quick_summary_is_valid(text: str) -> bool:
    """
    快速摘要的格式校验：
    - 必须包含 **问题** / **方法** / **结论** 三行
    - 每行必须以中文句号/问号/感叹号或英文标点结尾，避免半句
    """
    s = (text or "").strip()
    if not s:
        return False

    lines = [l.strip() for l in s.splitlines() if l.strip()]
    want = {"**问题**", "**方法**", "**结论**"}
    got = set()
    for l in lines:
        for k in want:
            if l.startswith(k):
                got.add(k)
    if got != want:
        return False

    def line_ok(prefix: str) -> bool:
        line = next((l for l in lines if l.startswith(prefix)), "")
        if not line:
            return False
        # 必须以句末标点结束，避免“在/中/的”等半句截断
        return line.endswith(("。", "！", "？", ".", "!", "?"))

    return line_ok("**问题**") and line_ok("**方法**") and line_ok("**结论**")


def quick_summary_invalid_reason(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return "empty"
    lines = [l.strip() for l in s.splitlines() if l.strip()]
    want = {"**问题**", "**方法**", "**结论**"}
    got = set()
    for l in lines:
        for k in want:
            if l.startswith(k):
                got.add(k)
    if got != want:
        missing = sorted(list(want - got))
        return f"missing_fields:{','.join(missing)}"

    def line_ok(prefix: str) -> bool:
        line = next((l for l in lines if l.startswith(prefix)), "")
        if not line:
            return False
        return line.endswith(("。", "！", "？", ".", "!", "?"))

    bad = []
    for k in ("**问题**", "**方法**", "**结论**"):
        if not line_ok(k):
            bad.append(k)
    if bad:
        return f"bad_punctuation:{','.join(bad)}"
    return "unknown"


def extract_section_tail(md_text: str, heading: str) -> str:
    """
    从 md 中提取某个自动生成段落（heading）后的尾部内容。
    返回不含 heading 的文本（strip 后）。
    """
    if not md_text:
        return ""
    key = f"## {heading}"
    idx = md_text.rfind(key)
    if idx == -1:
        return ""
    return md_text[idx + len(key) :].strip()


def strip_auto_sections(md_text: str) -> str:
    """
    发送给 LLM 的“论文 Markdown 元数据”只保留正文前半段，避免把旧的自动总结/速览再喂回模型。
    """
    if not md_text:
        return ""
    markers = [
        "\n\n---\n\n## 论文详细总结（自动生成）",
        "\n\n---\n\n## 速览摘要（自动生成）",
    ]
    cut_points = [md_text.find(m) for m in markers if md_text.find(m) != -1]
    if not cut_points:
        return md_text
    cut = min(cut_points)
    return md_text[:cut].rstrip()


def normalize_meta_tldr_line(md_text: str) -> Tuple[str, bool]:
    """
    兼容历史版本：TLDR 行曾被写成 '**TLDR**: xxx \\'。
    这里把 TLDR 行末尾的反斜杠去掉。
    """
    if not md_text:
        return md_text, False
    changed = False
    lines = md_text.splitlines()
    out: List[str] = []
    for line in lines:
        if line.startswith("**TLDR**"):
            new_line = line.rstrip()
            if new_line.endswith("\\"):
                new_line = new_line[:-1].rstrip()
            if new_line != line:
                changed = True
            out.append(new_line)
        else:
            out.append(line)
    return "\n".join(out), changed


def ensure_single_sentence_end(text: str) -> str:
    """
    给 TLDR/短句补一个句末标点（避免重复 '。。'）。
    """
    s = (text or "").strip()
    if not s:
        return s
    s = s.rstrip("。.!?！？")
    return s + "。"


def upsert_auto_block(md_path: str, heading: str, content: str) -> None:
    """
    将自动生成内容写入 md：
    - 若已存在同名 heading，则替换从该块开始到文件末尾
    - 否则追加到文件末尾
    """
    key = f"## {heading}"
    block = f"\n\n---\n\n{key}\n\n{content}".rstrip() + "\n"

    with open(md_path, "r", encoding="utf-8") as f:
        txt = f.read()

    idx = txt.rfind(key)
    if idx == -1:
        new_txt = txt.rstrip() + block
    else:
        start = txt.rfind("\n\n---\n\n", 0, idx)
        if start == -1:
            start = idx
        new_txt = txt[:start].rstrip() + block

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(new_txt)


def generate_deep_summary(md_file_path: str, txt_file_path: str, max_retries: int = 3) -> str | None:
    if LLM_CLIENT is None:
        log("[WARN] 未配置 BLT_API_KEY，跳过精读总结。")
        return None
    if not os.path.exists(md_file_path):
        return None

    with open(md_file_path, "r", encoding="utf-8") as f:
        paper_md_content = strip_auto_sections(f.read())

    paper_txt_content = ""
    if os.path.exists(txt_file_path):
        with open(txt_file_path, "r", encoding="utf-8") as f:
            paper_txt_content = f.read()

    system_prompt = (
        "你是一名资深学术论文分析助手，请使用中文、以 Markdown 形式，"
        "对给定论文做结构化、深入、客观的总结。"
    )
    user_prompt = (
        "请基于下面提供的论文内容，生成一段详细的中文总结，要求按照如下要点依次展开：\n"
        "1. 论文的核心问题与整体含义（研究动机和背景）。\n"
        "2. 论文提出的方法论：核心思想、关键技术细节、公式或算法流程（用文字说明即可）。\n"
        "3. 实验设计：使用了哪些数据集 / 场景，它的 benchmark 是什么，对比了哪些方法。\n"
        "4. 资源与算力：如果文中有提到，请总结使用了多少算力（GPU 型号、数量、训练时长等）。若未明确说明，也请指出这一点。\n"
        "5. 实验数量与充分性：大概做了多少组实验（如不同数据集、消融实验等），这些实验是否充分、是否客观、公平。\n"
        "6. 论文的主要结论与发现。\n"
        "7. 优点：方法或实验设计上有哪些亮点。\n"
        "8. 不足与局限：包括实验覆盖、偏差风险、应用限制等。\n\n"
        "请用分层标题和项目符号（Markdown 格式）组织上述内容，语言尽量简洁但信息要尽量完整。\n"
        "要求：最后单独输出一行“（完）”作为结束标记。"
    )

    messages = [{"role": "system", "content": system_prompt}]
    if paper_txt_content:
        messages.append({"role": "user", "content": f"### 论文 PDF 提取文本 ###\n{paper_txt_content}"})
    messages.append({"role": "user", "content": f"### 论文 Markdown 元数据 ###\n{paper_md_content}"})
    messages.append({"role": "user", "content": user_prompt})

    last = ""
    for attempt in range(1, max_retries + 1):
        try:
            summary = call_blt_text(LLM_CLIENT, messages, temperature=0.3, max_tokens=4096)
            summary = (summary or "").strip()
            if not summary:
                continue
            last = summary
            if os.getenv("DPR_DEBUG_STEP6") == "1":
                log(f"[DEBUG][STEP6] deep_summary attempt={attempt} len={len(summary)} tail={summary[-20:]!r}")
            if "（完）" in summary and not looks_truncated(summary):
                return summary
            # 续写一次：避免输出被截断
            cont_messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "你上一次的总结可能被截断了，请从中断处继续补全，不要重复已输出内容。"},
                {"role": "user", "content": f"上一次输出如下：\n\n{summary}\n\n请继续补全，最后以一行“（完）”结束。"},
            ]
            cont = call_blt_text(LLM_CLIENT, cont_messages, temperature=0.3, max_tokens=2048)
            cont = (cont or "").strip()
            merged = f"{summary}\n\n{cont}".strip()
            if os.getenv("DPR_DEBUG_STEP6") == "1":
                log(f"[DEBUG][STEP6] deep_summary_cont attempt={attempt} len={len(cont)} merged_tail={merged[-20:]!r}")
            if "（完）" in merged and not looks_truncated(merged):
                return merged
        except Exception as e:
            log(f"[WARN] 精读总结失败（第 {attempt} 次）：{e}")
            time.sleep(2 * attempt)
    return last or None


def generate_quick_summary(title: str, abstract: str, max_retries: int = 3) -> str | None:
    if LLM_CLIENT is None:
        if abstract:
            return abstract.strip()[:200]
        return None

    system_prompt = "你是论文速读助手，请用中文输出一个简短速览摘要。"
    user_prompt = f"标题：{title}\n摘要：{abstract}"
    format_prompt = (
        "请严格按下面格式输出 3 行（每行必须以句号“。”结束）：\n"
        "**问题**：<一句话>\n"
        "**方法**：<一句话>\n"
        "**结论**：<一句话>\n\n"
        "不要输出其它说明文字。"
    )
    messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}, {"role": "user", "content": format_prompt}]

    for attempt in range(1, max_retries + 1):
        try:
            summary = call_blt_text(LLM_CLIENT, messages, temperature=0.2, max_tokens=512)
            summary = (summary or "").strip()
            if not summary:
                continue
            if os.getenv("DPR_DEBUG_STEP6") == "1":
                log(f"[DEBUG][STEP6] quick_summary attempt={attempt} len={len(summary)} valid={quick_summary_is_valid(summary)} reason={quick_summary_invalid_reason(summary)}")
            return summary
        except Exception as e:
            log(f"[WARN] 速读总结失败（第 {attempt} 次）：{e}")
            time.sleep(2 * attempt)
    return None


def build_tags_html(section: str, llm_tags: List[str]) -> str:
    tags_html: List[str] = []
    if section == "deep":
        tags_html.append('<span class="tag-label tag-blue">精读区</span>')
    else:
        tags_html.append('<span class="tag-label tag-green">速读区</span>')
    seen = set()
    for tag in llm_tags:
        raw = str(tag).strip()
        if not raw:
            continue
        kind, label = split_sidebar_tag(raw)
        label = (label or "").strip()
        if not label:
            continue
        if label in seen:
            continue
        seen.add(label)

        # 使用“面板”里的配色语义：
        # - keyword: 绿色
        # - query:   蓝色
        # - paper:   紫色（预留）
        css = {
            "keyword": "tag-green",
            "query": "tag-blue",
            "paper": "tag-pink",
        }.get(kind, "tag-pink")
        tags_html.append(
            f'<span class="tag-label {css}">{html.escape(label)}</span>'
        )
    return " ".join(tags_html)


def format_date_str(date_str: str) -> str:
    if len(date_str) == 8:
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    return date_str


def prepare_paper_paths(docs_dir: str, date_str: str, title: str, arxiv_id: str) -> Tuple[str, str, str]:
    ym = date_str[:6]
    day = date_str[6:]
    slug = slugify(title)
    basename = f"{arxiv_id}-{slug}" if arxiv_id else slug
    target_dir = os.path.join(docs_dir, ym, day)
    md_path = os.path.join(target_dir, f"{basename}.md")
    txt_path = os.path.join(target_dir, f"{basename}.txt")
    paper_id = f"{ym}/{day}/{basename}"
    return md_path, txt_path, paper_id


def normalize_sidebar_tag(tag: str) -> str:
    text = (tag or "").strip()
    if not text:
        return ""
    for prefix in ("keyword:", "query:", "paper:", "ref:", "cite:"):
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text


def split_sidebar_tag(tag: str) -> Tuple[str, str]:
    """
    将 tag 解析为 (kind, label)：
    - keyword:xxx -> ("keyword", "xxx")
    - query:xxx   -> ("query", "xxx")
    - paper/ref/cite:xxx -> ("paper", "xxx")  # 预留：论文引用/跟踪标签
    - 其它 -> ("other", 原文本)
    """
    raw = (tag or "").strip()
    if not raw:
        return ("other", "")
    for prefix, kind in (
        ("keyword:", "keyword"),
        ("query:", "query"),
        ("paper:", "paper"),
        ("ref:", "paper"),
        ("cite:", "paper"),
    ):
        if raw.startswith(prefix):
            return (kind, raw[len(prefix) :].strip())
    return ("other", raw)


def extract_sidebar_tags(paper: Dict[str, Any], max_tags: int = 6) -> List[Tuple[str, str]]:
    """
    侧边栏展示的标签：
    - 优先 llm_tags（更贴近最终推荐意图），并追加 tags（粗召回标签）作为补充
    - 去重 + 限制数量，避免侧边栏过长
    """
    raw: List[str] = []
    if isinstance(paper.get("llm_tags"), list):
        raw.extend([str(t) for t in (paper.get("llm_tags") or [])])
    if isinstance(paper.get("tags"), list):
        raw.extend([str(t) for t in (paper.get("tags") or [])])

    seen_labels = set()
    kw: List[Tuple[str, str]] = []
    q: List[Tuple[str, str]] = []
    paper_tags: List[Tuple[str, str]] = []
    other: List[Tuple[str, str]] = []

    for t in raw:
        kind, label = split_sidebar_tag(t)
        label = (label or "").strip()
        if not label:
            continue
        if label in seen_labels:
            continue
        seen_labels.add(label)
        if kind == "keyword":
            kw.append((kind, label))
        elif kind == "query":
            q.append((kind, label))
        elif kind == "paper":
            paper_tags.append((kind, label))
        else:
            other.append((kind, label))

        if max_tags > 0 and len(seen_labels) >= max_tags:
            break

    # 展示顺序：关键词 -> 智能订阅(query) -> 论文引用(paper) -> 其它
    return kw + q + paper_tags + other


def ensure_text_content(pdf_url: str, txt_path: str) -> str:
    if os.path.exists(txt_path):
        with open(txt_path, "r", encoding="utf-8") as f:
            return f.read()
    text_content = fetch_paper_markdown_via_jina(pdf_url)
    if text_content is None and pdf_url:
        resp = requests.get(pdf_url, timeout=60)
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp_pdf:
            tmp_pdf.write(resp.content)
            tmp_pdf.flush()
            text_content = extract_pdf_text(tmp_pdf.name)
    os.makedirs(os.path.dirname(txt_path), exist_ok=True)
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text_content or "")
    return text_content or ""


def build_markdown_content(
    paper: Dict[str, Any],
    section: str,
    zh_title: str,
    zh_abstract: str,
    tags_html: str,
) -> str:
    def meta_line(label: str, value: str, add_slash: bool = True) -> str:
        v = (value or "").strip()
        if not v:
            return ""
        if add_slash:
            return f"**{label}**: {v} \\"
        return f"**{label}**: {v}"

    title = (paper.get("title") or "").strip()
    authors = paper.get("authors") or []
    published = str(paper.get("published") or "").strip()
    if published:
        published = published[:10]
    pdf_url = str(paper.get("link") or paper.get("pdf_url") or "").strip()
    score = paper.get("llm_score")
    evidence = (
        paper.get("llm_evidence_cn")
        or paper.get("llm_evidence")
        or paper.get("llm_evidence_en")
        or ""
    ).strip()
    tldr = (
        paper.get("llm_tldr_cn")
        or paper.get("llm_tldr")
        or paper.get("llm_tldr_en")
        or ""
    ).strip()
    abstract_en = (paper.get("abstract") or "").strip()
    if not abstract_en:
        abstract_en = "arXiv did not provide an abstract for this paper."

    lines = [
        f"# {title}",
    ]
    if zh_title:
        lines.append(f"# {zh_title}")
    lines.append("")
    lines.append(meta_line("Authors", ", ".join(authors) if authors else "Unknown"))
    lines.append(meta_line("Date", published or "Unknown"))
    if pdf_url:
        lines.append(meta_line("PDF", pdf_url))
    if tags_html:
        lines.append(meta_line("Tags", tags_html))
    if score is not None:
        lines.append(meta_line("Score", str(score)))
    if evidence:
        lines.append(meta_line("Evidence", evidence))
    if tldr:
        # TLDR 行不需要尾部反斜杠
        lines.append(meta_line("TLDR", tldr, add_slash=False))
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Abstract")
    lines.append(abstract_en)
    if zh_abstract:
        lines.append("")
        lines.append("## 摘要")
        lines.append(zh_abstract)

    return "\n".join(lines)


def process_paper(
    paper: Dict[str, Any],
    section: str,
    date_str: str,
    docs_dir: str,
) -> Tuple[str, str]:
    title = (paper.get("title") or "").strip()
    arxiv_id = str(paper.get("id") or paper.get("paper_id") or "").strip()
    md_path, txt_path, paper_id = prepare_paper_paths(docs_dir, date_str, title, arxiv_id)

    if os.path.exists(md_path):
        # 修复模式：若自动总结/速览存在“被截断”的迹象，则仅重生成该段落，不改动前面正文
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                existing = f.read()
        except Exception:
            existing = ""

        # 修复历史格式：TLDR 行末尾不应带反斜杠
        fixed, changed = normalize_meta_tldr_line(existing)
        if changed:
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(fixed + ("\n" if not fixed.endswith("\n") else ""))
            existing = fixed
            if os.getenv("DPR_DEBUG_STEP6") == "1":
                log(f"[DEBUG][STEP6] fixed TLDR trailing slash: {os.path.basename(md_path)}")

        if section == "deep":
            tail = extract_section_tail(existing, "论文详细总结（自动生成）")
            if tail and not looks_truncated(tail):
                return paper_id, title

            pdf_url = str(paper.get("link") or paper.get("pdf_url") or "").strip()
            ensure_text_content(pdf_url, txt_path)
            summary = generate_deep_summary(md_path, txt_path)
            if summary:
                upsert_auto_block(md_path, "论文详细总结（自动生成）", summary)
            return paper_id, title
        else:
            tail = extract_section_tail(existing, "速览摘要（自动生成）")
            # 若已经有速览摘要且不是 TLDR 回退，则保持不变
            if tail and not tail.lstrip().startswith("**TLDR**"):
                return paper_id, title
            if os.getenv("DPR_DEBUG_STEP6") == "1" and tail:
                log(f"[DEBUG][STEP6] quick_summary needs fix: {os.path.basename(md_path)} reason={quick_summary_invalid_reason(tail)} tail={tail[-40:]!r}")

            abstract_en = (paper.get("abstract") or "").strip()
            summary = generate_quick_summary(title, abstract_en)
            if not summary:
                tldr = (paper.get("llm_tldr_cn") or paper.get("llm_tldr") or "").strip()
                if tldr:
                    summary = f"**TLDR**：{ensure_single_sentence_end(tldr)}"
            if summary:
                upsert_auto_block(md_path, "速览摘要（自动生成）", summary)
            return paper_id, title

    pdf_url = str(paper.get("link") or paper.get("pdf_url") or "").strip()
    ensure_text_content(pdf_url, txt_path)

    abstract_en = (paper.get("abstract") or "").strip()
    zh_title, zh_abstract = translate_title_and_abstract_to_zh(title, abstract_en)
    tags_html = build_tags_html(section, paper.get("llm_tags") or [])
    content = build_markdown_content(paper, section, zh_title, zh_abstract, tags_html)

    os.makedirs(os.path.dirname(md_path), exist_ok=True)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(content)

    if section == "deep":
        summary = generate_deep_summary(md_path, txt_path)
        if summary:
            upsert_auto_block(md_path, "论文详细总结（自动生成）", summary)
    else:
        summary = generate_quick_summary(title, abstract_en)
        if not summary:
            tldr = (paper.get("llm_tldr_cn") or paper.get("llm_tldr") or "").strip()
            if tldr:
                summary = f"**TLDR**：{ensure_single_sentence_end(tldr)}"
        if summary:
            upsert_auto_block(md_path, "速览摘要（自动生成）", summary)

    return paper_id, title


def update_sidebar(
    sidebar_path: str,
    date_str: str,
    deep_entries: List[Tuple[str, str, List[Tuple[str, str]]]],
    quick_entries: List[Tuple[str, str, List[Tuple[str, str]]]],
) -> None:
    date_label = format_date_str(date_str)
    day_heading = f"  * {date_label}\n"

    lines: List[str] = []
    if os.path.exists(sidebar_path):
        with open(sidebar_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    daily_idx = -1
    for i, line in enumerate(lines):
        if line.strip().startswith("* Daily Papers"):
            daily_idx = i
            break
    if daily_idx == -1:
        if not any("[首页]" in line for line in lines):
            lines.append("* [首页](/)\n")
        lines.append("* Daily Papers\n")
        daily_idx = len(lines) - 1

    day_idx = -1
    for i in range(daily_idx + 1, len(lines)):
        line = lines[i]
        if line.startswith("* "):
            break
        if line == day_heading:
            day_idx = i
            break

    if day_idx != -1:
        end = day_idx + 1
        while end < len(lines):
            if lines[end].startswith("  * ") and not lines[end].startswith("    * "):
                break
            end += 1
        del lines[day_idx:end]

    block: List[str] = [day_heading]
    block.append("    * 精读区\n")
    for paper_id, title, tags in deep_entries:
        safe_title = html.escape((title or "").strip() or paper_id)
        href = f"#/{paper_id}"
        tag_html = " ".join(
            f'<span class="dpr-sidebar-tag dpr-sidebar-tag-{html.escape(kind)}">{html.escape(label)}</span>'
            for kind, label in (tags or [])
        )
        tags_block = f'<div class="dpr-sidebar-tags">{tag_html}</div>' if tag_html else ""
        block.append(
            "      * "
            f'<a class="dpr-sidebar-item-link" href="{href}"><div class="dpr-sidebar-title">{safe_title}</div>'
            f"{tags_block}</a>\n"
        )
    block.append("    * 速读区\n")
    for paper_id, title, tags in quick_entries:
        safe_title = html.escape((title or "").strip() or paper_id)
        href = f"#/{paper_id}"
        tag_html = " ".join(
            f'<span class="dpr-sidebar-tag dpr-sidebar-tag-{html.escape(kind)}">{html.escape(label)}</span>'
            for kind, label in (tags or [])
        )
        tags_block = f'<div class="dpr-sidebar-tags">{tag_html}</div>' if tag_html else ""
        block.append(
            "      * "
            f'<a class="dpr-sidebar-item-link" href="{href}"><div class="dpr-sidebar-title">{safe_title}</div>'
            f"{tags_block}</a>\n"
        )

    insert_idx = daily_idx + 1
    lines[insert_idx:insert_idx] = block

    with open(sidebar_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 6: generate docs for deep/quick sections.")
    parser.add_argument("--date", type=str, default=TODAY_STR, help="date string YYYYMMDD.")
    parser.add_argument("--mode", type=str, default=None, help="mode for recommend file.")
    parser.add_argument("--docs-dir", type=str, default=None, help="override docs dir.")
    args = parser.parse_args()

    date_str = args.date or TODAY_STR
    mode = args.mode
    if not mode:
        config = load_config()
        setting = (config or {}).get("arxiv_paper_setting") or {}
        mode = str(setting.get("mode") or "standard").strip()
    if "," in mode:
        mode = mode.split(",", 1)[0].strip()

    docs_dir = args.docs_dir or resolve_docs_dir()
    archive_dir = os.path.join(ROOT_DIR, "archive", date_str, "recommend")
    recommend_path = os.path.join(archive_dir, f"arxiv_papers_{date_str}.{mode}.json")
    if not os.path.exists(recommend_path):
        log(f"[WARN] recommend 文件不存在（今天可能没有新论文）：{recommend_path}，将跳过 Step 6。")
        return

    log_substep("6.1", "读取 recommend 结果", "START")
    payload = {}
    try:
        with open(recommend_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    finally:
        log_substep("6.1", "读取 recommend 结果", "END")
    deep_list = payload.get("deep_dive") or []
    quick_list = payload.get("quick_skim") or []
    if not deep_list and not quick_list:
        log("[INFO] 推荐列表为空，将跳过生成 docs 与更新侧边栏。")
        return

    deep_entries: List[Tuple[str, str, List[Tuple[str, str]]]] = []
    quick_entries: List[Tuple[str, str, List[Tuple[str, str]]]] = []

    log_substep("6.2", "生成精读区文章", "START")
    for paper in deep_list:
        pid, title = process_paper(paper, "deep", date_str, docs_dir)
        deep_entries.append((pid, title, extract_sidebar_tags(paper)))
    log_substep("6.2", "生成精读区文章", "END")

    log_substep("6.3", "生成速读区文章", "START")
    for paper in quick_list:
        pid, title = process_paper(paper, "quick", date_str, docs_dir)
        quick_entries.append((pid, title, extract_sidebar_tags(paper)))
    log_substep("6.3", "生成速读区文章", "END")

    sidebar_path = os.path.join(docs_dir, "_sidebar.md")
    log_substep("6.4", "更新侧边栏", "START")
    update_sidebar(sidebar_path, date_str, deep_entries, quick_entries)
    log_substep("6.4", "更新侧边栏", "END")
    log(f"[OK] docs updated: {docs_dir}")


if __name__ == "__main__":
    main()
