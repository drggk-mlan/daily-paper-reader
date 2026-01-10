#!/usr/bin/env python
# Step 6：根据推荐结果生成 Docs（精读区 / 速读区），并更新侧边栏。

import argparse
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


def generate_deep_summary(md_file_path: str, txt_file_path: str, max_retries: int = 3) -> str | None:
    if LLM_CLIENT is None:
        log("[WARN] 未配置 BLT_API_KEY，跳过精读总结。")
        return None
    if not os.path.exists(md_file_path):
        return None

    with open(md_file_path, "r", encoding="utf-8") as f:
        paper_md_content = f.read()

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
        "请用分层标题和项目符号（Markdown 格式）组织上述内容，语言尽量简洁但信息要尽量完整。"
    )

    messages = [{"role": "system", "content": system_prompt}]
    if paper_txt_content:
        messages.append({"role": "user", "content": f"### 论文 PDF 提取文本 ###\n{paper_txt_content}"})
    messages.append({"role": "user", "content": f"### 论文 Markdown 元数据 ###\n{paper_md_content}"})
    messages.append({"role": "user", "content": user_prompt})

    for attempt in range(1, max_retries + 1):
        try:
            summary = call_blt_text(LLM_CLIENT, messages, temperature=0.3, max_tokens=2048)
            if summary.strip():
                return summary.strip()
        except Exception as e:
            log(f"[WARN] 精读总结失败（第 {attempt} 次）：{e}")
            time.sleep(2 * attempt)
    return None


def generate_quick_summary(title: str, abstract: str, max_retries: int = 3) -> str | None:
    if LLM_CLIENT is None:
        if abstract:
            return abstract.strip()[:200]
        return None

    system_prompt = (
        "你是论文速读助手，请用中文输出一个简短速览摘要。"
        "必须包含：问题、方法、结论（或贡献）。"
        "总长度不超过 120 字。"
    )
    user_prompt = f"标题：{title}\n摘要：{abstract}"
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    for attempt in range(1, max_retries + 1):
        try:
            summary = call_blt_text(LLM_CLIENT, messages, temperature=0.2, max_tokens=512)
            if summary.strip():
                return summary.strip()
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
    for tag in llm_tags:
        text = str(tag).strip()
        if not text:
            continue
        tags_html.append(f'<span class="tag-label tag-pink">{text}</span>')
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
    title = (paper.get("title") or "").strip()
    authors = paper.get("authors") or []
    published = str(paper.get("published") or "").strip()
    if published:
        published = published[:10]
    pdf_url = str(paper.get("link") or paper.get("pdf_url") or "").strip()
    score = paper.get("llm_score")
    evidence = (paper.get("llm_evidence") or "").strip()
    abstract_en = (paper.get("abstract") or "").strip()
    if not abstract_en:
        abstract_en = "arXiv did not provide an abstract for this paper."

    lines = [
        f"# {title}",
    ]
    if zh_title:
        lines.append(f"# {zh_title}")
    lines.append("")
    lines.append(f"**Authors**: {', '.join(authors) if authors else 'Unknown'}")
    lines.append(f"**Date**: {published or 'Unknown'}")
    if pdf_url:
        lines.append(f"**PDF**: {pdf_url}")
    if tags_html:
        lines.append(f"**Tags**: {tags_html}")
    if score is not None:
        lines.append(f"**Score**: {score}")
    if evidence:
        lines.append(f"**Evidence**: {evidence}")
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
            with open(md_path, "a", encoding="utf-8") as f:
                f.write("\n\n---\n\n## 论文详细总结（自动生成）\n\n")
                f.write(summary)
    else:
        summary = generate_quick_summary(title, abstract_en)
        if summary:
            with open(md_path, "a", encoding="utf-8") as f:
                f.write("\n\n---\n\n## 速览摘要（自动生成）\n\n")
                f.write(summary)

    return paper_id, title


def update_sidebar(sidebar_path: str, date_str: str, deep_entries: List[Tuple[str, str]], quick_entries: List[Tuple[str, str]]) -> None:
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
    for paper_id, title in deep_entries:
        block.append(f"      * [{title}]({paper_id})\n")
    block.append("    * 速读区\n")
    for paper_id, title in quick_entries:
        block.append(f"      * [{title}]({paper_id})\n")

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
        raise FileNotFoundError(f"missing recommend file: {recommend_path}")

    payload = {}
    with open(recommend_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    deep_list = payload.get("deep_dive") or []
    quick_list = payload.get("quick_skim") or []

    deep_entries: List[Tuple[str, str]] = []
    quick_entries: List[Tuple[str, str]] = []

    for paper in deep_list:
        pid, title = process_paper(paper, "deep", date_str, docs_dir)
        deep_entries.append((pid, title))

    for paper in quick_list:
        pid, title = process_paper(paper, "quick", date_str, docs_dir)
        quick_entries.append((pid, title))

    sidebar_path = os.path.join(docs_dir, "_sidebar.md")
    update_sidebar(sidebar_path, date_str, deep_entries, quick_entries)
    log(f"[OK] docs updated: {docs_dir}")


if __name__ == "__main__":
    main()
