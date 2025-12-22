#!/usr/bin/env python
import datetime
import os
import re
import time
import sqlite3
import tempfile

import arxiv
import fitz  # PyMuPDF
import requests
import openai

# 配置
DOCS_DIR = os.path.expanduser("~/workplace/daily-paper-reader/docs")
TODAY = datetime.date.today().strftime("%Y-%m-%d")

# 与后端共用的数据库文件路径（如果存在，则可从中读取订阅关键词）
DB_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "app",
    "chat.db",
)

# 大模型配置（与 app/main.py 保持一致）
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "glm-4.6")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4")
LLM_CLIENT = None
if LLM_API_KEY:
    LLM_CLIENT = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)


def get_keywords() -> list[str]:
    """
    优先从数据库 subscriptions_keywords 中读取订阅关键词；
    如果数据库或表不存在，或者列表为空，则回退到环境变量 ARXIV_KEYWORDS；
    如果仍然为空，则使用一个默认示例，方便本地调试。
    """
    # 1. 先尝试从数据库读取订阅关键词
    try:
        if os.path.exists(DB_FILE):
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.execute(
                    "SELECT keyword FROM subscriptions_keywords ORDER BY id ASC"
                )
                rows = cursor.fetchall()
                keywords = [r[0].strip() for r in rows if r[0] and r[0].strip()]
                if keywords:
                    return keywords
    except Exception as e:
        print(f"[WARN] 从数据库读取订阅关键词失败，将回退到环境变量：{e}")

    # 2. 回退到环境变量
    raw = os.getenv("ARXIV_KEYWORDS", "")
    if raw.strip():
        return [k.strip() for k in raw.split(",") if k.strip()]

    # 3. 最后使用一个默认示例
    return ["Symbolic Regression"]


def build_arxiv_query(keywords: list[str]) -> str:
    """
    构建 arxiv 包的搜索 query。
    保持与之前一致的语义：
    - 每个关键词当作一个整体，用 all:"xxx" 来搜索
    - 关键词之间使用 OR 连接，实现“或”的逻辑
    """
    parts = []
    for kw in keywords:
        parts.append(f'all:"{kw}"')
    return " OR ".join(parts)


def search_arxiv_today(keywords: list[str], max_results: int = 50) -> list[dict]:
    """
    使用 arxiv 包调用 arXiv 搜索接口，获取“最近 N 天（按 UTC）发布”的论文。

    返回列表中的每个元素形如：
    {
        "title": str,
        "authors": [str, ...],
        "pdf_url": str,
        "published_date": "YYYY-MM-DD",
        "arxiv_id": str,
    }
    """
    if not keywords:
        return []

    # 最近 N 天的窗口（按 UTC），默认 2 天，避免本地时区与 arXiv UTC 不一致导致“今天”查不到结果
    days_window = int(os.getenv("ARXIV_DAYS_WINDOW", "10"))
    if days_window < 1:
        days_window = 1
    utc_today = datetime.datetime.utcnow().date()
    cutoff_date = utc_today - datetime.timedelta(days=days_window - 1)

    query = build_arxiv_query(keywords)
    search = arxiv.Search(
        query=query,
        max_results=max_results,
        sort_by=arxiv.SortCriterion.SubmittedDate,
        sort_order=arxiv.SortOrder.Descending,
    )
    client = arxiv.Client()

    results: list[dict] = []
    for result in client.results(search):
        # published 是 datetime
        published_dt = result.published
        if not isinstance(published_dt, datetime.datetime):
            continue
        pub_date = published_dt.date()
        published_date = pub_date.strftime("%Y-%m-%d")

        # 只保留最近 N 天（UTC）的结果
        if pub_date < cutoff_date:
            continue

        title = (result.title or "").strip() or "Untitled"
        summary = (getattr(result, "summary", "") or "").strip()
        authors = [a.name for a in result.authors] if result.authors else []
        pdf_url = result.pdf_url
        arxiv_id = result.get_short_id()

        if not pdf_url:
            continue

        results.append(
            {
                "title": title,
                "summary": summary,
                "authors": authors,
                "pdf_url": pdf_url,
                "published_date": published_date,
                "arxiv_id": arxiv_id,
            }
        )

    return results


def slugify(title: str) -> str:
    """
    将论文标题转换为适合文件名和 URL 的 slug。
    规则：小写、空白转为连字符、移除非字母数字和连字符。
    """
    s = title.strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^a-z0-9\-]+", "", s)
    return s or "paper"


def extract_pdf_text(pdf_path: str) -> str:
    """
    使用 PyMuPDF 从 PDF 中抽取纯文本，并按页面顺序拼接。
    """
    doc = fitz.open(pdf_path)
    texts = []
    try:
        for page in doc:
            # 使用默认布局提取文本，兼顾可读性与鲁棒性
            texts.append(page.get_text("text"))
    finally:
        doc.close()
    return "\n\n".join(texts)


def generate_paper_summary(paper_id: str, md_file_path: str, txt_file_path: str, max_retries: int = 3) -> str | None:
    """
    调用大模型为指定论文生成一段详细总结：
    - 总结论文整体含义
    - 详细说明方法论
    - 说明实验设置、benchmark、算力、实验数量与充分性
    - 分析结论、优点与不足

    最多重试 max_retries 次，失败返回 None。
    """
    if LLM_CLIENT is None:
        print("[WARN] 未配置 LLM_API_KEY，跳过自动总结生成。")
        return None

    if not os.path.exists(md_file_path):
        print(f"[WARN] Markdown 文件不存在，无法生成总结：{md_file_path}")
        return None

    # 读取 Markdown 与文本内容（为避免上下文过长，这里截断到一定长度）
    with open(md_file_path, "r", encoding="utf-8") as f:
        paper_md_content = f.read()

    paper_txt_content = ""
    if os.path.exists(txt_file_path):
        with open(txt_file_path, "r", encoding="utf-8") as f:
            paper_txt_content = f.read()

    # 简单截断，防止上下文过长：优先保留前面的内容
    max_ctx_chars = int(os.getenv("LLM_MAX_CTX_CHARS", "20000"))
    if len(paper_txt_content) > max_ctx_chars:
        paper_txt_content = paper_txt_content[:max_ctx_chars]

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

    messages = [
        {"role": "system", "content": system_prompt},
    ]

    if paper_txt_content:
        messages.append(
            {
                "role": "user",
                "content": f"### 论文 PDF 提取文本（截断后） ###\n{paper_txt_content}",
            }
        )

    messages.append(
        {
            "role": "user",
            "content": f"### 论文 Markdown 元数据 ###\n{paper_md_content}",
        }
    )

    messages.append({"role": "user", "content": user_prompt})

    for attempt in range(1, max_retries + 1):
        try:
            resp = LLM_CLIENT.chat.completions.create(
                model=LLM_MODEL,
                messages=messages,
                temperature=0.3,
                stream=False,
            )
            summary = resp.choices[0].message.content
            if summary:
                return summary.strip()
        except Exception as e:
            print(f"[WARN] 生成论文总结失败（第 {attempt} 次）：{e}")
            # 简单退避重试
            time.sleep(2 * attempt)

    print(f"[ERROR] 生成论文总结失败，超过最大重试次数（{max_retries}）。paper_id={paper_id}")
    return None


def process_single_paper(paper: dict, source: str | None = None, keywords: list[str] | None = None) -> None:
    """
    处理单篇论文：
    - 下载 PDF
    - 抽取文本为 .txt
    - 生成 Markdown 文件
    - 更新 _sidebar.md
    """
    paper_title = paper["title"]
    paper_authors = paper.get("authors") or []
    pdf_url = paper["pdf_url"]
    date = paper.get("published_date", TODAY)
    arxiv_id = paper.get("arxiv_id", "")

    # 为避免同一天多篇论文重名，文件名中加入 arxiv_id
    slug = slugify(paper_title)
    basename = f"{arxiv_id}-{slug}" if arxiv_id else slug

    # 目录结构：docs/YYYYMM/DD/xxx.md
    # 其中 YYYYMM 来自发布日期，DD 是天
    try:
        y, m, d = date.split("-")
        ym = f"{y}{m}"
        day = d
    except Exception:
        # 如果日期解析失败，退回到不分目录的平铺结构
        ym = "unknown"
        day = "00"

    rel_dir = f"{ym}/{day}"
    paper_id = f"{rel_dir}/{basename}"

    target_dir = os.path.join(DOCS_DIR, ym, day)
    md_file_name = f"{basename}.md"
    md_file_path = os.path.join(target_dir, md_file_name)
    txt_file_name = f"{basename}.txt"
    txt_file_path = os.path.join(target_dir, txt_file_name)

    # 如果已经处理过同一篇论文，可以直接跳过（避免重复生成）
    if os.path.exists(md_file_path):
        print(f"[SKIP] 已存在 Markdown：{md_file_path}")
        return

    os.makedirs(target_dir, exist_ok=True)

    # 1. 下载 PDF 原文到临时文件，仅用于抽取文本，不在 docs 中保留 PDF
    resp = requests.get(pdf_url, timeout=60)
    resp.raise_for_status()
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp_pdf:
        tmp_pdf.write(resp.content)
        tmp_pdf.flush()

        # 2. 使用 PyMuPDF 抽取文本内容，保存为 .txt 文件
        pdf_text = extract_pdf_text(tmp_pdf.name)

    with open(txt_file_path, "w", encoding="utf-8") as f:
        f.write(pdf_text)

    # 3. 生成对应的 Markdown 文件（用于 Docsify 展示）
    # 标签区域：根据来源和关键词构建，颜色与订阅面板一致
    tag_parts: list[str] = []
    if source == "keywords" and keywords:
        for kw in keywords:
            tag_parts.append(
                f'<span class="tag-label tag-green">keywords: {kw}</span>'
            )
    elif source == "zotero":
        tag_parts.append(
            '<span class="tag-label tag-blue">zotero: recommended</span>'
        )
    tags_html = " ".join(tag_parts) if tag_parts else ""

    content = f"""
# {paper_title}

**Authors**: {', '.join(paper_authors) if paper_authors else 'Unknown'}
**Date**: {date}

{"**Tags**: " + tags_html if tags_html else ""}

---

## Abstract
...
"""
    with open(md_file_path, "w", encoding="utf-8") as f:
        f.write(content)

    # 3.1 调用大模型生成详细总结，并写入 Markdown
    try:
        summary = generate_paper_summary(paper_id, md_file_path, txt_file_path)
        if summary:
            with open(md_file_path, "a", encoding="utf-8") as f:
                f.write("\n\n---\n\n## 论文详细总结（自动生成）\n\n")
                f.write(summary)
            print(f"[OK] 已为 {md_file_name} 生成自动总结。")
        else:
            print(f"[WARN] 未能为 {md_file_name} 生成自动总结。")
    except Exception as e:
        print(f"[ERROR] 生成自动总结时出错（{md_file_name}）：{e}")

    # 4. 更新 _sidebar.md (把新文章插到最前面)
    sidebar_path = os.path.join(DOCS_DIR, "_sidebar.md")
    # Docsify 中的路由 ID 与 paper_id 对应（包含年月日子目录）
    new_entry = f"  * [{date} - {paper_title}]({paper_id})\n"

    lines = []
    if os.path.exists(sidebar_path):
        with open(sidebar_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    # 找到 "Daily Papers" 这一行，插在它后面
    insert_idx = -1
    for i, line in enumerate(lines):
        if "Daily Papers" in line:
            insert_idx = i + 1
            break

    if insert_idx != -1:
        lines.insert(insert_idx, new_entry)
        with open(sidebar_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    else:
        # 如果没找到，就追加
        with open(sidebar_path, "a", encoding="utf-8") as f:
            f.write("\n* Daily Papers\n" + new_entry)

    print(f"[OK] 生成 {md_file_path}, {txt_file_path}")


def main():
    keywords = get_keywords()
    print(f"[INFO] 今日日期：{TODAY}")
    print(f"[INFO] 使用关键词（OR 逻辑）：{keywords}")
    print(f"[INFO] UTC 当前日期：{datetime.datetime.utcnow().date()}，窗口天数：{os.getenv('ARXIV_DAYS_WINDOW', '2')}天")

    papers = search_arxiv_today(keywords)
    if not papers:
        print("[INFO] 今天没有匹配到符合条件的新论文。")
        return

    print(f"[INFO] 找到 {len(papers)} 篇今日新论文，即将生成本地文件...")
    for idx, paper in enumerate(papers, start=1):
        print(f"[INFO] 处理第 {idx} 篇：{paper['title']}")
        try:
            process_single_paper(paper, source="keywords", keywords=keywords)
        except Exception as e:
            # 单篇失败不影响其他论文
            print(f"[ERROR] 处理论文失败：{paper.get('title')}，原因：{e}")


if __name__ == "__main__":
    main()
