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

# 项目根目录与配置文件路径
ROOT_DIR = os.path.dirname(os.path.dirname(__file__))
CONFIG_FILE = os.path.join(ROOT_DIR, "config.yaml")


def load_config() -> dict:
    """
    从仓库根目录读取 config.yaml。
    如果文件不存在或解析失败，则返回空字典，并保持向后兼容旧逻辑。
    """
    if not os.path.exists(CONFIG_FILE):
        return {}

    try:
        import yaml  # type: ignore
    except Exception:
        # 未安装 PyYAML 时仅给出提示，不影响旧逻辑使用
        print("[WARN] 未安装 PyYAML，无法解析 config.yaml，将退回旧配置逻辑。")
        return {}

    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                return data
            print("[WARN] config.yaml 顶层结构不是字典，将忽略该配置文件。")
            return {}
    except Exception as e:
        print(f"[WARN] 读取 config.yaml 失败，将退回旧配置逻辑：{e}")
        return {}


CONFIG: dict = load_config()


def resolve_docs_dir() -> str:
    """
    解析文档目录，优先级：
    1. 环境变量 DOCS_DIR
    2. config.yaml 中 crawler.docs_dir（支持相对路径）
    3. 默认：<项目根>/docs
    """
    # 1. 环境变量最高优先级
    docs_dir = os.getenv("DOCS_DIR")

    # 2. 配置文件中的 crawler.docs_dir
    crawler_cfg = (CONFIG or {}).get("crawler") or {}
    cfg_docs_dir = crawler_cfg.get("docs_dir")
    if not docs_dir and cfg_docs_dir:
        # 支持相对路径（相对项目根目录）
        if os.path.isabs(cfg_docs_dir):
            docs_dir = cfg_docs_dir
        else:
            docs_dir = os.path.join(ROOT_DIR, cfg_docs_dir)

    # 3. 默认值
    if not docs_dir:
        docs_dir = os.path.join(ROOT_DIR, "docs")

    return docs_dir


def get_crawler_params() -> tuple[int, int]:
    """
    读取抓取相关配置：
    - days_window：搜索时间窗口（天），默认 10
    - max_results：最大结果数，默认 50

    优先级：
    1. 环境变量 ARXIV_DAYS_WINDOW / ARXIV_MAX_RESULTS
    2. config.yaml 中 crawler.days_window / crawler.max_results
       （如果存在则覆盖环境变量）
    3. 内置默认值
    """
    # 内置默认值
    days_window = 10
    max_results = 50

    # 先读取环境变量
    try:
        days_window = int(os.getenv("ARXIV_DAYS_WINDOW", str(days_window)))
    except Exception:
        pass
    try:
        max_results = int(os.getenv("ARXIV_MAX_RESULTS", str(max_results)))
    except Exception:
        pass

    # 再用 config.yaml 覆盖（如果存在）
    crawler_cfg = (CONFIG or {}).get("crawler") or {}
    if "days_window" in crawler_cfg:
        try:
            days_window = int(crawler_cfg["days_window"])
        except Exception:
            print("[WARN] config.yaml 中 crawler.days_window 解析失败，使用已有值。")
    if "max_results" in crawler_cfg:
        try:
            max_results = int(crawler_cfg["max_results"])
        except Exception:
            print("[WARN] config.yaml 中 crawler.max_results 解析失败，使用已有值。")

    # 合理性保护
    if days_window < 1:
        days_window = 1
    if max_results < 1:
        max_results = 1

    return days_window, max_results


# 配置
DOCS_DIR = resolve_docs_dir()
CRAWLER_DAYS_WINDOW, CRAWLER_MAX_RESULTS = get_crawler_params()
TODAY = datetime.date.today().strftime("%Y-%m-%d")

# 与后端共用的数据库文件路径（如果存在，则可从中读取订阅关键词）
DB_FILE = os.path.join(ROOT_DIR, "app", "chat.db")

# 大模型配置（与 app/main.py 保持一致）
LLM_API_KEY = os.getenv("LLM_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "glm-4.7")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://open.bigmodel.cn/api/coding/paas/v4")
LLM_CLIENT = None
if LLM_API_KEY:
    LLM_CLIENT = openai.OpenAI(api_key=LLM_API_KEY, base_url=LLM_BASE_URL)

# 重排序（rerank）配置
RERANK_API_KEY = os.getenv("RERANK_API_KEY")
RERANK_MODEL = os.getenv("RERANK_MODEL", "qwen3-embedding-4b")
RERANK_ENDPOINT = os.getenv("RERANK_ENDPOINT", "https://api.bltcy.ai/v1/rerank")
RERANK_QUERY = os.getenv("RERANK_QUERY", "这个问题和attention模型挂钩吗")


def get_keywords() -> list[str]:
    """
    优先从 config.yaml 中的 subscriptions.keywords 读取订阅关键词；
    如果未配置或为空，再从数据库 subscriptions_keywords 中读取；
    如果数据库或表不存在，或者列表为空，则回退到环境变量 ARXIV_KEYWORDS；
    如果仍然为空，则使用一个默认示例，方便本地调试。
    """
    # 0. 首选 config.yaml 中的 subscriptions.keywords
    config_keywords: list[str] = []
    subs_cfg = (CONFIG or {}).get("subscriptions") or {}
    cfg_keywords = subs_cfg.get("keywords")
    if isinstance(cfg_keywords, list):
        for item in cfg_keywords:
            if isinstance(item, str):
                kw = item.strip()
                if kw:
                    config_keywords.append(kw)
            elif isinstance(item, dict):
                # 兼容对象形式：{ keyword: "...", alias: "..." }
                kw = (item.get("keyword") or "").strip()
                if kw:
                    config_keywords.append(kw)

    if config_keywords:
        return config_keywords

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


def build_arxiv_query(raw_keyword: str) -> str:
    """
    基于单个原始关键词字符串构建 arxiv 搜索 query。

    支持的语法：
    - 使用 "||" 作为 OR 分隔符，例如：
      "LLM || foundation model" -> all:"LLM" OR all:"foundation model"
    - 使用 "&&" 作为 AND 分隔符，例如：
      "LLM && RL" -> all:"LLM" AND all:"RL"
      "LLM && RL || diffusion" -> (all:"LLM" AND all:"RL") OR all:"diffusion"
    - 使用 "author:" 前缀仅按作者搜索，例如：
      "author:Goodfellow" -> au:"Goodfellow"
      "author:LeCun || GAN" -> au:"LeCun" OR all:"GAN"
    """
    if not raw_keyword:
        return ""

    # 内部辅助：从单个片段生成一个 arxiv 字段查询（作者/全字段）
    def build_term(part: str) -> str | None:
        s = (part or "").strip()
        if not s:
            return None

        # 兼容 "author:xxx" / "author : xxx" / "Author：xxx"
        idx_normal = s.find(":")
        idx_full = s.find("：")
        idx = -1
        if idx_normal >= 0 and idx_full >= 0:
            idx = min(idx_normal, idx_full)
        elif idx_normal >= 0:
            idx = idx_normal
        elif idx_full >= 0:
            idx = idx_full

        if idx >= 0:
            prefix = s[:idx].strip().lower()
            value = s[idx + 1 :].strip()
            if prefix == "author":
                if not value:
                    return None
                return f'au:"{value}"'

        # 默认：全字段搜索
        return f'all:"{s}"'

    # 先按 "||" 拆分为多个 OR 组，每个组内再按 "&&" 做 AND 组合
    raw_or_groups = [p.strip() for p in raw_keyword.split("||") if p.strip()]
    if not raw_or_groups:
        return ""

    group_exprs: list[str] = []
    for group in raw_or_groups:
        and_parts = [p.strip() for p in group.split("&&") if p.strip()]
        if not and_parts:
            continue

        term_exprs: list[str] = []
        for part in and_parts:
            term = build_term(part)
            if term:
                term_exprs.append(term)

        if not term_exprs:
            continue

        # 为了保持逻辑明确，这里为 AND 组加上括号
        if len(term_exprs) == 1:
            group_exprs.append(term_exprs[0])
        else:
            group_exprs.append("(" + " AND ".join(term_exprs) + ")")

    return " OR ".join(group_exprs)


def search_arxiv_today(keywords: list[str], max_results: int = 50) -> list[dict]:
    """
    使用 arxiv 包调用 arXiv 搜索接口，获取“最近 N 天（按 UTC）发布”的论文。

    调整后的策略：
    - 从配置中获取若干个原始关键词（每一项对应面板中的一行配置）；
    - 针对每一个关键词单独发起一次搜索请求；
    - 每个关键词最多抓取 max_results 篇论文（单关键词上限）；
    - 每个关键词之间 sleep 3 秒，避免请求过于频繁；
    - 不限制 arXiv 的学科领域，仅按关键词语义搜索。

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

    # 最近 N 天的窗口（按 UTC），使用全局抓取配置
    days_window = CRAWLER_DAYS_WINDOW
    # 使用配置中的 max_results 作为默认上限，允许调用方显式传入覆盖
    if max_results <= 0:
        max_results = CRAWLER_MAX_RESULTS

    utc_today = datetime.datetime.utcnow().date()
    cutoff_date = utc_today - datetime.timedelta(days=days_window - 1)

    client = arxiv.Client()

    # 使用字典去重：同一篇论文可能被多个关键词命中
    results_by_id: dict[str, dict] = {}

    for idx, raw_kw in enumerate(keywords):
        kw = (raw_kw or "").strip()
        if not kw:
            continue

        query_str = build_arxiv_query(kw)
        if not query_str:
            continue

        print(f"[INFO] 使用关键词（单独搜索）：{kw}")
        print(f"[DEBUG] 构造的 arxiv query: {query_str}")

        search = arxiv.Search(
            query=query_str,
            max_results=max_results,
            sort_by=arxiv.SortCriterion.SubmittedDate,
            sort_order=arxiv.SortOrder.Descending,
        )

        try:
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

                if arxiv_id in results_by_id:
                    # 已经收集过的论文，这里只做去重，不额外记录
                    continue

                results_by_id[arxiv_id] = {
                    "title": title,
                    "summary": summary,
                    "authors": authors,
                    "pdf_url": pdf_url,
                    "published_date": published_date,
                    "arxiv_id": arxiv_id,
                }
        except Exception as e:
            print(f"[WARN] 使用关键词「{kw}」搜索 arxiv 时出错，将跳过该关键词：{e}")
        finally:
            # 关键词之间间隔 3 秒，避免请求过于频繁
            if idx < len(keywords) - 1:
                time.sleep(3)

    return list(results_by_id.values())


def call_rerank_for_papers(papers: list[dict]) -> None:
    """
    使用外部 Rerank API 对已经抓取到的论文进行重排序打分。

    当前仅负责按 50 篇一组的方式发起请求，并打印响应结果，暂不改变后续处理顺序。
    后续如果你拿到更明确的返回结构，我们可以基于返回的 score/index
    来调整 papers 列表的排序或做过滤。
    """
    if not papers:
        return
    if not RERANK_API_KEY:
        print("[WARN] 未配置 RERANK_API_KEY，跳过重排序请求。")
        return

    # 每篇论文的文档内容：标题 + 摘要
    documents: list[str] = []
    for p in papers:
        title = (p.get("title") or "").strip()
        summary = (p.get("summary") or "").strip()
        if summary:
            doc = f"{title}\n\n{summary}"
        else:
            doc = title
        documents.append(doc)

    total = len(documents)
    print(f"[RERANK] 准备对 {total} 篇论文调用重排序接口，每 50 篇一组。")

    headers = {
        "Authorization": f"Bearer {RERANK_API_KEY}",
        "Content-Type": "application/json",
    }

    batch_size = 50
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch_docs = documents[start:end]
        payload = {
            "model": RERANK_MODEL,
            "query": RERANK_QUERY,
            "top_n": len(batch_docs),
            "documents": batch_docs,
        }
        batch_idx = start // batch_size + 1
        try:
            print(f"[RERANK] 调用第 {batch_idx} 组（索引 {start}~{end - 1}）...")
            resp = requests.post(RERANK_ENDPOINT, json=payload, headers=headers, timeout=60)
            print(f"[RERANK] 第 {batch_idx} 组返回状态码：{resp.status_code}")
            # 先简单打印前 200 个字符作为调试信息，方便你观察返回结构
            text = resp.text or ""
            preview = text[:200].replace("\n", " ")
            print(f"[RERANK] 第 {batch_idx} 组响应前 200 字符：{preview}")
        except Exception as e:
            print(f"[RERANK][WARN] 第 {batch_idx} 组调用重排序接口失败：{e}")


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


def fetch_paper_markdown_via_jina(pdf_url: str, max_retries: int = 3) -> str | None:
    """
    优先通过 https://r.jina.ai/ 获取结构化 Markdown 文本：
    - 直接请求：https://r.jina.ai/{pdf_url}
    - 默认最多重试 max_retries 次；
    - 请求成功且返回非空文本时，直接作为论文的“文本内容”使用；
    - 如果多次请求都失败，则返回 None，调用方可回退到本地 PyMuPDF 抽取。
    """
    if not pdf_url:
        return None

    base = "https://r.jina.ai/"
    full_url = base + pdf_url

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[JINA] 第 {attempt} 次请求：{full_url}")
            resp = requests.get(full_url, timeout=60)
            if resp.status_code != 200:
                print(
                    f"[JINA][WARN] 状态码 {resp.status_code}，响应内容前 100 字符："
                    f"{(resp.text or '')[:100].replace(os.linesep, ' ')}"
                )
            else:
                text = (resp.text or "").strip()
                if text:
                    print("[JINA] 获取到结构化 Markdown 文本，将直接用作 .txt 内容。")
                    return text
                else:
                    print("[JINA][WARN] 返回内容为空，重试中...")
        except Exception as e:
            print(f"[JINA][WARN] 请求失败（第 {attempt} 次）：{e}")

        # 简单退避，避免过于频繁
        time.sleep(2 * attempt)

    print("[JINA][ERROR] 多次请求 https://r.jina.ai/ 失败，将回退到 PyMuPDF 抽取。")
    return None


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

    # 读取 Markdown 与文本内容（不再对上下文进行截断，直接使用完整内容）
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

    messages = [
        {"role": "system", "content": system_prompt},
    ]

    if paper_txt_content:
        messages.append(
            {
                "role": "user",
                "content": f"### 论文 PDF 提取文本 ###\n{paper_txt_content}",
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


def translate_title_and_abstract_to_zh(title: str, abstract: str) -> tuple[str, str]:
    """
    使用已配置的大模型，将英文标题和摘要翻译为中文。
    - 保留英文原文，仅在可用时追加中文翻译；
    - 若未配置 LLM_API_KEY 或调用失败，则返回空字符串，不影响主流程。
    """
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
        resp = LLM_CLIENT.chat.completions.create(
            model=LLM_MODEL,
            messages=messages,
            temperature=0.2,
            stream=False,
        )
        content = (resp.choices[0].message.content or "").strip()
    except Exception as e:
        print(f"[WARN] 翻译标题和摘要为中文失败：{e}")
        return "", ""

    if not content:
        return "", ""

    # 简单按标记切分出标题和摘要的中文部分
    zh_title = ""
    zh_abstract = ""
    try:
        # 先按“标题”块切
        parts = content.split("【摘要（中文）】")
        if len(parts) == 2:
            title_block = parts[0]
            abstract_block = parts[1]
        else:
            # 无法严格匹配时，全部当摘要处理
            title_block = ""
            abstract_block = content

        if title_block:
            t = title_block.split("【标题（中文）】", 1)
            if len(t) == 2:
                zh_title = t[1].strip()
        zh_abstract = abstract_block.strip()
    except Exception:
        # 解析失败时，整个内容当成摘要使用
        zh_title = ""
        zh_abstract = content

    return zh_title, zh_abstract


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

    # 1. 优先通过 https://r.jina.ai/ 获取结构化 Markdown 文本；
    #    如果多次尝试均失败，再回退到下载 PDF + PyMuPDF 抽取文本。
    text_content: str | None = fetch_paper_markdown_via_jina(pdf_url)

    if text_content is None:
        # 回退方案：下载 PDF 原文到临时文件，仅用于抽取文本，不在 docs 中保留 PDF
        resp = requests.get(pdf_url, timeout=60)
        resp.raise_for_status()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp_pdf:
            tmp_pdf.write(resp.content)
            tmp_pdf.flush()

            # 使用 PyMuPDF 抽取文本内容
            text_content = extract_pdf_text(tmp_pdf.name)

    with open(txt_file_path, "w", encoding="utf-8") as f:
        f.write(text_content or "")

    # 3. 生成对应的 Markdown 文件（用于 Docsify 展示）
    # 3.0 构造标签区域（根据来源和关键词构建，颜色与订阅面板一致）
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

    # 3.1 使用 arxiv 返回的摘要填充 Abstract 区域；若缺失则给出占位说明
    abstract_en = (paper.get("summary") or "").strip()
    if not abstract_en:
        abstract_en = "arXiv did not provide an abstract for this paper, or the abstract could not be parsed correctly."

    # 3.2 调用大模型生成中文标题与摘要（在有大模型配置时）
    zh_title, zh_abstract = translate_title_and_abstract_to_zh(
        paper_title,
        abstract_en,
    )

    content = f"""
# {paper_title}

{f'# {zh_title}\n\n' if zh_title else ''}

**Authors**: {', '.join(paper_authors) if paper_authors else 'Unknown'}
**Date**: {date}

{"**Tags**: " + tags_html if tags_html else ""}

---

## Abstract
{abstract_en}

{f'\n\n## 摘要\n{zh_abstract}' if zh_abstract else ''}
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

    # 4. 更新 _sidebar.md
    #    目标结构按“Day 文件夹”分组，例如：
    #    * Daily Papers
    #      * 2025-12-19
    #        * [Paper Title](202512/19/xxx-xxx)
    sidebar_path = os.path.join(DOCS_DIR, "_sidebar.md")
    # Docsify 中的路由 ID 与 paper_id 对应（包含年月日子目录）
    day_heading = f"  * {date}\n"
    paper_entry = f"    * [{paper_title}]({paper_id})\n"

    lines: list[str] = []
    if os.path.exists(sidebar_path):
        with open(sidebar_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

    # 确保存在 "* Daily Papers" 分组
    daily_idx = -1
    for i, line in enumerate(lines):
        if line.strip().startswith("* Daily Papers"):
            daily_idx = i
            break

    # 如果原来没有 Daily Papers 区块，则初始化一个简单的结构
    if daily_idx == -1:
        # 尽量保留已有的“首页”入口；如果没有，就补一个
        has_home = any("[首页]" in line for line in lines)
        if not has_home:
            lines.append("* [首页](/)\n")
        lines.append("* Daily Papers\n")
        daily_idx = len(lines) - 1

    # 查找是否已经有当前日期的小节
    day_idx = -1
    for i in range(daily_idx + 1, len(lines)):
        line = lines[i]
        # 遇到下一个顶级分组（不以两个空格开头的 *）就结束本分组搜索
        if line.startswith("* "):
            break
        if line == day_heading:
            day_idx = i
            break

    if day_idx == -1:
        # 当前日期还没有小节：在 Daily Papers 之后插入“当天小节 + 第一篇论文”
        insert_idx = daily_idx + 1
        lines.insert(insert_idx, day_heading)
        lines.insert(insert_idx + 1, paper_entry)
    else:
        # 已有当天小节：把新论文插入到该日期下的最前面
        insert_idx = day_idx + 1
        # 跳过该日期下已有的论文条目（四个空格缩进）
        while insert_idx < len(lines) and lines[insert_idx].startswith("    * "):
            insert_idx += 1
        # 新条目插在 day_heading 后面，使最新论文在最上方
        lines.insert(day_idx + 1, paper_entry)

    with open(sidebar_path, "w", encoding="utf-8") as f:
        f.writelines(lines)

    print(f"[OK] 生成 {md_file_path}, {txt_file_path}")


def main():
    keywords = get_keywords()
    print(f"[INFO] 今日日期：{TODAY}")
    print(f"[INFO] 使用关键词（逐个搜索）：{keywords}")
    print(
        f"[INFO] UTC 当前日期：{datetime.datetime.utcnow().date()}，"
        f"窗口天数：{CRAWLER_DAYS_WINDOW} 天，最大论文数：{CRAWLER_MAX_RESULTS}"
    )

    # 每个关键词单独搜索，单关键词最多 20 篇论文
    papers = search_arxiv_today(keywords, max_results=20)
    if not papers:
        print("[INFO] 今天没有匹配到符合条件的新论文。")
        return

    print(f"[INFO] 找到 {len(papers)} 篇今日新论文。")

    # 先按 50 篇一组调用外部 Rerank 接口，仅做打分和调试输出
    call_rerank_for_papers(papers)

    print("[INFO] 即将为这些论文生成本地文件...")
    for idx, paper in enumerate(papers, start=1):
        print(f"[INFO] 处理第 {idx} 篇：{paper['title']}")
        try:
            process_single_paper(paper, source="keywords", keywords=keywords)
        except Exception as e:
            # 单篇失败不影响其他论文
            print(f"[ERROR] 处理论文失败：{paper.get('title')}，原因：{e}")


if __name__ == "__main__":
    main()
