// Docsify 配置与公共插件（评论区 + Zotero 元数据）
window.$docsify = {
  name: 'Daily Paper Reader',
  repo: '',
  // 文档内容与侧边栏都存放在 docs/ 下
  basePath: 'docs/', // 所有 Markdown 路由以 docs/ 为前缀
  loadSidebar: '_sidebar.md', // 在 basePath 下加载 _sidebar.md
  // 始终使用根目录的 _sidebar.md，避免每个子目录都要放一份
  alias: {
    '/.*/_sidebar.md': '/_sidebar.md',
  },
  // 只在侧边栏展示论文列表标题，不展示文内小节（例如 Abstract）
  subMaxLevel: 0,

  // --- 核心：注册自定义插件 ---
  plugins: [
    function (hook, vm) {
      // 确保 marked 开启 GFM 表格支持，并允许内联 HTML（用于聊天区 Markdown 渲染）
      if (window.marked && window.marked.setOptions) {
        const baseOptions =
          (window.marked.getDefaults && window.marked.getDefaults()) || {};
        window.marked.setOptions(
          Object.assign({}, baseOptions, {
            gfm: true,
            breaks: false,
            tables: true,
            // 允许 <sup> 等内联 HTML 直接渲染，而不是被转义
            sanitize: false,
            mangle: false,
            headerIds: false,
          }),
        );
      }

      // 1. 解析当前文章 ID (简单用文件名作为 ID)
      const getPaperId = () => {
        return vm.route.file.replace('.md', '');
      };

      const metaFallbacks = {
        citation_title: 'Daily Paper Reader Default Entry',
        citation_journal_title: 'Daily Paper Reader (ArXiv)',
        citation_pdf_url: 'https://daily-paper-reader.invalid/default.pdf',
        citation_publication_date: '2024-01-01',
        citation_date: '2024/01/01',
      };

      const defaultAuthors = ['Daily Paper Reader Team', 'Docsify Renderer'];

      // Zotero 摘要结构标记：方便后续在 Zotero 插件中重新解析
      const START_MARKER = '【🤖 AI Summary】';
      const CHAT_MARKER = '【💬 Chat History】';
      const ORIG_MARKER = '【📄 Original Abstract】';

      // Zotero 元数据更新函数：可被 Docsify 生命周期和聊天模块重复调用
      const updateZoteroMetaFromPage = (paperId, vmRouteFile) => {
        try {
          // 优先使用自定义标题条（避免 h1 被隐藏/改造后 innerText 不稳定）
          const dprEn = document.querySelector('.dpr-title-en');
          const dprCn = document.querySelector('.dpr-title-cn');
          let title = '';
          if (dprEn && (dprEn.textContent || '').trim()) {
            title = (dprEn.textContent || '').trim();
          } else if (dprCn && (dprCn.textContent || '').trim()) {
            title = (dprCn.textContent || '').trim();
          } else {
            const titleEl = document.querySelector('.markdown-section h1');
            title = titleEl ? (titleEl.textContent || '').trim() : document.title;
          }
          if (title) {
            // 清理标题中的多余空白与插件注入内容
            title = title.replace(/\s+/g, ' ').trim();
          }

          let pdfLinkEl = document.querySelector('a[href*="arxiv.org/pdf"]');
          if (!pdfLinkEl) {
            pdfLinkEl = document.querySelector('a[href$=".pdf"]');
          }

          let pdfUrl = '';
          if (pdfLinkEl) {
            pdfUrl = new URL(pdfLinkEl.href, window.location.href).href;
          }

          let date = '';
          const matchDate = vmRouteFile
            ? vmRouteFile.match(/(\d{4}-\d{2}-\d{2})/)
            : null;
          if (matchDate) {
            date = matchDate[1];
          }
          const citationDate = date ? date.replace(/-/g, '/') : '';

          let authors = [];
          let tagsLine = '';
          document.querySelectorAll('.markdown-section p').forEach((p) => {
            if (p.innerText.includes('Authors:')) {
              let text = p.innerText.replace('Authors:', '').trim();
              // 清理可能被其它扩展注入的换行和尾部信息，以及尾部日期
              text = text.replace(/\s+/g, ' ').trim();
              text = text
                .replace(/Date\s*:\s*\d{4}-\d{2}-\d{2}.*/i, '')
                .trim();
              authors = text
                .split(/,|，/)
                .map((a) => a.trim())
                .filter(Boolean);
            } else if (p.innerText.includes('Tags:')) {
              // 提取 Tags 行，用于 AI Summary 区块展示
              tagsLine = (p.innerText || '').trim();
            }
          });

          updateMetaTag('citation_title', title);
          updateMetaTag('citation_journal_title', 'Daily Paper Reader (ArXiv)');
          updateMetaTag('citation_pdf_url', pdfUrl, {
            useFallback: false,
          });
          updateMetaTag('citation_publication_date', date);
          updateMetaTag('citation_date', citationDate);

          // 构造给 Zotero 用的“摘要”元信息：按「AI 总结 / 对话历史 / 原始摘要」分段组织
          let abstractText = '';
          const sectionEl = document.querySelector('.markdown-section');
          if (sectionEl) {
            let aiSummaryText = '';
            let origAbstractText = '';

            // 1) 从 Markdown 中提取“论文详细总结（自动生成）”这一节，作为 AI 总结
            const h2List = Array.from(sectionEl.querySelectorAll('h2'));
            const summaryHeader = h2List.find((h) =>
              h.innerText.includes('论文详细总结'),
            );
            if (summaryHeader) {
              let cursor = summaryHeader.nextElementSibling;
              const parts = [];
              while (
                cursor &&
                cursor.tagName !== 'H1' &&
                cursor.tagName !== 'H2'
              ) {
                parts.push(cursor.innerText || '');
                cursor = cursor.nextElementSibling;
              }
              aiSummaryText = parts.join('\n\n').trim();
            }

            // 2) 提取「原始摘要」区域（例如 "## Abstract" 或包含“摘要”的二级标题）
            const abstractHeader = h2List.find((h) =>
              /abstract|摘要/i.test(h.innerText || ''),
            );
            if (abstractHeader) {
              let cursor = abstractHeader.nextElementSibling;
              const parts = [];
              while (
                cursor &&
                cursor.tagName !== 'H1' &&
                cursor.tagName !== 'H2'
              ) {
                // 一旦遇到聊天容器（或其父容器），立即停止，避免把“私人研讨区”等内容当作摘要
                if (
                  cursor.id === 'paper-chat-container' ||
                  (cursor.querySelector &&
                    cursor.querySelector('#paper-chat-container'))
                ) {
                  break;
                }
                parts.push(cursor.innerText || '');
                cursor = cursor.nextElementSibling;
              }
              origAbstractText = parts.join('\n\n').trim();
            }

            // 如果没有找到 AI 总结，就退回到正文前几段作为粗略总结
            if (!aiSummaryText) {
              const paras = [];
              sectionEl.querySelectorAll('p').forEach((p) => {
                if (paras.length >= 6) return;
                // 跳过聊天区域中的段落，避免把私人研讨区内容当作总结
                if (p.closest && p.closest('#paper-chat-container')) return;
                paras.push(p);
              });
              aiSummaryText = paras
                .map((p) => p.innerText || '')
                .join('\n\n')
                .trim();
            }

            // 3) 解析聊天历史，按「User / AI」打标签
            let chatSection = '';
            const chatRoot = document.getElementById('chat-history');
            if (chatRoot) {
              const items = chatRoot.querySelectorAll('.msg-item');
              const lines = [];
              items.forEach((item) => {
                const roleEl = item.querySelector('.msg-role');
                const contentEl = item.querySelector('.msg-content');
                if (!roleEl || !contentEl) return;
                const roleText = roleEl.textContent || '';
                // 显式排除“思考过程”类消息（thinking）
                if (roleText.includes('思考过程')) return;
                let speaker = '';
                if (roleText.includes('你')) {
                  speaker = 'User';
                } else if (roleText.includes('助手')) {
                  speaker = 'AI';
                } else {
                  // 略过其它未知角色
                  return;
                }
                const contentText = (contentEl.innerText || '').trim();
                if (!contentText) return;
                const icon = speaker === 'User' ? '👤' : '🤖';
                lines.push(`${icon} ${speaker}: ${contentText}`);
              });
              if (lines.length) {
                // 不再截断，对话区所有内容全部写入摘要
                chatSection = lines.join('\n\n');
              }
            }

            const parts = [];
            if (aiSummaryText || tagsLine) {
              // AI Summary 区块：保留 Tags 行，但不再包含 Authors 信息
              let aiBlock = `${START_MARKER}\n`;
              if (tagsLine) {
                aiBlock += `${tagsLine}\n\n`;
              }
              if (aiSummaryText) {
                aiBlock += aiSummaryText;
              }
              parts.push(aiBlock.trim());
            }
            if (chatSection) {
              parts.push(`${CHAT_MARKER}\n${chatSection}`);
            }
            if (origAbstractText) {
              parts.push(`${ORIG_MARKER}\n${origAbstractText}`);
            }
            abstractText = parts.join('\n\n\n').trim();
          }

          if (abstractText) {
            // 为兼容 Zotero 的摘要存储行为，将换行统一替换为占位符 __BR__
            const abstractForMeta = abstractText.replace(/\n/g, '__BR__');

            // 写入多种摘要字段，提升 Zotero 等工具的识别率
            updateMetaTag('citation_abstract', abstractForMeta, {
              useFallback: false,
            });
            updateMetaTag('description', abstractForMeta, {
              useFallback: false,
            });
            updateMetaTag('dc.description', abstractForMeta, {
              useFallback: false,
            });
            updateMetaTag('abstract', abstractForMeta, {
              useFallback: false,
            });
            updateMetaTag('DC.description', abstractForMeta, {
              useFallback: false,
            });
          }

          document
            .querySelectorAll('meta[name="citation_author"]')
            .forEach((el) => el.remove());
          const authorList = authors.length ? authors : defaultAuthors;
          authorList.forEach((author) => {
            const meta = document.createElement('meta');
            meta.name = 'citation_author';
            meta.content = author;
            document.head.appendChild(meta);
          });

          document.dispatchEvent(
            new Event('ZoteroItemUpdated', {
              bubbles: true,
              cancelable: true,
            }),
          );
        } catch (e) {
          console.error('Zotero meta update failed:', e);
        }
      };

      // 导出给其它前端模块（例如聊天模块）主动刷新 Zotero 元数据
      window.DPRZoteroMeta = window.DPRZoteroMeta || {};
      window.DPRZoteroMeta.updateFromPage = (paperId, vmRouteFile) =>
        updateZoteroMetaFromPage(paperId, vmRouteFile);

      // 公共工具：在指定元素上渲染公式
      const renderMathInEl = (el) => {
        if (!window.renderMathInElement || !el) return;
        window.renderMathInElement(el, {
          delimiters: [
            { left: '$$', right: '$$', display: true },
            { left: '$', right: '$', display: false },
          ],
          throwOnError: false,
        });
      };

      // 公共工具：简单表格 + 标记修正：
      // 1）移除协议标记 [ANS]/[THINK]
      // 2）移除表格行之间多余空行，避免把同一张表拆成两块
      const normalizeTables = (markdown) => {
        if (!markdown) return '';
        // 清理历史遗留的协议标记
        let text = markdown
          .replace(/\[ANS\]/g, '')
          .replace(/\[THINK\]/g, '');

        const lines = text.split('\n');
        const isTableLine = (line) => /^\s*\|.*\|\s*$/.test(line);
        const result = [];
        for (let i = 0; i < lines.length; i++) {
          const line = lines[i];
          const prev = result.length ? result[result.length - 1] : '';
          const next = i + 1 < lines.length ? lines[i + 1] : '';
          if (
            line.trim() === '' &&
            isTableLine(prev || '') &&
            isTableLine(next || '')
          ) {
            // 跳过表格行之间的空行
            continue;
          }
          result.push(line);
        }
        return result.join('\n');
      };

      const escapeHtml = (str) => {
        return str
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      };

      // 自定义表格渲染：检测 Markdown 表格块并手写生成 <table>，
      // 其他内容仍交给 marked 渲染。
      const renderMarkdownWithTables = (markdown) => {
        const text = normalizeTables(markdown || '');
        const lines = text.split('\n');
        const isTableLine = (line) => /^\s*\|.*\|\s*$/.test(line);
        const isAlignLine = (line) =>
          /^\s*\|(?:\s*:?-+:?\s*\|)+\s*$/.test(line);

        const parseRow = (line) => {
          const trimmed = line.trim().replace(/^\|/, '').replace(/\|$/, '');
          return trimmed.split('|').map((cell) => cell.trim());
        };

        const inlineRender = (cellText) => {
          if (!cellText) return '';
          if (window.marked && window.marked.parseInline) {
            return window.marked.parseInline(cellText);
          }
          return escapeHtml(cellText);
        };

        const blocks = [];
        let i = 0;

        const flushParagraph = (paraLines) => {
          const paraText = paraLines.join('\n').trim();
          if (!paraText) return;
          if (window.marked) {
            blocks.push(window.marked.parse(`\n${paraText}\n`));
          } else {
            blocks.push(`<p>${escapeHtml(paraText)}</p>`);
          }
        };

        while (i < lines.length) {
          const line = lines[i];

          // 检测表格块：当前行是表格行，下一行是对齐行
          if (
            isTableLine(line) &&
            i + 1 < lines.length &&
            isAlignLine(lines[i + 1])
          ) {
            const headerLine = lines[i];
            i += 2; // 跳过对齐行

            const bodyLines = [];
            while (i < lines.length && isTableLine(lines[i])) {
              bodyLines.push(lines[i]);
              i++;
            }

            const headers = parseRow(headerLine);
            const rows = bodyLines.map(parseRow);

            let html = '<table class="chat-table"><thead><tr>';
            headers.forEach((h) => {
              html += `<th>${inlineRender(h)}</th>`;
            });
            html += '</tr></thead><tbody>';
            rows.forEach((row) => {
              html += '<tr>';
              row.forEach((cell) => {
                html += `<td>${inlineRender(cell)}</td>`;
              });
              html += '</tr>';
            });
            html += '</tbody></table>';

            blocks.push(html);
          } else {
            // 非表格块：收集到下一个表格或结尾
            const paraLines = [];
            while (
              i < lines.length &&
              !(
                isTableLine(lines[i]) &&
                i + 1 < lines.length &&
                isAlignLine(lines[i + 1])
              )
            ) {
              paraLines.push(lines[i]);
              i++;
            }
            flushParagraph(paraLines);
          }
        }

        return blocks.join('');
      };

      const updateMetaTag = (name, content, options = {}) => {
        const old = document.querySelector(`meta[name="${name}"]`);
        if (old) old.remove();
        const useFallback = options.useFallback !== false;
        const value = content || (useFallback ? metaFallbacks[name] : '');
        if (!value) return;
        const meta = document.createElement('meta');
        meta.name = name;
        meta.content = value;
        document.head.appendChild(meta);
      };

      // 导出给外部模块（例如聊天模块）复用
      window.DPRMarkdown = {
        normalizeTables,
        renderMarkdownWithTables,
        renderMathInEl,
      };

      // 3. 小屏下：点击侧边栏条目后自动收起侧边栏（全屏列表 → 正文）
      const setupMobileSidebarAutoCloseOnItemClick = () => {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;
        if (nav.dataset.mobileAutoCloseBound === '1') return;
        nav.dataset.mobileAutoCloseBound = '1';

        nav.addEventListener('click', (event) => {
          const link = event.target.closest('a');
          if (!link) return;

          const href = link.getAttribute('href') || '';
          // 只处理 Docsify 内部路由（#/ 开头），避免影响外链
          if (!href.includes('#/')) return;

          const width =
            window.innerWidth || document.documentElement.clientWidth || 0;
          if (width > 768) return;

          // 让 Docsify 先完成路由跳转，再收起侧边栏
          setTimeout(() => {
            const body = document.body;
            if (!body) return;
            body.classList.remove('close'); // 移除表示“展开”的 close 类，隐藏侧边栏
          }, 0);
        });
      };

      // 4. 侧边栏按“日期”折叠的辅助函数
      const setupCollapsibleSidebarByDay = () => {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;

        const STORAGE_KEY = 'dpr_sidebar_day_state_v1';
        let state = {};
        try {
          const raw = window.localStorage
            ? window.localStorage.getItem(STORAGE_KEY)
            : null;
          if (raw) {
            state = JSON.parse(raw) || {};
          }
        } catch {
          state = {};
        }
        // 先扫描一遍，找出所有日期和最新一天
        const items = nav.querySelectorAll('li');
        const dayItems = [];
        let latestDay = '';

        items.forEach((li) => {
          if (li.dataset.dayToggleApplied === '1') return;

          const childUl = li.querySelector(':scope > ul');
          const directLink = li.querySelector(':scope > a');
          if (!childUl || directLink) return;

          // 取第一个文本节点作为标签文本
          const first = li.firstChild;
          if (!first || first.nodeType !== Node.TEXT_NODE) return;
          const rawText = (first.textContent || '').trim();
          if (!/^\d{4}-\d{2}-\d{2}$/.test(rawText)) return;

          dayItems.push({ li, text: rawText, first });
          if (!latestDay || rawText > latestDay) {
            latestDay = rawText;
          }
        });

        if (!dayItems.length) return;

        // 判断是否出现了“更新后的新一天”
        const prevLatest =
          typeof state.__latestDay === 'string' ? state.__latestDay : null;
        const isNewDay =
          latestDay &&
          (!prevLatest || (typeof prevLatest === 'string' && latestDay > prevLatest));

        // 如果出现了新的一天：清空历史状态，只保留最新一天的信息
        if (isNewDay) {
          state = { __latestDay: latestDay };
        } else if (!prevLatest && latestDay) {
          // 第一次使用，没有历史记录但也不算“新一天触发重置”的场景：记录当前最新日期
          state.__latestDay = latestDay;
        }

        const hasAnyState =
          !isNewDay && Object.keys(state).some((k) => k !== '__latestDay');

        const ensureStateSaved = () => {
          try {
            if (window.localStorage) {
              window.localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
            }
          } catch {
            // ignore
          }
        };

        // 第二遍：真正安装折叠行为
        dayItems.forEach(({ li, text: rawText, first }) => {
          if (li.dataset.dayToggleApplied === '1') return;

          // 创建可点击的容器（包含日期文字和小箭头）
          const wrapper = document.createElement('div');
          wrapper.className = 'sidebar-day-toggle';

          const labelSpan = document.createElement('span');
          labelSpan.className = 'sidebar-day-toggle-label';
          labelSpan.textContent = rawText;

          const arrowSpan = document.createElement('span');
          arrowSpan.className = 'sidebar-day-toggle-arrow';
          arrowSpan.textContent = '▾';

          wrapper.appendChild(labelSpan);
          wrapper.appendChild(arrowSpan);

          // 用 wrapper 替换原始文本节点
          li.replaceChild(wrapper, first);

          // 决定默认展开 / 收起：
          // - 如果本次是“出现了新的一天”：清空历史，只展开最新一天；
          // - 否则若已有用户偏好（state），按偏好来；
          // - 否则（首次使用且没有历史）：仅“最新一天”展开，其余收起。
          let collapsed;
          if (isNewDay) {
            collapsed = rawText === latestDay ? false : true;
          } else if (hasAnyState) {
            const saved = state[rawText];
            if (saved === 'open') {
              collapsed = false;
            } else if (saved === 'closed') {
              collapsed = true;
            } else {
              // 新出现的日期：默认跟最新一天策略走
              collapsed = rawText === latestDay ? false : true;
            }
          } else {
            collapsed = rawText === latestDay ? false : true;
          }

          if (collapsed) {
            li.classList.add('sidebar-day-collapsed');
            arrowSpan.textContent = '▸';
          } else {
            li.classList.remove('sidebar-day-collapsed');
            arrowSpan.textContent = '▾';
          }

          wrapper.addEventListener('click', () => {
            const collapsed = li.classList.toggle('sidebar-day-collapsed');
            arrowSpan.textContent = collapsed ? '▸' : '▾';
            state[rawText] = collapsed ? 'closed' : 'open';
            state.__latestDay = latestDay;
            ensureStateSaved();
          });

          li.dataset.dayToggleApplied = '1';
        });
      };

      // 4. 论文“已阅读”状态管理（存储在 localStorage）
      const READ_STORAGE_KEY = 'dpr_read_papers_v1';

      const loadReadState = () => {
        try {
          if (!window.localStorage) return {};
          const raw = window.localStorage.getItem(READ_STORAGE_KEY);
          if (!raw) return {};
          const obj = JSON.parse(raw);
          if (!obj || typeof obj !== 'object') return {};

          // 兼容旧版本（值为 true 的情况）
          const normalized = {};
          Object.keys(obj).forEach((k) => {
            const v = obj[k];
            if (v === true || v === 'read') {
              normalized[k] = 'read';
            } else if (v === 'good' || v === 'bad') {
              normalized[k] = v;
            }
          });
          return normalized;
        } catch {
          return {};
        }
      };

      const saveReadState = (state) => {
        try {
          if (!window.localStorage) return;
          window.localStorage.setItem(READ_STORAGE_KEY, JSON.stringify(state));
        } catch {
          // ignore
        }
      };

	      const markSidebarReadState = (currentPaperId) => {
	        const nav = document.querySelector('.sidebar-nav');
	        if (!nav) return;

	        const state = loadReadState();
        if (currentPaperId) {
          if (!state[currentPaperId]) {
            state[currentPaperId] = 'read';
          }
          saveReadState(state);
        }

        const applyLiState = (li, paperIdFromHref) => {
          const status = state[paperIdFromHref];
          li.classList.remove(
            'sidebar-paper-read',
            'sidebar-paper-good',
            'sidebar-paper-bad',
          );
          if (status === 'good') {
            li.classList.add('sidebar-paper-good');
          } else if (status === 'bad') {
            li.classList.add('sidebar-paper-bad');
          } else if (status) {
            li.classList.add('sidebar-paper-read');
          }
        };

	        const links = nav.querySelectorAll('a[href*="#/"]');
	        links.forEach((a) => {
	          const href = a.getAttribute('href') || '';
	          const m = href.match(/#\/(.+)$/);
	          if (!m) return;
	          const paperIdFromHref = m[1].replace(/\/$/, '');
	          const li = a.closest('li');
	          if (!li) return;
	          // 标记这是一个具体论文条目，方便样式细化（避免整天标题一起高亮）
	          li.classList.add('sidebar-paper-item');

	          // 为侧边栏条目追加“不错 / 一般”圆圈图标按钮
	          let actionWrapper = li.querySelector('.sidebar-paper-rating-icons');
	          let goodIcon = actionWrapper
	            ? actionWrapper.querySelector('.sidebar-paper-rating-icon.good')
	            : null;
	          let badIcon = actionWrapper
	            ? actionWrapper.querySelector('.sidebar-paper-rating-icon.bad')
	            : null;
	          if (!actionWrapper) {
	            actionWrapper = document.createElement('span');
	            actionWrapper.className = 'sidebar-paper-rating-icons';

	            goodIcon = document.createElement('button');
	            goodIcon.className = 'sidebar-paper-rating-icon good';
	            goodIcon.title = '标记为「不错」';
	            goodIcon.innerHTML = '✓';

	            badIcon = document.createElement('button');
	            badIcon.className = 'sidebar-paper-rating-icon bad';
	            badIcon.title = '标记为「一般」';
	            badIcon.innerHTML = '✕';

	            goodIcon.addEventListener('click', (e) => {
	              e.preventDefault();
	              e.stopPropagation();
	              const latestState = loadReadState();
	              const current = latestState[paperIdFromHref];
	              if (current === 'good') {
	                latestState[paperIdFromHref] = 'read';
	              } else {
	                latestState[paperIdFromHref] = 'good';
	              }
	              saveReadState(latestState);
	              // 重新应用整棵侧边栏的已读/评价样式，确保当前选中项立即刷新
	              markSidebarReadState(null);
	            });

	            badIcon.addEventListener('click', (e) => {
	              e.preventDefault();
	              e.stopPropagation();
	              const latestState = loadReadState();
	              const current = latestState[paperIdFromHref];
	              if (current === 'bad') {
	                latestState[paperIdFromHref] = 'read';
	              } else {
	                latestState[paperIdFromHref] = 'bad';
	              }
	              saveReadState(latestState);
	              markSidebarReadState(null);
	            });

	            actionWrapper.appendChild(goodIcon);
	            actionWrapper.appendChild(badIcon);
	            a.parentNode.insertBefore(actionWrapper, a.nextSibling);
	          }

	          // 无论按钮是否刚创建，都要基于“最新 state”刷新激活态（支持空格键切换）
	          try {
	            const s = state[paperIdFromHref];
	            if (goodIcon) goodIcon.classList.toggle('active', s === 'good');
	            if (badIcon) badIcon.classList.toggle('active', s === 'bad');
	          } catch {
	            // ignore
	          }

	          applyLiState(li, paperIdFromHref);
	        });
	      };

      // 侧边栏/正文的论文页标题条：英文右侧，中文左侧，中间竖线
      const isPaperRouteFile = (file) => {
        const f = String(file || '');
        return /^\d{6}\/\d{2}\/.+\.md$/i.test(f);
      };

      const fitTextToBox = (el, minPx, maxPx) => {
        if (!el) return;
        let size = maxPx;
        el.style.fontSize = `${size}px`;
        // 逐步缩小直到不溢出或达到最小值
        // 注意：scrollHeight > clientHeight 表示溢出（包含被 line-clamp 截断的情况）
        while (size > minPx && el.scrollHeight > el.clientHeight + 1) {
          size -= 1;
          el.style.fontSize = `${size}px`;
        }
      };

      // 为切页动效准备一个“正文包装层”，避免把聊天浮层/白色遮罩一起做淡入淡出（否则会闪烁）
      const DPR_PAGE_CONTENT_CLASS = 'dpr-page-content';

      const ensurePageContentRoot = () => {
        const section = document.querySelector('.markdown-section');
        if (!section) return null;
        const existing = section.querySelector(
          `:scope > .${DPR_PAGE_CONTENT_CLASS}`,
        );
        if (existing) return existing;

        const root = document.createElement('div');
        root.className = DPR_PAGE_CONTENT_CLASS;
        // 将当前渲染出来的正文内容整体移入 root（此时 chat 模块尚未插入，避免把输入框一起移入）
        while (section.firstChild) {
          root.appendChild(section.firstChild);
        }
        section.appendChild(root);
        return root;
      };

      const getPageAnimEl = () => {
        const section = document.querySelector('.markdown-section');
        if (!section) return null;
        return (
          section.querySelector(`:scope > .${DPR_PAGE_CONTENT_CLASS}`) || section
        );
      };

      const applyPaperTitleBar = () => {
        const file = vm && vm.route ? vm.route.file : '';
        if (!isPaperRouteFile(file)) {
          document.body.classList.remove('dpr-paper-page');
          return;
        }
        document.body.classList.add('dpr-paper-page');

        const section = document.querySelector('.markdown-section');
        if (!section) return;
        const root =
          section.querySelector(`:scope > .${DPR_PAGE_CONTENT_CLASS}`) || section;

        // 防止重复插入
        const existing = root.querySelector('.dpr-title-bar');
        if (existing) existing.remove();

        const h1s = Array.from(root.querySelectorAll('h1'));
        if (!h1s.length) return;

        // 约定：如果有两个 h1，则第一个为英文、第二个为中文；
        // 如果只有一个 h1，则认为是“单标题”，放在左侧（cn 区），避免 dpr-title-single 隐藏右侧后变空白。
        let enTitle = (h1s[0].textContent || '').trim();
        let cnTitle = (h1s[1] ? (h1s[1].textContent || '').trim() : '').trim();
        if (h1s.length === 1) {
          cnTitle = enTitle;
          enTitle = '';
        }

        // 隐藏原始 h1，但保留在 DOM 里供复制/SEO/元信息提取兜底
        h1s.forEach((h) => h.classList.add('dpr-title-hidden'));

        const bar = document.createElement('div');
        bar.className = 'dpr-title-bar';
        bar.innerHTML = `
          <div class="dpr-title-cn">${escapeHtml(cnTitle || '')}</div>
          <div class="dpr-title-sep" aria-hidden="true"></div>
          <div class="dpr-title-en">${escapeHtml(enTitle || '')}</div>
        `;
        if (!cnTitle) {
          bar.classList.add('dpr-title-single');
        }

        root.insertBefore(bar, root.firstChild);

        // 字体自适应：让标题条高度稳定，长标题自动缩小
        requestAnimationFrame(() => {
          const cnEl = bar.querySelector('.dpr-title-cn');
          const enEl = bar.querySelector('.dpr-title-en');
          if (cnEl && cnTitle) fitTextToBox(cnEl, 14, 22);
          if (enEl && enTitle) fitTextToBox(enEl, 13, 20);
        });
      };

      // 论文页导航：左右滑动 / 键盘方向键切换论文
      const DPR_NAV_STATE = {
        paperHrefs: [],
        currentHref: '',
        lastNavTs: 0,
        lastNavSource: '', // 'click' | 'key' | 'wheel' | 'swipe' | ''
      };

      const DPR_SIDEBAR_CENTER_STATE = {
        lastHref: '',
        lastTs: 0,
      };

      const DPR_SIDEBAR_ACTIVE_INDICATOR = {
        el: null,
        parent: null,
        justMoved: false,
      };

      const getSidebarScrollEl = () => {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return null;
        const candidates = [
          nav,
          nav.closest('.sidebar'),
          nav.parentElement,
          document.querySelector('.sidebar'),
        ].filter(Boolean);
        for (const el of candidates) {
          try {
            if (el.scrollHeight > el.clientHeight + 4) return el;
          } catch {
            // ignore
          }
        }
        return nav;
      };

      const ensureSidebarActiveIndicator = () => {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return null;

        if (
          DPR_SIDEBAR_ACTIVE_INDICATOR.el &&
          DPR_SIDEBAR_ACTIVE_INDICATOR.parent === nav &&
          nav.contains(DPR_SIDEBAR_ACTIVE_INDICATOR.el)
        ) {
          return { el: DPR_SIDEBAR_ACTIVE_INDICATOR.el, newlyCreated: false };
        }

        // 清理旧的（例如热更新/重复初始化场景）
        try {
          if (DPR_SIDEBAR_ACTIVE_INDICATOR.el && DPR_SIDEBAR_ACTIVE_INDICATOR.el.remove) {
            DPR_SIDEBAR_ACTIVE_INDICATOR.el.remove();
          }
        } catch {
          // ignore
        }

        const indicator = document.createElement('div');
        indicator.className = 'dpr-sidebar-active-indicator';
        indicator.setAttribute('aria-hidden', 'true');
        // 刚创建时先禁用 transition，避免出现“从 sidebar 顶部滑下来”的二次动效
        indicator.style.transition = 'none';
        // 放在最前面，确保在所有 li 下面
        nav.insertBefore(indicator, nav.firstChild);
        DPR_SIDEBAR_ACTIVE_INDICATOR.el = indicator;
        DPR_SIDEBAR_ACTIVE_INDICATOR.parent = nav;
        return { el: indicator, newlyCreated: true };
      };

      const moveSidebarActiveIndicatorToEl = (li, options = {}) => {
        if (!li) return;
        const { animate = true } = options || {};
        const ensured = ensureSidebarActiveIndicator();
        if (!ensured || !ensured.el) return;
        const indicator = ensured.el;
        const newlyCreated = ensured.newlyCreated;

        // 只对论文条目启用（避免日期分组标题等）
        if (!li.classList || !li.classList.contains('sidebar-paper-item')) return;

        const x = li.offsetLeft;
        const y = li.offsetTop;
        const w = li.offsetWidth;
        const h = li.offsetHeight;

        // 新建/或要求不动画时：先关 transition，直接定位到最终位置，再恢复 transition
        if (newlyCreated || !animate) {
          indicator.style.transition = 'none';
        }

        indicator.style.width = `${w}px`;
        indicator.style.height = `${h}px`;
        indicator.style.transform = `translate3d(${x}px, ${y}px, 0)`;

        if (newlyCreated || !animate) {
          requestAnimationFrame(() => {
            indicator.style.transition = '';
          });
        }
      };

      const moveSidebarActiveIndicatorToHref = (href, options = {}) => {
        const targetHref = normalizeHref(href);
        if (!targetHref) return;
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;
        const link = nav.querySelector(`a[href="${targetHref}"]`);
        if (!link) return;
        const li = link.closest('li');
        moveSidebarActiveIndicatorToEl(li, options);
      };

      const DPR_TRANSITION = {
        // 'enter-from-left' | 'enter-from-right' | ''
        pendingEnter: '',
      };

      const normalizeHref = (href) => {
        const raw = String(href || '').trim();
        if (!raw) return '';
        // 统一成 "#/xxxx" 形式
        if (raw.startsWith('#/')) return raw;
        if (raw.startsWith('#')) return '#/' + raw.slice(1).replace(/^\//, '');
        return '#/' + raw.replace(/^\//, '');
      };

      const isPaperHref = (href) => {
        const h = normalizeHref(href);
        // 只匹配论文页：#/YYYYMM/DD/slug
        return /^#\/\d{6}\/\d{2}\/.+/i.test(h);
      };

      const collectPaperHrefsFromSidebar = () => {
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return [];
        const links = Array.from(nav.querySelectorAll('a[href]'));
        const out = [];
        const seen = new Set();
        links.forEach((a) => {
          const href = a.getAttribute('href') || '';
          if (!isPaperHref(href)) return;
          const norm = normalizeHref(href);
          if (seen.has(norm)) return;
          seen.add(norm);
          out.push(norm);
        });
        return out;
      };

      const updateNavState = () => {
        DPR_NAV_STATE.paperHrefs = collectPaperHrefsFromSidebar();
        const file = vm && vm.route ? vm.route.file : '';
        if (file && isPaperRouteFile(file)) {
          DPR_NAV_STATE.currentHref = normalizeHref('#/' + String(file).replace(/\.md$/i, ''));
        } else {
          DPR_NAV_STATE.currentHref = '';
        }
      };

      const centerSidebarOnHref = (href) => {
        const targetHref = normalizeHref(href);
        if (!targetHref) return;
        if (targetHref === DPR_SIDEBAR_CENTER_STATE.lastHref) return;
        const nav = document.querySelector('.sidebar-nav');
        if (!nav) return;

        const link =
          nav.querySelector(`a[href="${targetHref}"]`) ||
          nav.querySelector(`a[href="${targetHref.replace(/^#\//, '#/')}"]`);
        if (!link) return;

        const item = link.closest('li') || link;
        const scrollEl = getSidebarScrollEl();
        if (!scrollEl || scrollEl.scrollHeight <= scrollEl.clientHeight + 4) {
          DPR_SIDEBAR_CENTER_STATE.lastHref = targetHref;
          return;
        }

        const scrollRect = scrollEl.getBoundingClientRect();
        const itemRect = item.getBoundingClientRect();

        const currentTop = scrollEl.scrollTop;
        const deltaTop = itemRect.top - scrollRect.top;
        const targetTop =
          currentTop + deltaTop - (scrollRect.height / 2 - itemRect.height / 2);

        const clamped = Math.max(
          0,
          Math.min(targetTop, scrollEl.scrollHeight - scrollEl.clientHeight),
        );

        DPR_SIDEBAR_CENTER_STATE.lastTs = Date.now();
        DPR_SIDEBAR_CENTER_STATE.lastHref = targetHref;

        // 居中时只需要“滚动”动画，不做额外高亮动画
        const duration = prefersReducedMotion() ? 0 : DPR_TRANSITION_MS;
        animateScrollTop(scrollEl, clamped, duration);
      };

      const centerSidebarOnCurrent = () => {
        // 优先跟随 Docsify 的“active”状态（这才是你看到的选中项）
        const nav = document.querySelector('.sidebar-nav');
        if (nav) {
          const activeLi = nav.querySelector('li.active');
          const activeLink = nav.querySelector('a.active');
          const el = activeLi || activeLink;
          if (el) {
            const href = (activeLink && activeLink.getAttribute('href')) || '';
            // 如果拿得到 href，就走 href 去重；否则用一个稳定的占位 key
            const key = href ? normalizeHref(href) : '__active__';
            if (key && key === DPR_SIDEBAR_CENTER_STATE.lastHref) return;

            const scrollEl = getSidebarScrollEl();
            if (!scrollEl) return;

            const scrollRect = scrollEl.getBoundingClientRect();
            const itemRect = el.getBoundingClientRect();

            const currentTop = scrollEl.scrollTop;
            const deltaTop = itemRect.top - scrollRect.top;
            const targetTop =
              currentTop +
              deltaTop -
              (scrollRect.height / 2 - itemRect.height / 2);

            const clamped = Math.max(
              0,
              Math.min(targetTop, scrollEl.scrollHeight - scrollEl.clientHeight),
            );

            DPR_SIDEBAR_CENTER_STATE.lastTs = Date.now();
            DPR_SIDEBAR_CENTER_STATE.lastHref = key;

            const duration = prefersReducedMotion() ? 0 : DPR_TRANSITION_MS;
            animateScrollTop(scrollEl, clamped, duration);
            return;
          }
        }

        // 兜底：按当前路由 href 匹配
        const href = DPR_NAV_STATE.currentHref || '';
        if (!href) return;
        centerSidebarOnHref(href);
      };

      const shouldIgnoreKeyNav = (event) => {
        if (!event) return true;
        if (event.defaultPrevented) return true;
        if (event.metaKey || event.ctrlKey || event.altKey) return true;
        const target = event.target;
        if (!target) return false;
        const tag = (target.tagName || '').toUpperCase();
        if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
        if (target.isContentEditable) return true;
        return false;
      };

      const navigateByDelta = (delta) => {
        const list = DPR_NAV_STATE.paperHrefs || [];
        if (!list.length) return;
        const now = Date.now();
        if (now - (DPR_NAV_STATE.lastNavTs || 0) < 450) return;
        DPR_NAV_STATE.lastNavTs = now;

        const current = DPR_NAV_STATE.currentHref;
        // 首页：右键/左滑（delta=+1）跳到最新一天第一篇
        if (!current) {
          if (delta > 0) {
            triggerPageNav(list[0], 'forward');
          }
          return;
        }

        const idx = list.indexOf(current);
        if (idx === -1) return;
        const nextIdx = idx + delta;
        if (nextIdx < 0 || nextIdx >= list.length) return;
        triggerPageNav(list[nextIdx], delta > 0 ? 'forward' : 'backward');
      };

      const prefersReducedMotion = () => {
        try {
          return (
            window.matchMedia &&
            window.matchMedia('(prefers-reduced-motion: reduce)').matches
          );
        } catch {
          return false;
        }
      };

      // 统一“sidebar 居中滚动”和“页面切换”的动画时长，确保观感一致
      const DPR_TRANSITION_MS = 320;
      try {
        document.documentElement.style.setProperty(
          '--dpr-transition-ms',
          `${DPR_TRANSITION_MS}ms`,
        );
      } catch {
        // ignore
      }

      const DPR_SIDEBAR_SCROLL_ANIM = {
        rafId: 0,
      };

      const easeInOutCubic = (t) => {
        return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
      };

      const animateScrollTop = (el, targetTop, durationMs) => {
        if (!el) return;

        try {
          if (DPR_SIDEBAR_SCROLL_ANIM.rafId) {
            cancelAnimationFrame(DPR_SIDEBAR_SCROLL_ANIM.rafId);
            DPR_SIDEBAR_SCROLL_ANIM.rafId = 0;
          }
        } catch {
          // ignore
        }

        const to = Math.max(
          0,
          Math.min(targetTop, el.scrollHeight - el.clientHeight),
        );
        const from = el.scrollTop;
        const delta = to - from;
        if (Math.abs(delta) < 1 || !durationMs) {
          el.scrollTop = to;
          return;
        }

        const start =
          (window.performance && performance.now && performance.now()) ||
          Date.now();
        const step = (now) => {
          const t = Math.min(1, (now - start) / durationMs);
          const p = easeInOutCubic(t);
          el.scrollTop = from + delta * p;
          if (t < 1) {
            DPR_SIDEBAR_SCROLL_ANIM.rafId = requestAnimationFrame(step);
          } else {
            DPR_SIDEBAR_SCROLL_ANIM.rafId = 0;
          }
        };
        DPR_SIDEBAR_SCROLL_ANIM.rafId = requestAnimationFrame(step);
      };

      const triggerPageNav = (href, direction) => {
        const target = normalizeHref(href);
        if (!target) return;

        // 先把 sidebar 的“选中高亮层”滑动到目标条目，和页面切换同步
        moveSidebarActiveIndicatorToHref(target, { animate: true });
        DPR_SIDEBAR_ACTIVE_INDICATOR.justMoved = true;

        // 通过左右键/滑动切换时：提前把 sidebar 滚到目标项附近，提升“跟手”观感
        if (DPR_NAV_STATE.lastNavSource !== 'click') {
          centerSidebarOnHref(target);
        }

        // 决定入场方向：forward => 新页从右进；backward => 新页从左进
        DPR_TRANSITION.pendingEnter =
          direction === 'backward' ? 'enter-from-left' : 'enter-from-right';

        if (prefersReducedMotion()) {
          window.location.hash = target;
          return;
        }

        const animEl = getPageAnimEl();
        if (!animEl) {
          window.location.hash = target;
          return;
        }

        const exitClass =
          direction === 'backward' ? 'dpr-page-exit-right' : 'dpr-page-exit-left';

        animEl.classList.add('dpr-page-exit', exitClass);
        // 等退场动画结束后再切换路由
        setTimeout(() => {
          window.location.hash = target;
        }, DPR_TRANSITION_MS);
      };

      const PREFETCH_STATE = {
        cache: new Map(),
      };

      const hrefToMdUrl = (href) => {
        const h = normalizeHref(href);
        const m = h.match(/^#\/(.+)$/);
        if (!m) return '';
        const file = m[1].replace(/\/$/, '') + '.md';
        return 'docs/' + file;
      };

      const prefetchHref = async (href) => {
        const url = hrefToMdUrl(href);
        if (!url) return;
        const key = url;
        const now = Date.now();
        const prev = PREFETCH_STATE.cache.get(key);
        if (prev && now - prev.ts < 5 * 60 * 1000) return; // 5 分钟内不重复拉取
        try {
          const res = await fetch(url, { cache: 'force-cache' });
          if (!res.ok) return;
          // 读一下 body，确保写入浏览器缓存（同时做内存缓存兜底）
          const text = await res.text();
          PREFETCH_STATE.cache.set(key, { ts: now, len: text.length });
        } catch {
          // ignore
        }
      };

      const prefetchAdjacent = () => {
        const list = DPR_NAV_STATE.paperHrefs || [];
        if (!list.length) return;
        const current = DPR_NAV_STATE.currentHref;
        if (!current) {
          // 首页：预取最新一天第一篇
          prefetchHref(list[0]);
          return;
        }
        const idx = list.indexOf(current);
        if (idx === -1) return;
        const prev = idx > 0 ? list[idx - 1] : '';
        const next = idx + 1 < list.length ? list[idx + 1] : '';
        if (prev) prefetchHref(prev);
        if (next) prefetchHref(next);
      };

      const ensureNavHandlers = () => {
        if (window.__dprNavBound) return;
        window.__dprNavBound = true;

        const toggleGoodForCurrent = () => {
          const current = DPR_NAV_STATE.currentHref || '';
          if (!current) return;
          const m = current.match(/^#\/(.+)$/);
          if (!m) return;
          const paperId = m[1];

          const state = loadReadState();
          const cur = state[paperId];
          // 空格：在 good 与 read 之间切换
          if (cur === 'good') {
            state[paperId] = 'read';
          } else {
            state[paperId] = 'good';
          }
          saveReadState(state);
          markSidebarReadState(null);
        };

        // 键盘：左右方向键
        window.addEventListener('keydown', (e) => {
          const key = e.key || '';
          if (shouldIgnoreKeyNav(e)) return;
          if (key === ' ') {
            // 空格键：切换“不错（绿色勾）”
            e.preventDefault();
            toggleGoodForCurrent();
            return;
          }
          if (key !== 'ArrowLeft' && key !== 'ArrowRight') return;
          // 只在当前页面聚焦时工作：浏览器已聚焦窗口即可
          e.preventDefault();
          DPR_NAV_STATE.lastNavSource = 'key';
          navigateByDelta(key === 'ArrowRight' ? +1 : -1);
        });

        // 点击论文链接也走同一套“整页切换”动效（避免只有滑动/方向键有动画）
        document.addEventListener('click', (e) => {
          try {
            if (!e || e.defaultPrevented) return;
            // 仅拦截普通左键点击，避免影响新标签页/复制链接等行为
            if (typeof e.button === 'number' && e.button !== 0) return;
            if (e.metaKey || e.ctrlKey || e.altKey || e.shiftKey) return;

            const link = e.target && e.target.closest ? e.target.closest('a[href]') : null;
            if (!link) return;
            const href = link.getAttribute('href') || '';
            if (!isPaperHref(href)) return;

            const target = normalizeHref(href);
            if (!target) return;
            if (target === (DPR_NAV_STATE.currentHref || '')) return;

            // 鼠标点击 sidebar：不触发“居中”逻辑
            DPR_NAV_STATE.lastNavSource = 'click';

            // 推断方向：按侧边栏顺序判断“前进/后退”
            let direction = 'forward';
            const list = DPR_NAV_STATE.paperHrefs || [];
            const cur = DPR_NAV_STATE.currentHref || '';
            if (list.length && cur) {
              const curIdx = list.indexOf(cur);
              const tgtIdx = list.indexOf(target);
              if (curIdx !== -1 && tgtIdx !== -1) {
                direction = tgtIdx < curIdx ? 'backward' : 'forward';
              }
            }

            // 只在论文页启用动效拦截，避免首页点击出现“无动画但有延迟”的体验
            if (document.body && document.body.classList.contains('dpr-paper-page') && !prefersReducedMotion()) {
              e.preventDefault();
              triggerPageNav(target, direction);
            }
          } catch {
            // ignore
          }
        });

        // 鼠标/触控板横向滚动：切换论文，并阻止浏览器的“整页滑动/回退动效”
        document.addEventListener(
          'wheel',
          (e) => {
            if (shouldIgnoreKeyNav(e)) return;
            const dx = e.deltaX || 0;
            const dy = e.deltaY || 0;
            if (Math.abs(dx) < 28) return;
            if (Math.abs(dx) < Math.abs(dy) * 1.2) return;
            e.preventDefault();
            // dx < 0：向左滑 => 下一篇
            // dx > 0：向右滑 => 上一篇
            DPR_NAV_STATE.lastNavSource = 'wheel';
            navigateByDelta(dx < 0 ? +1 : -1);
          },
          { passive: false },
        );

        // 触摸滑动：左右切换
        let startX = 0;
        let startY = 0;
        let startAt = 0;
        let lockHorizontal = false;
        const threshold = 60;

        const onTouchStart = (e) => {
          const t = e.touches && e.touches[0];
          if (!t) return;
          startX = t.clientX;
          startY = t.clientY;
          startAt = Date.now();
          lockHorizontal = false;
        };

        const onTouchMove = (e) => {
          const t = e.touches && e.touches[0];
          if (!t) return;
          const dx = t.clientX - startX;
          const dy = t.clientY - startY;
          if (Math.abs(dx) < 18) return;
          if (Math.abs(dx) > Math.abs(dy) * 1.2) {
            lockHorizontal = true;
          }
          if (lockHorizontal) {
            // 阻止浏览器的横向滑动/回退动效，让切换更“丝滑”
            if (e.cancelable) {
              e.preventDefault();
            }
          }
        };

        const onTouchEnd = (e) => {
          const t = e.changedTouches && e.changedTouches[0];
          if (!t) return;
          const dx = t.clientX - startX;
          const dy = t.clientY - startY;
          const dt = Date.now() - startAt;
          // 排除长按、轻微滑动、明显上下滚动
          if (dt > 900) return;
          if (Math.abs(dx) < threshold) return;
          if (Math.abs(dx) < Math.abs(dy) * 1.2) return;
          // dx < 0：向左滑 => 下一篇（相当于 ArrowRight）
          // dx > 0：向右滑 => 上一篇（相当于 ArrowLeft）
          DPR_NAV_STATE.lastNavSource = 'swipe';
          navigateByDelta(dx < 0 ? +1 : -1);
        };

        document.addEventListener('touchstart', onTouchStart, { passive: true });
        document.addEventListener('touchmove', onTouchMove, { passive: false });
        document.addEventListener('touchend', onTouchEnd, { passive: true });
      };

      // --- Docsify 生命周期钩子 ---
      hook.doneEach(function () {
        // 当前路由对应的“论文 ID”（简单用文件名去掉 .md）
        const paperId = getPaperId();
        const routePath = vm.route && vm.route.path ? vm.route.path : '';
        const lowerId = (paperId || '').toLowerCase();

        // 首页（如 README.md 或根路径）不展示研讨区，只做数学渲染和 Zotero 元数据更新
        const isHomePage =
          !paperId ||
          lowerId === 'readme' ||
          routePath === '/' ||
          routePath === '';
        const file = vm && vm.route ? vm.route.file : '';
        const isPaperPage = isPaperRouteFile(file);

        // A. 对正文区域进行一次全局公式渲染（支持 $...$ / $$...$$）
        const mainContent = document.querySelector('.markdown-section');
        if (mainContent) {
          // 先创建正文包装层，避免后续切页动画影响聊天浮层
          const root = isPaperPage ? ensurePageContentRoot() : null;
          renderMathInEl(root || mainContent);
        }

        // 论文页标题条排版（只对 docs/YYYYMM/DD/*.md 生效）
        applyPaperTitleBar();

        // 论文页左右切换：更新导航列表并绑定事件（只绑定一次）
        updateNavState();
        ensureNavHandlers();
        // 预取相邻论文的 Markdown（利用浏览器 cache，让切换更丝滑）
        prefetchAdjacent();

        // 页面入场动画：根据上一跳的方向做滑入
        const animEl = getPageAnimEl();
        if (animEl) {
          // 清理上一次退场残留（防止极端情况下没清掉）
          animEl.classList.remove(
            'dpr-page-exit',
            'dpr-page-exit-left',
            'dpr-page-exit-right',
          );
          const enter = DPR_TRANSITION.pendingEnter;
          DPR_TRANSITION.pendingEnter = '';
          if (enter && !prefersReducedMotion()) {
            animEl.classList.add('dpr-page-enter', enter);
            requestAnimationFrame(() => {
              // 触发 transition 到“静止态”
              animEl.classList.add('dpr-page-enter-active');
              setTimeout(() => {
                animEl.classList.remove(
                  'dpr-page-enter',
                  'dpr-page-enter-active',
                  'enter-from-left',
                  'enter-from-right',
                );
              }, DPR_TRANSITION_MS + 40);
            });
          }
        }

        if (!isHomePage && window.PrivateDiscussionChat) {
          window.PrivateDiscussionChat.initForPage(paperId);
        }

        // ----------------------------------------------------
        // E. 小屏点击侧边栏条目后自动收起
        // ----------------------------------------------------
        setupMobileSidebarAutoCloseOnItemClick();

        // ----------------------------------------------------
        // F. 侧边栏按日期折叠
        // ----------------------------------------------------
        setupCollapsibleSidebarByDay();

        // ----------------------------------------------------
        // G. 侧边栏已阅读论文状态高亮
        // ----------------------------------------------------
        if (!isHomePage && paperId) {
          markSidebarReadState(paperId);
        } else {
          // 首页也需要应用已有的“已读高亮”，但不新增记录
          markSidebarReadState(null);
        }

        // 让滑动高亮层跟随当前 active 项（点击、路由变化后会更新 active 类）
        try {
          const nav = document.querySelector('.sidebar-nav');
          if (nav) {
            const activeLi = nav.querySelector('li.active.sidebar-paper-item');
            if (activeLi) {
              // 路由加载完成后：直接“贴齐”到实际 active 位置，避免出现二次滑动
              moveSidebarActiveIndicatorToEl(activeLi, { animate: false });
            }
          }
        } catch {
          // ignore
        } finally {
          DPR_SIDEBAR_ACTIVE_INDICATOR.justMoved = false;
        }

        // 自动把当前论文在 sidebar 中滚动到居中位置，便于连续阅读
        if (DPR_NAV_STATE.lastNavSource !== 'click') {
          requestAnimationFrame(() => {
            requestAnimationFrame(() => {
              centerSidebarOnCurrent();
            });
          });
        }

        // 本次 doneEach 的来源只用于控制“是否居中”，用完即清理
        DPR_NAV_STATE.lastNavSource = '';

        // ----------------------------------------------------
        // H. Zotero 元数据注入逻辑 (带延时和唤醒)
        // ----------------------------------------------------
        setTimeout(() => {
          updateZoteroMetaFromPage(paperId, vm.route.file);
        }, 1); // 延迟执行，等待 DOM 渲染完毕
      });
      // ----------------------------------------------------
      // I. 响应式侧边栏：窄屏首次加载时模拟点击按钮自动折叠一次
      // ----------------------------------------------------
      const SIDEBAR_AUTO_COLLAPSE_WIDTH = 1024;

      const autoCollapseOnInitForNarrowScreen = () => {
        const windowWidth =
          window.innerWidth || document.documentElement.clientWidth || 0;
        if (windowWidth >= SIDEBAR_AUTO_COLLAPSE_WIDTH) return;

        const body = document.body;
        // 已经是关闭状态就不再触发，避免反向展开
        if (body.classList && body.classList.contains('close')) return;

        const toggleBtn = document.querySelector('.sidebar-toggle');
        if (!toggleBtn) return;

        // 使用原生 click，让 Docsify 自己处理 close / transform 等细节
        toggleBtn.click();
      };

      // 初始化时执行一次
      autoCollapseOnInitForNarrowScreen();    },
  ],
};
