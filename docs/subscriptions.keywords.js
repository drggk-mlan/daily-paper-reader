// 订阅关键词管理模块
// 负责：渲染关键词列表、增加/删除关键词

window.SubscriptionsKeywords = (function () {
  let keywordsListEl = null;
  let keywordInput = null;
  let keywordAliasInput = null;
  let addBtn = null;
  let msgEl = null;
  let reloadAll = null;

  // 简单的 HTML 转义，避免 XSS
  const escapeHtml = (str) => {
    if (!str) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  };

  const render = (items) => {
    if (!keywordsListEl) return;
    if (!items || !items.length) {
      keywordsListEl.innerHTML =
        '<div style="color:#999;">暂无关键词订阅，可在下方新增。</div>';
      return;
    }
    keywordsListEl.innerHTML = '';
    items.forEach((item) => {
      const row = document.createElement('div');
      row.style.display = 'flex';
      row.style.alignItems = 'center';
      row.style.justifyContent = 'space-between';
      row.style.marginBottom = '2px';
      const alias = item.alias || '';
      row.innerHTML = `
        <span>${
          alias
            ? '<span class="tag-label tag-green">' +
              escapeHtml(alias) +
              '</span>'
            : ''
        }${escapeHtml(item.keyword || '')}</span>
        <button data-id="${
          item.id
        }" class="arxiv-keyword-del" style="border:none;background:none;color:#c00;font-size:11px;cursor:pointer;">删除</button>
      `;
      keywordsListEl.appendChild(row);
    });

    keywordsListEl.querySelectorAll('.arxiv-keyword-del').forEach((btn) => {
      if (btn._bound) return;
      btn._bound = true;
      btn.addEventListener('click', async () => {
        const id = btn.getAttribute('data-id');
        if (!id) return;
        try {
          await fetch(
            `${window.API_BASE_URL}/api/subscriptions/keyword/${id}`,
            { method: 'DELETE' },
          );
          if (typeof reloadAll === 'function') reloadAll();
        } catch (err) {
          console.error(err);
        }
      });
    });
  };

  const addKeyword = async () => {
    if (!keywordInput || !keywordAliasInput) return;
    const keyword = (keywordInput.value || '').trim();
    const alias = (keywordAliasInput.value || '').trim();
    if (!keyword) {
      if (msgEl) {
        msgEl.textContent = '关键词不能为空';
        msgEl.style.color = '#c00';
      }
      return;
    }
    if (!alias) {
      if (msgEl) {
        msgEl.textContent = '备注为必填项';
        msgEl.style.color = '#c00';
      }
      return;
    }

    try {
      const res = await fetch(
        `${window.API_BASE_URL}/api/subscriptions/keyword`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ keyword, alias }),
        },
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        if (msgEl) {
          msgEl.textContent = data.detail || '新增关键词失败';
          msgEl.style.color = '#c00';
        }
      } else {
        if (msgEl) {
          msgEl.textContent = '关键词已新增。';
          msgEl.style.color = '#080';
        }
        keywordInput.value = '';
        keywordAliasInput.value = '';
        if (typeof reloadAll === 'function') reloadAll();
      }
    } catch (e) {
      console.error(e);
      if (msgEl) {
        msgEl.textContent = '新增关键词失败，请稍后重试';
        msgEl.style.color = '#c00';
      }
    }
  };

  const attach = (context) => {
    keywordsListEl = context.keywordsListEl || null;
    keywordInput = context.keywordInput || null;
    keywordAliasInput = context.keywordAliasInput || null;
    addBtn = context.keywordAddBtn || null;
    msgEl = context.msgEl || null;
    reloadAll = context.reloadAll || null;

    if (addBtn && !addBtn._bound) {
      addBtn._bound = true;
      addBtn.addEventListener('click', addKeyword);
    }
    if (keywordAliasInput && !keywordAliasInput._bound) {
      keywordAliasInput._bound = true;
      keywordAliasInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
          e.preventDefault();
          addKeyword();
        }
      });
    }
  };

  return {
    attach,
    render,
  };
})();

