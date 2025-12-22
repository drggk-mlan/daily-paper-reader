// 订阅 Zotero 管理模块
// 负责：渲染 Zotero 列表、测试账号、增加/删除 Zotero 订阅

window.SubscriptionsZotero = (function () {
  let zoteroListEl = null;
  let zoteroIdInput = null;
  let zoteroKeyInput = null;
  let zoteroAliasInput = null;
  let zoteroTestBtn = null;
  let zoteroAddBtn = null;
  let msgEl = null;
  let reloadAll = null;
  let zoteroVerified = false;

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
    if (!zoteroListEl) return;
    if (!items || !items.length) {
      zoteroListEl.innerHTML =
        '<div style="color:#999;">暂无 Zotero 订阅，可在下方新增。</div>';
      return;
    }
    zoteroListEl.innerHTML = '';
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
            ? '<span class="tag-label tag-blue">' +
              escapeHtml(alias) +
              '</span>'
            : ''
        }${escapeHtml(item.zotero_id || '')}</span>
        <button data-id="${
          item.id
        }" class="zotero-del-btn" style="border:none;background:none;color:#c00;font-size:11px;cursor:pointer;">删除</button>
      `;
      zoteroListEl.appendChild(row);
    });

    zoteroListEl.querySelectorAll('.zotero-del-btn').forEach((btn) => {
      if (btn._bound) return;
      btn._bound = true;
      btn.addEventListener('click', async () => {
        const id = btn.getAttribute('data-id');
        if (!id) return;
        try {
          await fetch(
            `${window.API_BASE_URL}/api/subscriptions/zotero/${id}`,
            { method: 'DELETE' },
          );
          if (typeof reloadAll === 'function') reloadAll();
        } catch (err) {
          console.error(err);
        }
      });
    });
  };

  const addZotero = async () => {
    if (!zoteroIdInput || !zoteroKeyInput || !zoteroAliasInput) return;
    if (!zoteroVerified) {
      if (msgEl) {
        msgEl.textContent = '请先点击"测试"按钮验证 Zotero 账号';
        msgEl.style.color = '#c00';
      }
      return;
    }

    const zid = (zoteroIdInput.value || '').trim();
    const key = (zoteroKeyInput.value || '').trim();
    const alias = (zoteroAliasInput.value || '').trim();
    if (!zid || !key) {
      if (msgEl) {
        msgEl.textContent = 'Zotero ID 和 Key 不能为空';
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
        `${window.API_BASE_URL}/api/subscriptions/zotero`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ zotero_id: zid, api_key: key, alias }),
        },
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        if (msgEl) {
          msgEl.textContent = data.detail || '新增 Zotero 失败';
          msgEl.style.color = '#c00';
        }
      } else {
        if (msgEl) {
          msgEl.textContent = 'Zotero 已新增。';
          msgEl.style.color = '#080';
        }
        zoteroIdInput.value = '';
        zoteroKeyInput.value = '';
        zoteroAliasInput.value = '';
        zoteroVerified = false;
        if (zoteroTestBtn) {
          zoteroTestBtn.textContent = '测试';
          zoteroTestBtn.style.background = '';
          zoteroTestBtn.style.color = '';
        }
        if (typeof reloadAll === 'function') reloadAll();
      }
    } catch (e) {
      console.error(e);
      if (msgEl) {
        msgEl.textContent = '新增 Zotero 失败，请稍后重试';
        msgEl.style.color = '#c00';
      }
    }
  };

  const testZotero = async () => {
    if (!zoteroIdInput || !zoteroKeyInput || !zoteroTestBtn) return;
    const zid = (zoteroIdInput.value || '').trim();
    const key = (zoteroKeyInput.value || '').trim();

    if (!zid || !key) {
      if (msgEl) {
        msgEl.textContent = 'Zotero ID 和 API Key 不能为空';
        msgEl.style.color = '#c00';
      }
      return;
    }

    zoteroTestBtn.disabled = true;
    zoteroTestBtn.textContent = '测试中...';
    if (msgEl) {
      msgEl.textContent = '正在验证 Zotero 账号...';
      msgEl.style.color = '#666';
    }

    try {
      const url = `https://api.zotero.org/users/${encodeURIComponent(
        zid,
      )}/items?limit=1`;
      const res = await fetch(url, {
        method: 'GET',
        headers: {
          'Zotero-API-Key': key,
          'Zotero-API-Version': '3',
        },
      });

      if (res.status === 200) {
        zoteroVerified = true;
        zoteroTestBtn.textContent = '✓ 验证成功';
        zoteroTestBtn.style.background = '#4CAF50';
        zoteroTestBtn.style.color = '#fff';
        if (msgEl) {
          msgEl.textContent = 'Zotero 账号验证成功，可以新增了';
          msgEl.style.color = '#4CAF50';
        }
      } else if (res.status === 403) {
        zoteroVerified = false;
        zoteroTestBtn.textContent = '✗ 验证失败';
        zoteroTestBtn.style.background = '#f44336';
        zoteroTestBtn.style.color = '#fff';
        if (msgEl) {
          msgEl.textContent = 'API Key 无效或权限不足';
          msgEl.style.color = '#c00';
        }
      } else if (res.status === 404) {
        zoteroVerified = false;
        zoteroTestBtn.textContent = '✗ 验证失败';
        zoteroTestBtn.style.background = '#f44336';
        zoteroTestBtn.style.color = '#fff';
        if (msgEl) {
          msgEl.textContent = '用户 ID 不存在';
          msgEl.style.color = '#c00';
        }
      } else {
        zoteroVerified = false;
        zoteroTestBtn.textContent = '✗ 验证失败';
        zoteroTestBtn.style.background = '#f44336';
        zoteroTestBtn.style.color = '#fff';
        if (msgEl) {
          msgEl.textContent = `验证失败: HTTP ${res.status}`;
          msgEl.style.color = '#c00';
        }
      }
    } catch (e) {
      console.error('Zotero 验证错误:', e);
      zoteroVerified = false;
      zoteroTestBtn.textContent = '✗ 测试失败';
      zoteroTestBtn.style.background = '#f44336';
      zoteroTestBtn.style.color = '#fff';
      if (msgEl) {
        msgEl.textContent = '测试失败: ' + (e.message || '请检查网络连接');
        msgEl.style.color = '#c00';
      }
    } finally {
      zoteroTestBtn.disabled = false;
      // 一段时间后恢复按钮文案
      setTimeout(() => {
        if (!zoteroVerified && zoteroTestBtn) {
          zoteroTestBtn.textContent = '测试';
          zoteroTestBtn.style.background = '';
          zoteroTestBtn.style.color = '';
        }
      }, 3000);
    }
  };

  const attach = (context) => {
    zoteroListEl = context.zoteroListEl || null;
    zoteroIdInput = context.zoteroIdInput || null;
    zoteroKeyInput = context.zoteroKeyInput || null;
    zoteroAliasInput = context.zoteroAliasInput || null;
    zoteroTestBtn = context.zoteroTestBtn || null;
    zoteroAddBtn = context.zoteroAddBtn || null;
    msgEl = context.msgEl || null;
    reloadAll = context.reloadAll || null;

    if (zoteroAddBtn && !zoteroAddBtn._bound) {
      zoteroAddBtn._bound = true;
      zoteroAddBtn.addEventListener('click', addZotero);
    }

    if (zoteroTestBtn && !zoteroTestBtn._bound) {
      zoteroTestBtn._bound = true;
      zoteroTestBtn.addEventListener('click', testZotero);
    }

    // 输入变化时重置验证状态
    if (zoteroIdInput && !zoteroIdInput._bound) {
      zoteroIdInput._bound = true;
      zoteroIdInput.addEventListener('input', () => {
        if (zoteroVerified && zoteroTestBtn) {
          zoteroVerified = false;
          zoteroTestBtn.textContent = '测试';
          zoteroTestBtn.style.background = '';
          zoteroTestBtn.style.color = '';
        }
      });
    }
    if (zoteroKeyInput && !zoteroKeyInput._bound) {
      zoteroKeyInput._bound = true;
      zoteroKeyInput.addEventListener('input', () => {
        if (zoteroVerified && zoteroTestBtn) {
          zoteroVerified = false;
          zoteroTestBtn.textContent = '测试';
          zoteroTestBtn.style.background = '';
          zoteroTestBtn.style.color = '';
        }
      });
    }
    if (zoteroAliasInput && !zoteroAliasInput._bound) {
      zoteroAliasInput._bound = true;
      zoteroAliasInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
          e.preventDefault();
          addZotero();
        }
      });
    }
  };

  return {
    attach,
    render,
  };
})();

