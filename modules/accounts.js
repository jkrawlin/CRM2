// Accounts module: simple chart-of-accounts CRUD (name, type, opening balance)

let _deps = null;
let _accounts = [];
let _unsub = null;

export function initAccounts(deps) {
  _deps = deps; // { db, collection, query, onSnapshot, addDoc, showToast, cleanData }
  try { renderAccountsTable(); } catch {}
}

export function subscribeAccounts() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot, showToast } = _deps;
  if (_unsub) _unsub();
  const q = query(collection(db, 'accounts'));
  _unsub = onSnapshot(q, (snap) => {
    _accounts = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    renderAccountsTable();
    try { window.__setAccountsShadow && window.__setAccountsShadow(_accounts); } catch {}
    try { window.dispatchEvent(new CustomEvent('accounts:updated', { detail: _accounts })); } catch {}
  }, (err) => {
    console.error('Error loading accounts', err);
    showToast && showToast('Error loading accounts', 'error');
  });
}

export function stopAccounts() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function renderAccountsTable() {
  const tbody = document.getElementById('accountsTableBody');
  const empty = document.getElementById('accountsEmptyState');
  if (!tbody || !empty) return;
  if (!_accounts.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  tbody.innerHTML = _accounts
    .slice()
    .sort((a,b)=> (a.name||'').localeCompare(b.name||''))
    .map(a => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2 font-semibold text-gray-900">${a.name || ''}</td>
        <td class="px-4 py-2">${a.type || ''}</td>
        <td class="px-4 py-2 text-right">${fmt(a.opening || 0)}</td>
      </tr>
    `).join('');
}

