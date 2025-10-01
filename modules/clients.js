// Clients module: company management
// Keeps state, realtime subscription, rendering, and modal handlers

let _deps = null;
let _clients = [];
let _unsub = null;
let _assignments = [];
let _filterInitDone = false;
let _customFilterWired = false;

function byName(a, b) {
  // Prefer company name for sorting; fallback to person/name last
  const an = String(a?.company || a?.companyName || a?.businessName || a?.name || '').toLowerCase();
  const bn = String(b?.company || b?.companyName || b?.businessName || b?.name || '').toLowerCase();
  return an.localeCompare(bn);
}

function getClientLabel(c) {
  // Prefer company-style fields for labels in dropdowns; fallback to name
  const n = c?.company || c?.companyName || c?.businessName || c?.name || '';
  return n || '(unnamed)';
}

function getContactPerson(c) {
  return (
    c?.contactPerson ||
    c?.contactName ||
    c?.contact ||
    c?.person ||
    c?.companyContact ||
    c?.name || // treat generic "name" as the contact person's name if provided
    '-'
  );
}

export function initClients(deps) {
  _deps = deps; // { db, collection, query, onSnapshot, addDoc, serverTimestamp, showToast, cleanData, getAssignments }

  // Wire UI events
  const openClientModalBtn = document.getElementById('openClientModalBtn');
  if (openClientModalBtn) openClientModalBtn.addEventListener('click', () => openClientModal());
  const clientForm = document.getElementById('clientForm');
  if (clientForm) clientForm.addEventListener('submit', handleClientFormSubmit);

  // Expose for inline onclick in HTML
  window.closeClientModal = closeClientModal;
  window.openClientModal = openClientModal;

  // Initialize custom filter dropdown UI
  try { initCustomClientsFilter(); } catch {}
  // Initial render to show empty state even before first snapshot
  try { renderClientsTable(); } catch {}

  // Track assignments to compute Monthly per client
  try {
    const getter = deps && deps.getAssignments;
    _assignments = typeof getter === 'function' ? (getter() || []) : [];
  } catch { _assignments = []; }
  document.addEventListener('assignments:updated', (e) => {
    try { _assignments = Array.isArray(e.detail) ? e.detail.slice() : []; } catch { _assignments = []; }
    try { renderClientsTable(); } catch {}
  });

  // Ensure the custom dropdown handlers are wired once
  try { wireCustomClientsFilter(); } catch {}
}

export function subscribeClients() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot, showToast } = _deps;
  if (_unsub) _unsub();
  const q = query(collection(db, 'customers'));
  _unsub = onSnapshot(
    q,
    (snapshot) => {
  _clients = snapshot.docs.map(d => ({ id: d.id, ...d.data() }));
  // Rebuild the Clients tab custom filter options on live updates
  try { rebuildCustomClientsFilter(true); } catch {}
      renderClientsTable();
      // Notify listeners that clients data updated (for Client Billing and others)
      try {
        const evt = new CustomEvent('clients:updated', { detail: _clients.slice() });
        document.dispatchEvent(evt);
      } catch {}
    },
    (err) => {
      console.error('Error loading clients:', err);
      showToast && showToast('Error loading clients', 'error');
    }
  );
}

export function stopClients() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function getClients() {
  return _clients;
}

export function renderClientsTable() {
  const tbody = document.getElementById('clientsTableBody');
  const empty = document.getElementById('clientsEmptyState');
  if (!tbody || !empty) return;
  // Prefer controller-provided selection to decouple from DOM readiness
  let filterId = window.__clientsFilterId || '';
  if (!filterId) { try { window.__clientsFilterId = '*'; } catch {} filterId = '*'; }
  if (filterId === '*') filterId = '';
  if (!_clients.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmtCurrency = (n) => `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const today = new Date();
  const todayYmd = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
  const isActiveInMonth = (start, end, ym) => {
    // Active if start <= endOfMonth and (no end or end >= startOfMonth)
    const y = Number(ym.slice(0,4));
    const m = Number(ym.slice(5,7)) - 1;
    const startOfMonth = new Date(y, m, 1);
    const endOfMonth = new Date(y, m + 1, 0);
    const s = start ? new Date(start) : null;
    const e = end ? new Date(end) : null;
    const sOk = !s || s <= endOfMonth;
    const eOk = !e || e >= startOfMonth;
    return sOk && eOk;
  };
  const currentYm = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}`;
  const monthlyForClient = (clientId) => {
    try {
      const list = Array.isArray(_assignments) ? _assignments : [];
      let sum = 0;
      for (const a of list) {
        if (!a || a.clientId !== clientId) continue;
        const rt = String(a.rateType||'').toLowerCase();
        const rate = Number(a.rate||0) || 0;
        if (rt === 'monthly' && rate) {
          if (isActiveInMonth(a.startDate, a.endDate, currentYm)) sum += rate;
        }
      }
      return sum;
    } catch { return 0; }
  };
  const assignedCountForClient = (clientId) => {
    try {
      const list = Array.isArray(_assignments) ? _assignments : [];
      let cnt = 0;
      for (const a of list) {
        if (!a || a.clientId !== clientId) continue;
        if (isActiveInMonth(a.startDate, a.endDate, currentYm)) cnt += 1;
      }
      return cnt;
    } catch { return 0; }
  };
  const rows = _clients
    .slice()
    .filter(c => !filterId || c.id === filterId)
    .sort(byName)
    .map(c => {
      const cells = [
        // Company column: use only company-style fields; do NOT fall back to person name
        `<td class="px-4 py-2 font-semibold text-gray-900">${escapeHtml(c.company || c.companyName || c.businessName || '')}</td>`,
        `<td class="px-4 py-2">${escapeHtml(c.email || '')}</td>`,
        `<td class="px-4 py-2">${escapeHtml(c.phone || '-')}</td>`,
        `<td class="px-4 py-2">${escapeHtml(getContactPerson(c))}</td>`,
        `<td class="px-4 py-2 text-right">${assignedCountForClient(c.id)}</td>`,
        `<td class="px-4 py-2 text-right">${fmtCurrency(monthlyForClient(c.id))}</td>`,
        `<td class="px-4 py-2">${escapeHtml(c.address || '-')}</td>`,
      ];
      return `<tr class="hover:bg-gray-50">${cells.join('')}</tr>`;
    }).join('');
  tbody.innerHTML = rows;
  if (!rows) { empty.classList.remove('hidden'); } else { empty.classList.add('hidden'); }
}

// Clients filter dropdown — fully rebuilt
// Custom Clients filter dropdown — improved UI
function initCustomClientsFilter() {
  const btn = document.getElementById('clientsFilterBtn');
  const menu = document.getElementById('clientsFilterMenu');
  const txt = document.getElementById('clientsFilterBtnText');
  if (!btn || !menu || !txt) return;
  if (!_filterInitDone) {
    txt.textContent = 'Loading clients…';
    menu.innerHTML = '';
    _filterInitDone = true;
  }
}

function wireCustomClientsFilter() {
  if (_customFilterWired) return;
  const root = document.getElementById('clientsFilterCustom');
  const btn = document.getElementById('clientsFilterBtn');
  const menu = document.getElementById('clientsFilterMenu');
  const txt = document.getElementById('clientsFilterBtnText');
  if (!root || !btn || !menu || !txt) return;
  const openMenu = () => { menu.classList.remove('hidden'); btn.setAttribute('aria-expanded','true'); };
  const closeMenu = () => { menu.classList.add('hidden'); btn.setAttribute('aria-expanded','false'); };
  const isOpen = () => !menu.classList.contains('hidden');
  btn.addEventListener('click', (e) => { e.preventDefault(); isOpen() ? closeMenu() : openMenu(); });
  // Close on outside click
  document.addEventListener('mousedown', (e) => { if (!root.contains(e.target)) closeMenu(); });
  // Keyboard navigation
  btn.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); openMenu(); const first = menu.querySelector('[data-opt]'); first?.focus(); }
  });
  menu.addEventListener('keydown', (e) => {
    const items = Array.from(menu.querySelectorAll('[data-opt]'));
    const idx = items.indexOf(document.activeElement);
    if (e.key === 'ArrowDown') { e.preventDefault(); (items[idx+1]||items[0])?.focus(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); (items[idx-1]||items[items.length-1])?.focus(); }
    else if (e.key === 'Escape') { e.preventDefault(); closeMenu(); btn.focus(); }
    else if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); document.activeElement?.click?.(); }
  });
  _customFilterWired = true;
}

function rebuildCustomClientsFilter(preserveSelection = true) {
  const btn = document.getElementById('clientsFilterBtn');
  const menu = document.getElementById('clientsFilterMenu');
  const txt = document.getElementById('clientsFilterBtnText');
  if (!btn || !menu || !txt) return;
  const prev = preserveSelection ? (window.__clientsFilterId || '*') : '*';
  const items = _clients.slice().sort(byName);
  if (!items.length) {
    txt.textContent = 'No clients available';
    menu.innerHTML = '';
    btn.disabled = true;
    return;
  }
  btn.disabled = false;
  const mkItem = (value, label) => `<button type="button" data-opt data-value="${value}" class="w-full text-left px-3 py-2 hover:bg-gray-100 focus:bg-gray-100 focus:outline-none">${escapeHtml(label)}</button>`;
  const html = [mkItem('*', 'All clients…')].concat(items.map(c => mkItem(c.id, getClientLabel(c)))).join('');
  menu.innerHTML = html;
  const selectedLabel = (prev === '*') ? 'All clients…' : (getClientLabel(items.find(i => i.id === prev) || {}) || 'Select a client');
  txt.textContent = selectedLabel;
  // Wire item clicks (rebind each rebuild)
  Array.from(menu.querySelectorAll('[data-opt]')).forEach(el => {
    el.addEventListener('click', () => {
      const val = el.getAttribute('data-value') || '*';
      window.__clientsFilterId = val;
      txt.textContent = val === '*' ? 'All clients…' : el.textContent;
      renderClientsTable();
      menu.classList.add('hidden');
    });
  });
}

export function forceRebuildClientsFilter() {
  try { rebuildCustomClientsFilter(true); } catch {}
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function openClientModal() {
  const modal = document.getElementById('clientModal');
  const form = document.getElementById('clientForm');
  if (form) form.reset();
  if (modal) modal.classList.add('show');
}

function closeClientModal() {
  const modal = document.getElementById('clientModal');
  if (modal) modal.classList.remove('show');
}

async function handleClientFormSubmit(e) {
  e.preventDefault();
  const { db, collection, addDoc, showToast, cleanData } = _deps || {};
  const name = document.getElementById('clientName')?.value.trim();
  const email = document.getElementById('clientEmail')?.value.trim();
  const phone = document.getElementById('clientPhone')?.value.trim();
  const company = document.getElementById('clientCompany')?.value.trim();
  const address = document.getElementById('clientAddress')?.value.trim();
  // Require at least a company for the Company column; treat name as optional contact person
  if (!company || !email || !address) {
    showToast && showToast('Company, Email, and Address are required', 'warning');
    return;
  }
  const payload = cleanData ? cleanData({
    // Persist both when provided; UI will use company for Company column and name for Contact Person
    name,
    email,
    phone,
    company,
    address,
    createdAt: new Date().toISOString(),
  }) : { name, email, phone, company, createdAt: new Date().toISOString() };
  try {
    const ref = await addDoc(collection(db, 'customers'), payload);
    showToast && showToast('Client added', 'success');
    // Optimistically update local state so the new client shows immediately
    try {
      const exists = _clients.some(c => c.id === ref.id);
      if (!exists) {
        _clients = _clients.concat([{ id: ref.id, ...payload }]);
        renderClientsTable();
        try { rebuildCustomClientsFilter(true); } catch {}
      }
    } catch {}
    closeClientModal();
  } catch (err) {
    console.error('Add client failed', err);
    showToast && showToast('Failed to add client', 'error');
  }
}
