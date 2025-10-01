// Clients module: company management
// Keeps state, realtime subscription, rendering, and modal handlers

let _deps = null;
let _clients = [];
let _unsub = null;
let _assignments = [];
let _filterInitDone = false;
let _customFilterWired = false;
let _editingClientId = null;

function byName(a, b) {
  const an = String(a?.name || a?.company || '').toLowerCase();
  const bn = String(b?.name || b?.company || '').toLowerCase();
  return an.localeCompare(bn);
}

function getClientLabel(c) {
  const n = c?.name || c?.company || '';
  return n || '(unnamed)';
}

export function initClients(deps) {
  _deps = deps; // { db, collection, query, onSnapshot, addDoc, serverTimestamp, showToast, cleanData, getAssignments }
  // Wire Add Client button and modal
  window.openClientModal = openClientModal;
  window.closeClientModal = closeClientModal;
  const addBtn = document.getElementById('openClientModalBtn');
  if (addBtn && !addBtn.__wired) {
    addBtn.addEventListener('click', (e) => { e.preventDefault(); _editingClientId = null; setClientModalMode('add'); openClientModal(); });
    addBtn.__wired = true;
  }
  const form = document.getElementById('clientForm');
  if (form && !form.__wired) { form.addEventListener('submit', handleClientFormSubmit); form.__wired = true; }
  // Simple select filter change triggers re-render
  const sel = document.getElementById('clientsFilterSelect');
  if (sel && !sel.__wired) { sel.addEventListener('change', () => { try { renderClientsTable(); } catch {} }); sel.__wired = true; }

  // Track assignments to compute Monthly per client
  try {
    const getter = deps && deps.getAssignments;
    _assignments = typeof getter === 'function' ? (getter() || []) : [];
  } catch { _assignments = []; }
  document.addEventListener('assignments:updated', (e) => {
    try { _assignments = Array.isArray(e.detail) ? e.detail.slice() : []; } catch { _assignments = []; }
    try { renderClientsTable(); } catch {}
  });

  // Do not wire custom dropdown in header-only view
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
      // In header-only view, ensure rows are cleared if something tries to render
      try { renderClientsTable(); } catch {}
      // Broadcast update for any listeners (filters, dropdowns, etc.)
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
  const currentYm = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}`;
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
  const monthlyForClient = (clientId) => {
    // Sum monthly assignment rates for active assignments in current month
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
  const activeCountForClient = (clientId) => {
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
    .map(c => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2 font-semibold text-gray-900">${escapeHtml(c.name || c.company || '')}</td>
        <td class="px-4 py-2">${escapeHtml(c.email || '')}</td>
        <td class="px-4 py-2">${escapeHtml(c.phone || '-')}</td>
        <td class="px-4 py-2">${escapeHtml(c.company || '-')}</td>
  <td class="px-4 py-2 text-right">${fmtCurrency((c.monthly!=null && c.monthly!==undefined && Number(c.monthly)>0) ? Number(c.monthly) : monthlyForClient(c.id))}</td>
        <td class="px-4 py-2 text-center">${activeCountForClient(c.id)}</td>
        <td class="px-4 py-2">${escapeHtml(c.address || '-')}</td>
        <td class="px-4 py-2 text-center">
          <div class="inline-flex gap-2">
            <button class="btn btn-secondary btn-sm" data-view-client="${c.id}"><i class="fas fa-eye"></i></button>
            <button class="btn btn-secondary btn-sm" data-assign-client="${c.id}"><i class="fas fa-people-arrows"></i></button>
            <button class="btn btn-primary btn-sm" data-edit-client="${c.id}"><i class="fas fa-pen"></i></button>
          </div>
        </td>
      </tr>
    `).join('');
  tbody.innerHTML = rows;
  if (!rows) { empty.classList.remove('hidden'); } else { empty.classList.add('hidden'); }
  // Delegated actions (wire once)
  if (!document.__clientsActionsWired) {
    document.addEventListener('click', (e) => {
      const btnAssign = e.target.closest?.('[data-assign-client]');
      if (btnAssign) {
        e.preventDefault();
        const id = btnAssign.getAttribute('data-assign-client');
        try {
          if (typeof window.openAssignmentModal === 'function') {
            window.openAssignmentModal();
            setTimeout(() => { try { const sel = document.getElementById('asClient'); if (sel) { sel.value = id; sel.dispatchEvent(new Event('change')); } const emp = document.getElementById('asEmployee'); if (emp) { emp.value=''; emp.dataset.empId=''; emp.dataset.empWhich=''; const h=document.getElementById('asEmployeeNameHelper'); if (h) h.textContent=''; } } catch {} }, 50);
          }
        } catch {}
        return;
      }
      const btnEdit = e.target.closest?.('[data-edit-client]');
      const btnView = e.target.closest?.('[data-view-client]');
      if (btnView) {
        e.preventDefault();
        const id = btnView.getAttribute('data-view-client');
        openClientView(id);
        return;
      }
      if (btnEdit) {
        e.preventDefault();
        const id = btnEdit.getAttribute('data-edit-client');
        openClientEditFlow(id);
      }
    });
    document.__clientsActionsWired = true;
  }
}

function openClientView(id) {
  try {
    const c = _clients.find(x => x.id === id);
    if (!c) return;
    const setText = (elId, val) => { const el = document.getElementById(elId); if (el) el.textContent = val || '—'; };
    setText('cvName', c.name || '—');
    setText('cvCompany', c.company || '—');
    setText('cvEmail', c.email || '—');
    setText('cvPhone', c.phone || '—');
    setText('cvAddress', c.address || '—');
    try { setText('cvCreatedAt', c.createdAt ? new Date(c.createdAt).toLocaleString() : '—'); } catch { setText('cvCreatedAt', '—'); }
    // Compute monthly and assigned
    const today = new Date();
    const currentYm = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}`;
    const isActiveInMonth = (start, end, ym) => {
      try {
        const y = Number(ym.slice(0,4));
        const m = Number(ym.slice(5,7)) - 1;
        const startOfMonth = new Date(y, m, 1);
        const endOfMonth = new Date(y, m + 1, 0);
        const s = start ? new Date(start) : null;
        const e = end ? new Date(end) : null;
        const sOk = !s || s <= endOfMonth;
        const eOk = !e || e >= startOfMonth;
        return sOk && eOk;
      } catch { return false; }
    };
    let monthly = 0, assigned = 0;
    const list = Array.isArray(_assignments) ? _assignments : [];
    for (const a of list) {
      if (!a || a.clientId !== c.id) continue;
      if (isActiveInMonth(a.startDate, a.endDate, currentYm)) {
        assigned += 1;
        if (String(a.rateType||'').toLowerCase()==='monthly') monthly += Number(a.rate||0)||0;
      }
    }
    // If client has fixed monthly override, prefer it
    if (c.monthly!=null && c.monthly!==undefined && Number(c.monthly) > 0) {
      monthly = Number(c.monthly);
    }
    const fmt = (n)=>`$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
    setText('cvMonthly', fmt(monthly));
    setText('cvAssigned', String(assigned));
    // Assignments table
    const body = document.getElementById('cvAssignmentsBody');
    const empty = document.getElementById('cvAssignmentsEmpty');
    const tbl = document.getElementById('cvAssignmentsTable');
    if (body) body.innerHTML = '';
    const rows = list.filter(a => a.clientId===c.id && isActiveInMonth(a.startDate, a.endDate, currentYm))
      .sort((a,b)=> (a.employeeName||'').localeCompare(b.employeeName||''))
      .map(a => {
        let which = (String(a.employeeType||'').toLowerCase()==='temporary') ? 'temporary' : 'employees';
        // Resolve employeeId if missing using QID lookup against employees/temporary lists
        let empId = a.employeeId || '';
        if (!empId) {
          try {
            const getEmployees = _deps && typeof _deps.getEmployees === 'function' ? _deps.getEmployees : null;
            const getTemporary = _deps && typeof _deps.getTemporaryEmployees === 'function' ? _deps.getTemporaryEmployees : null;
            const permList = getEmployees ? (getEmployees() || []) : [];
            const tempList = getTemporary ? (getTemporary() || []) : [];
            const qid = String(a.qid || '').replace(/\s+/g,'');
            if (qid) {
              if (which === 'temporary') {
                empId = (tempList.find(e => String(e.qid||'').replace(/\s+/g,'') === qid)?.id) || '';
              } else {
                empId = (permList.find(e => String(e.qid||'').replace(/\s+/g,'') === qid)?.id) || '';
              }
              // If still not found, try either list just in case employeeType is missing
              if (!empId) {
                empId = (permList.find(e => String(e.qid||'').replace(/\s+/g,'') === qid)?.id) || (tempList.find(e => String(e.qid||'').replace(/\s+/g,'') === qid)?.id) || '';
              }
            }
            // Fallback: unique name match if no qid/employeeId
            if (!empId && a.employeeName) {
              const norm = (s)=>String(s||'').trim().toLowerCase();
              const target = norm(a.employeeName);
              const matches = [];
              permList.forEach(e=>{ if (norm(e.name)===target) matches.push({id:e.id, which:'employees'}); });
              tempList.forEach(e=>{ if (norm(e.name)===target) matches.push({id:e.id, which:'temporary'}); });
              if (matches.length === 1) {
                empId = matches[0].id;
                // If which was unknown/misleading, prefer matched which
                if (!a.employeeType) which = matches[0].which;
              }
            }
          } catch {}
        }
        const empLink = empId
          ? `<a href="#" data-view-employee="${empId}" data-emp-which="${which}" class="text-indigo-600 hover:underline font-semibold cursor-pointer" role="button" tabindex="0" title="View employee details">${escapeHtml(a.employeeName||'')}</a>`
          : `<span class="font-semibold text-gray-900">${escapeHtml(a.employeeName||'')}</span>`;
        return `<tr class="hover:bg-gray-50">
        <td class="px-4 py-2">${empLink}</td>
        <td class="px-4 py-2">${a.startDate ? new Date(a.startDate).toLocaleDateString() : '-'}</td>
        <td class="px-4 py-2">${a.endDate ? new Date(a.endDate).toLocaleDateString() : '-'}</td>
        <td class="px-4 py-2">${a.rate ? `$${Number(a.rate).toLocaleString(undefined,{maximumFractionDigits:2})}/${a.rateType||'monthly'}` : '-'}</td>
        <td class="px-4 py-2">${escapeHtml(a.notes||'')}</td>
      </tr>`;
      }).join('');
    if (rows && body) body.innerHTML = rows;
    const hasRows = !!rows;
    if (empty) empty.style.display = hasRows ? 'none' : '';
    if (tbl) tbl.style.display = hasRows ? '' : 'none';
    const modal = document.getElementById('clientViewModal');
    if (modal) modal.classList.add('show');
    window.closeClientViewModal = () => { const m = document.getElementById('clientViewModal'); if (m) m.classList.remove('show'); };
  } catch {}
}

// Delegated handler: open Employee Details when clicking an employee name in Client View
if (!document.__clientsEmpLinkWired) {
  const openEmp = (link, prevent) => {
    if (!link) return;
    if (prevent) try { prevent(); } catch {}
    const id = link.getAttribute('data-view-employee');
    const which = link.getAttribute('data-emp-which') === 'temporary' ? 'temporary' : 'employees';
    try { window.closeClientViewModal && window.closeClientViewModal(); } catch {}
    try { window.viewEmployee && window.viewEmployee(id, which); } catch {}
  };
  // Capture-phase to ensure we intercept before default navigation or other handlers
  document.addEventListener('click', (e) => {
    const link = e.target && e.target.closest && e.target.closest('[data-view-employee]');
    if (!link) return;
    openEmp(link, () => e.preventDefault());
  }, true);
  // Bubble-phase as fallback
  document.addEventListener('click', (e) => {
    const link = e.target && e.target.closest && e.target.closest('[data-view-employee]');
    if (!link) return;
    openEmp(link, () => e.preventDefault());
  });
  // Keyboard support
  document.addEventListener('keydown', (e) => {
    const link = e.target && e.target.closest && e.target.closest('[data-view-employee]');
    if (!link) return;
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      openEmp(link);
    }
  });
  document.__clientsEmpLinkWired = true;
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
  const { db, collection, addDoc, updateDoc, doc, showToast, cleanData } = _deps || {};
  const toIso = (d) => {
    try {
      if (!d) return new Date().toISOString();
      if (typeof d === 'string') {
        // If already ISO-ish, return as-is
        if (/^\d{4}-\d{2}-\d{2}T[0-9:.-]+Z$/.test(d)) return d;
        const dd = new Date(d); if (!isNaN(dd.getTime())) return dd.toISOString();
        return new Date().toISOString();
      }
      if (typeof d === 'object') {
        if (typeof d.toDate === 'function') {
          const dd = d.toDate(); if (dd instanceof Date && !isNaN(dd.getTime())) return dd.toISOString();
        }
        if (typeof d.seconds === 'number') {
          const dd = new Date(d.seconds * 1000); if (!isNaN(dd.getTime())) return dd.toISOString();
        }
      }
    } catch {}
    return new Date().toISOString();
  };
  const name = document.getElementById('clientName')?.value.trim();
  const email = document.getElementById('clientEmail')?.value.trim();
  const phone = document.getElementById('clientPhone')?.value.trim();
  const company = document.getElementById('clientCompany')?.value.trim();
  const address = document.getElementById('clientAddress')?.value.trim();
  // Monthly Pay (required): ensure numeric >= 0. Use 0 to indicate no override.
  const clientMonthlyRaw = document.getElementById('clientMonthly')?.value;
  let clientMonthly = 0;
  if (clientMonthlyRaw === undefined || String(clientMonthlyRaw).trim() === '') {
    _deps?.showToast && _deps.showToast('Monthly Pay is required', 'warning');
    return;
  }
  {
    const n = Number(clientMonthlyRaw);
    if (isNaN(n) || n < 0) {
      _deps?.showToast && _deps.showToast('Monthly Pay must be a number greater than or equal to 0', 'warning');
      return;
    }
    clientMonthly = n;
  }
  if (!name || !email || !address) {
    showToast && showToast('Company Name, Email, and Address are required', 'warning');
    return;
  }
  try {
    if (_editingClientId) {
      // Update existing client; preserve createdAt to pass validation
      const existing = _clients.find(c => c.id === _editingClientId) || {};
      const createdAtIso = toIso(existing.createdAt);
      const payload = cleanData ? cleanData({
        name, email, phone, company, address, monthly: clientMonthly, createdAt: createdAtIso,
      }) : { name, email, phone, company, address, monthly: clientMonthly, createdAt: createdAtIso };
      await updateDoc(doc(db, 'customers', _editingClientId), payload);
      showToast && showToast('Client updated', 'success');
      // Optimistic local update
      try {
        const idx = _clients.findIndex(c => c.id === _editingClientId);
        if (idx !== -1) { _clients[idx] = { ..._clients[idx], ...payload }; renderClientsTable(); }
      } catch {}
    } else {
      const payload = cleanData ? cleanData({
        name, email, phone, company, address, monthly: clientMonthly, createdAt: new Date().toISOString(),
      }) : { name, email, phone, company, address, monthly: clientMonthly, createdAt: new Date().toISOString() };
      const ref = await addDoc(collection(db, 'customers'), payload);
      showToast && showToast('Client added', 'success');
      try {
        const exists = _clients.some(c => c.id === ref.id);
        if (!exists) { _clients = _clients.concat([{ id: ref.id, ...payload }]); renderClientsTable(); }
      } catch {}
    }
    closeClientModal();
    _editingClientId = null;
    setClientModalMode('add');
  } catch (err) {
    console.error('Save client failed', err);
    showToast && showToast('Failed to save client', 'error');
  }
}

function setClientModalMode(mode) {
  try {
    const title = document.getElementById('clientModalTitle');
    const submit = document.querySelector('button[form="clientForm"]');
    if (mode === 'edit') {
      if (title) title.textContent = 'Edit Client';
      if (submit) submit.innerHTML = '<i class="fas fa-save"></i> Update Client';
    } else {
      if (title) title.textContent = 'Add Client';
      if (submit) submit.innerHTML = '<i class="fas fa-save"></i> Save Client';
    }
  } catch {}
}

function openClientEditFlow(id) {
  try {
    const c = _clients.find(x => x.id === id);
    if (!c) return;
    _editingClientId = id;
    setClientModalMode('edit');
    const form = document.getElementById('clientForm');
    if (form) {
      const set = (id, v) => { const el = document.getElementById(id); if (el) el.value = v || ''; };
      set('clientName', c.name||'');
      set('clientEmail', c.email||'');
      set('clientPhone', c.phone||'');
      set('clientCompany', c.company||'');
      set('clientAddress', c.address||'');
      try {
        const m = document.getElementById('clientMonthly');
        if (m) {
          const val = Number(c.monthly || 0);
          m.value = String(val);
        }
      } catch {}
    }
    const modal = document.getElementById('clientModal');
    if (modal) modal.classList.add('show');
  } catch { openClientModal(); }
}
