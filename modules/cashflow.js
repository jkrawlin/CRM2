// Cash Flow module: list and add simple cash transactions

let _deps = null;
let _txns = [];
let _unsub = null;
let _accounts = [];

export function initCashflow(deps) {
  // deps: { db, collection, query, onSnapshot, addDoc, orderBy, where, showToast, cleanData, getAccounts, getEmployees?, getTemporaryEmployees?, serverTimestamp? }
  _deps = deps;
  const monthEl = document.getElementById('cashflowMonth');
  if (monthEl && !monthEl.value) {
    const now = new Date();
    monthEl.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  }
  const btn = document.getElementById('openCashTxnModalBtn');
  if (btn) btn.addEventListener('click', () => openCashTxnModal());
  const inc1 = document.getElementById('openIncomeTxnBtn');
  if (inc1) inc1.addEventListener('click', () => openCashTxnModal({ type: 'in' }));
  const exp1 = document.getElementById('openExpenseTxnBtn');
  if (exp1) exp1.addEventListener('click', () => openCashTxnModal({ type: 'out' }));
  const inc2 = document.getElementById('openIncomeTxnBtn2');
  if (inc2) inc2.addEventListener('click', () => openCashTxnModal({ type: 'in' }));
  const exp2 = document.getElementById('openExpenseTxnBtn2');
  if (exp2) exp2.addEventListener('click', () => openCashTxnModal({ type: 'out' }));
  const form = document.getElementById('cashTxnForm');
  if (form) form.addEventListener('submit', handleCashTxnSubmit);
  // Quick add driver UI removed
  const monthFilter = document.getElementById('cashflowMonth');
  if (monthFilter) monthFilter.addEventListener('change', () => subscribeCashflow());
  const accFilter = document.getElementById('cashflowAccountFilter');
  if (accFilter) accFilter.addEventListener('change', () => renderCashflowTable());
  window.openCashTxnModal = openCashTxnModal;
  window.closeCashTxnModal = closeCashTxnModal;
  try { populateAccountFilters(); } catch {}
}

function populateAccountFilters() {
  const sel1 = document.getElementById('cashflowAccountFilter');
  const sel2 = document.getElementById('cfAccount');
  if (!sel1 && !sel2) return;
  try { _accounts = (_deps.getAccounts && _deps.getAccounts()) || []; } catch { _accounts = []; }
  const assetOnly = _accounts.filter(a => (a.type || '').toLowerCase() === 'asset');
  const options = ['<option value="">All Accounts</option>']
    .concat(assetOnly.map(a => `<option value="${a.id}">${escapeHtml(a.name || '')}</option>`));
  if (sel1) sel1.innerHTML = options.join('');
  const options2 = assetOnly.map(a => `<option value="${a.id}">${escapeHtml(a.name || '')} (${a.type})</option>`);
  if (sel2) sel2.innerHTML = options2.join('');
}

export function subscribeCashflow() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot, where, orderBy, showToast } = _deps;
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
  const monthEl = document.getElementById('cashflowMonth');
  const ym = monthEl?.value || '';
  // We store date as YYYY-MM-DD; filter by prefix if needed using client-side for now
  const q = query(collection(db, 'cashflows'));
  _unsub = onSnapshot(q, (snap) => {
    const all = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    let filtered = all;
    if (ym) filtered = all.filter(t => (t.date || '').startsWith(ym + '-'));
    _txns = filtered.sort((a,b) => (a.date||'').localeCompare(b.date||''));
    renderCashflowTable();
    // Publish to global shadow for fund card computation
    try { window.__cashflowAll = all; } catch {}
    try { window.dispatchEvent(new CustomEvent('cashflow:updated', { detail: all })); } catch {}
  }, (err) => {
    console.error('Error loading cashflow', err);
    showToast && showToast('Error loading cash flow', 'error');
  });
}

export function stopCashflow() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function renderCashflowTable() {
  const tbody = document.getElementById('cashflowTableBody');
  const empty = document.getElementById('cashflowEmptyState');
  const sumEl = document.getElementById('cashflowSummary');
  if (!tbody || !empty) return;
  let rows = _txns;
  const accFilter = document.getElementById('cashflowAccountFilter');
  const accId = accFilter?.value || '';
  if (accId) rows = rows.filter(t => t.accountId === accId);
  if (!rows.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    if (sumEl) sumEl.textContent = '';
    return;
  }
  empty.classList.add('hidden');
  const fmt = (n) => `QAR ${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const accName = (id) => (_accounts.find(a => a.id === id)?.name) || '';
  let inSum = 0, outSum = 0;
  tbody.innerHTML = rows.map(t => {
    if (t.type === 'in') inSum += Number(t.amount || 0); else outSum += Number(t.amount || 0);
    return `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2">${escapeHtml(t.date || '')}</td>
        <td class="px-4 py-2">${escapeHtml(accName(t.accountId))}</td>
        <td class="px-4 py-2">${t.type === 'in' ? 'In' : 'Out'}</td>
        <td class="px-4 py-2">${escapeHtml(t.category || '')}</td>
        <td class="px-4 py-2 text-right">${fmt(t.amount || 0)}</td>
        <td class="px-4 py-2">${escapeHtml(t.notes || '')}</td>
      </tr>
    `;
  }).join('');
  if (sumEl) {
    const net = inSum - outSum;
    sumEl.textContent = `In: ${fmt(inSum)} • Out: ${fmt(outSum)} • Net: ${fmt(net)}`;
  }
}

function openCashTxnModal(preset) {
  populateAccountFilters();
  populateDriverOptions(); // shows only Job Position/Role matching 'Driver'
  const modal = document.getElementById('cashTxnModal');
  const form = document.getElementById('cashTxnForm');
  if (form) form.reset();
  const d = document.getElementById('cfDate');
  if (d) d.valueAsDate = new Date();
  const typeSel = document.getElementById('cfType');
  if (typeSel) {
    if (preset && (preset.type === 'in' || preset.type === 'out')) {
      typeSel.value = preset.type;
      typeSel.disabled = true; // lock type for quick actions
      try { modal?.setAttribute('data-kind', preset.type); } catch {}
    } else {
      typeSel.value = 'in';
      typeSel.disabled = false;
      try { modal?.removeAttribute('data-kind'); } catch {}
    }
  }
  // If Expense type, bias category when driver is chosen later
  try {
    const catEl = document.getElementById('cfCategory');
    const drvEl = document.getElementById('cfDriver');
    if (drvEl && catEl) {
      drvEl.addEventListener('change', () => {
        const v = (drvEl.value||'');
        if (v && (!catEl.value || /^(petrol|fuel)$/i.test(catEl.value.trim())===false)) catEl.value = 'Petrol';
      }, { once: true });
    }
  } catch {}
  // focus amount for speed
  setTimeout(() => { try { document.getElementById('cfAmount')?.focus(); } catch {} }, 0);
  if (modal) modal.classList.add('show');
}

function closeCashTxnModal() {
  const modal = document.getElementById('cashTxnModal');
  if (modal) modal.classList.remove('show');
  // unlock type for general opens later
  try { const typeSel = document.getElementById('cfType'); if (typeSel) typeSel.disabled = false; } catch {}
  try { const m = document.getElementById('cashTxnModal'); m?.removeAttribute('data-kind'); } catch {}
}

async function handleCashTxnSubmit(e) {
  e.preventDefault();
  const { db, collection, addDoc, showToast, cleanData } = _deps || {};
  const date = document.getElementById('cfDate')?.value || '';
  // Decide type with strong precedence for preset data-kind
  const modal = document.getElementById('cashTxnModal');
  const kindPreset = modal?.getAttribute('data-kind');
  const rawType = String(document.getElementById('cfType')?.value || 'in').trim().toLowerCase();
  let type = (kindPreset === 'out' || rawType === 'out' || rawType === 'expense' || rawType === 'debit') ? 'out' : 'in';
  let accountId = document.getElementById('cfAccount')?.value || '';
  const amount = Math.abs(Number(document.getElementById('cfAmount')?.value || 0)) || 0;
  const category = document.getElementById('cfCategory')?.value?.trim() || undefined;
  const notes = document.getElementById('cfNotes')?.value?.trim() || undefined;
  const driverId = document.getElementById('cfDriver')?.value || '';
  const isPetrol = /^(petrol|fuel)$/i.test(String(category||''));
  // Validation: if petrol/fuel, require driver
  if (type==='out' && isPetrol && !driverId) {
    try { const hint = document.getElementById('cfDriverHint'); if (hint) hint.style.display=''; } catch {}
    showToast && showToast('Please select an Assigned Driver for Petrol/Fuel expense', 'warning');
    return;
  }
  if (!date || !accountId || !type || !(amount > 0)) {
    showToast && showToast('Please fill date, account, type and positive amount', 'warning');
    return;
  }
  let acc = _accounts.find(a => a.id === accountId);
  // Ensure we post against an Asset account so fund updates. If selection is invalid or non-Asset, fallback to default Cash asset.
  if (!acc || (String(acc.type||'').toLowerCase() !== 'asset')) {
    try {
      const assetId = _deps.ensureAssetAccount ? await _deps.ensureAssetAccount('cash','Cash') : '';
      if (assetId) {
        accountId = assetId;
        acc = _accounts.find(a => a.id === assetId) || { id: assetId, name: 'Cash', type: 'Asset' };
      }
    } catch {}
  }
  const payload = cleanData({
    date,
    type, // 'in' | 'out'
    accountId,
    accountName: acc?.name || undefined,
    amount,
    category,
    notes,
    driverId: driverId || undefined,
    createdAt: new Date().toISOString(),
    createdBy: typeof window !== 'undefined' && window.__userUid ? window.__userUid : undefined,
  });
  try {
  const ref = await addDoc(collection(db, 'cashflows'), payload);
    showToast && showToast('Transaction added', 'success');
    // Force a deterministic fund recompute and persist; ignore errors
    try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
    // Optimistic add
    try {
      const exists = _txns.some(x => x.id === ref.id);
      if (!exists) {
        _txns = _txns.concat([{ id: ref.id, ...payload }]);
        renderCashflowTable();
        // Also update global shadow immediately so fund card updates without waiting for snapshot
        try {
          const all = Array.isArray(window.__cashflowAll) ? window.__cashflowAll.slice() : [];
          all.push({ id: ref.id, ...payload });
          window.__cashflowAll = all;
          window.dispatchEvent(new CustomEvent('cashflow:updated', { detail: all }));
          // Also trigger immediate fund card refresh if available
          try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
        } catch {}
      }
    } catch {}
    closeCashTxnModal();
  } catch (err) {
    console.error('Add cash transaction failed', err);
    showToast && showToast('Failed to add transaction', 'error');
  }
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function populateDriverOptions() {
  const sel = document.getElementById('cfDriver');
  if (!sel) return;
  let emps = [];
  try {
    const ge = _deps.getEmployees ? _deps.getEmployees() : [];
    const gt = _deps.getTemporaryEmployees ? _deps.getTemporaryEmployees() : [];
    emps = [...(ge||[]), ...(gt||[])];
  } catch {}
  // Only include people whose Job Position/Role contains 'Driver' (case-insensitive)
  const drivers = emps.filter(e => /driver/i.test(String(e.position||e.role||'')));
  // Sort by name for consistency
  drivers.sort((a,b)=> String(a.name||'').localeCompare(String(b.name||'')));
  const format = e => `<option value="${e.id}">${escapeHtml(e.name||'')}</option>`;
  const options = ['<option value="">-- Select Driver --</option>']
    .concat(drivers.map(format));
  if (drivers.length === 0) {
    options.push('<option value="" disabled>(No drivers found)</option>');
  }
  sel.innerHTML = options.join('');
  sel.onchange = null;
}

// Quick-add driver flow removed along with modal
