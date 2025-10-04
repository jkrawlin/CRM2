// Ledger module: per-account ledger view with running balance, built from cashflows

let _deps = null;
let _txns = [];
let _accounts = [];
let _unsub = null;

// Contract
// initLedger({ db, collection, query, onSnapshot, orderBy, where, showToast, cleanData, getAccounts })
export function initLedger(deps) {
  _deps = deps;
  // Default month
  const monthEl = document.getElementById('ledgerMonth');
  if (monthEl && !monthEl.value) {
    const now = new Date();
    monthEl.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  }
  // Populate account dropdowns
  try { populateAccountFilter(); } catch {}
  const monthFilter = document.getElementById('ledgerMonth');
  if (monthFilter) monthFilter.addEventListener('change', () => subscribeLedger());
  // Attach day filter listener (may be dynamically injected, so retry if absent)
  const attachDayListener = () => {
    const df = document.getElementById('ledgerDay');
    if (!df) return false;
    if (!df.__ledgerBound) {
      df.addEventListener('change', () => {
        // When a day is chosen, optionally sync month input for clarity
        const v = (df.value || '').trim();
        if (v.length === 10) {
          const monthInput = document.getElementById('ledgerMonth');
          if (monthInput && !monthInput.disabled) monthInput.value = v.slice(0,7); // keep month aligned
        }
        renderLedgerTable();
      });
      df.__ledgerBound = true;
    }
    return true;
  };
  if (!attachDayListener()) {
    let attempts = 0;
    const iv = setInterval(() => {
      attempts++;
      if (attachDayListener() || attempts > 10) clearInterval(iv);
    }, 500);
  }
  const accFilter = document.getElementById('ledgerAccountFilter');
  if (accFilter) accFilter.addEventListener('change', () => renderLedgerTable());
}

function populateAccountFilter() {
  const sel = document.getElementById('ledgerAccountFilter');
  if (!sel) return;
  try { _accounts = (_deps.getAccounts && _deps.getAccounts()) || []; } catch { _accounts = []; }
  const options = ['<option value="">Select Account</option>']
    .concat(_accounts.map(a => `<option value="${a.id}">${escapeHtml(a.name || '')} (${a.type || ''})</option>`));
  sel.innerHTML = options.join('');
}

// Public method to refresh account options and re-read current accounts list
export function refreshLedgerAccounts() {
  try { populateAccountFilter(); } catch {}
}

export function subscribeLedger() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot } = _deps;
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
  const q = query(collection(db, 'cashflows'));
  _unsub = onSnapshot(q, (snap) => {
    const all = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    _txns = all.slice().sort((a,b) => (a.date||'').localeCompare(b.date||''));
    renderLedgerTable();
  }, (err) => {
    console.error('Error loading ledger (cashflows)', err);
    _deps.showToast && _deps.showToast('Error loading ledger', 'error');
  });
}

export function stopLedger() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function renderLedgerTable() {
  const tbody = document.getElementById('ledgerTableBody');
  const empty = document.getElementById('ledgerEmptyState');
  const sumEl = document.getElementById('ledgerSummary');
  if (!tbody || !empty) return;

  const monthEl = document.getElementById('ledgerMonth');
  const ym = monthEl?.value || '';
  const dayEl = document.getElementById('ledgerDay');
  let dayVal = dayEl?.value || '';
  // Normalize day value to YYYY-MM-DD if larger timestamp entered
  if (dayVal && dayVal.length > 10) dayVal = dayVal.slice(0,10);
  const accSel = document.getElementById('ledgerAccountFilter');
  const accId = accSel?.value || '';
  if (!accId) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    if (sumEl) sumEl.textContent = '';
    return;
  }

  // Helper to extract pure date portion (YYYY-MM-DD) even if timestamp present
  const dateOnly = (d) => (d || '').slice(0, 10);

  // Filter to exact day or to month range
  const rows = _txns.filter(t => {
    const dOnly = dateOnly(t.date);
    if (dayVal) {
      if (dOnly !== dayVal) return false;
      return t.accountId === accId;
    }
    return (!ym || (dOnly.startsWith(ym + '-'))) && t.accountId === accId;
  });
  if (!rows.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    if (sumEl) sumEl.textContent = '';
    return;
  }
  empty.classList.add('hidden');

  // Compute opening balance = opening + all prior months' net for this account
  const account = _accounts.find(a => a.id === accId);
  const openingBase = Number(account?.opening || 0);
  let opening = openingBase;
  if (dayVal) {
    // Opening before selected day (compare by date-only)
    const prior = _txns.filter(t => t.accountId === accId && dateOnly(t.date) < dayVal);
    for (const t of prior) opening += (t.type === 'in' ? Number(t.amount||0) : -Number(t.amount||0));
  } else if (ym) {
    const firstOfMonth = `${ym}-01`;
    const prior = _txns.filter(t => t.accountId === accId && dateOnly(t.date) < firstOfMonth);
    for (const t of prior) opening += (t.type === 'in' ? Number(t.amount||0) : -Number(t.amount||0));
  }

  let running = opening;
  let debitSum = 0, creditSum = 0;
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;

  // Prepend opening line
  const lines = [];
  lines.push(`
    <tr class="bg-gray-50/60">
      <td class="px-4 py-2" colspan="4"><span class="text-gray-500">Amount</span></td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(running)}</td>
    </tr>
  `);

  for (const t of rows) {
    const isIn = t.type === 'in';
    const amt = Number(t.amount || 0);
    if (isIn) { debitSum += amt; running += amt; } else { creditSum += amt; running -= amt; }
    const desc = t.category ? `${t.category}${t.notes ? ' — ' + t.notes : ''}` : (t.notes || '');
    lines.push(`
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2">${escapeHtml(t.date || '')}</td>
        <td class="px-4 py-2">${escapeHtml(desc)}</td>
        <td class="px-4 py-2 text-right">${isIn ? fmt(amt) : ''}</td>
        <td class="px-4 py-2 text-right">${!isIn ? fmt(amt) : ''}</td>
        <td class="px-4 py-2 text-right">${fmt(running)}</td>
      </tr>
    `);
  }

  // Totals and closing
  lines.push(`
    <tr class="bg-gray-50">
      <td class="px-4 py-2" colspan="2"><span class="text-gray-700 font-semibold">Totals</span></td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(debitSum)}</td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(creditSum)}</td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(running)}</td>
    </tr>
  `);

  tbody.innerHTML = lines.join('');
  if (sumEl) {
    const net = debitSum - creditSum;
    const accName = account?.name || '';
    sumEl.textContent = `${accName}: Opening ${fmt(opening)} • Debits ${fmt(debitSum)} • Credits ${fmt(creditSum)} • Closing ${fmt(running)} (${net>=0?'+':''}${fmt(net).replace('$','$')})`;
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
