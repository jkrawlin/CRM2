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

  // Wire delegated delete handler once
  if (!document.__ledgerDeleteWired) {
    document.addEventListener('click', async (e) => {
      const btn = e.target && e.target.closest && e.target.closest('[data-delete-ledger-cf]');
      if (!btn) return;
      const id = btn.getAttribute('data-delete-ledger-cf');
      if (!id || !_deps || !_deps.deleteDoc || !_deps.doc || !_deps.db) return;
      const ok = window.confirm('Delete this transaction from the ledger?');
      if (!ok) return;
      try {
        if (typeof window.deleteCashflowWithLinks === 'function') {
          await window.deleteCashflowWithLinks(id);
        } else {
          await _deps.deleteDoc(_deps.doc(_deps.db, 'cashflows', id));
          _deps.showToast && _deps.showToast('Transaction deleted', 'success');
          try { window.__recomputeFund && window.__recomputeFund(); } catch {}
          renderLedgerTable();
        }
      } catch (err) {
        console.error('Ledger delete failed', err);
        _deps.showToast && _deps.showToast('Failed to delete transaction', 'error');
      }
    });
    document.__ledgerDeleteWired = true;
  }
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

  // Simplified: no running balance / opening; just raw debits & credits
  const account = _accounts.find(a => a.id === accId);
  const accSelEl = document.getElementById('ledgerAccountFilter');
  const optText = accSelEl && accSelEl.selectedIndex >= 0 ? (accSelEl.options[accSelEl.selectedIndex]?.text || '') : '';
  const inferredAsset = /(\(|\s)Asset(\)|\s|$)/i.test(optText);
  const byType = account && String(account.type||'').toLowerCase() === 'asset';
  const byName = account && /cash/i.test(String(account.name||''));
  const isCashAccount = Boolean(byType || byName || (!account && inferredAsset));
  let debitSum = 0, creditSum = 0;
  const __CUR_PREFIX = (typeof CURRENCY_PREFIX !== 'undefined') ? CURRENCY_PREFIX : 'QAR ';
  const fmt = (n) => `${__CUR_PREFIX}${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;

  const lines = [];
  // Header spacer row (optional) now removed

  for (const t of rows) {
    const isIn = t.type === 'in';
    const amt = Number(t.amount || 0);
  if (isIn) { debitSum += amt; } else { creditSum += amt; }
    const desc = t.category ? `${t.category}${t.notes ? ' — ' + t.notes : ''}` : (t.notes || '');
    lines.push(`
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2">${escapeHtml(t.date || '')}</td>
        <td class="px-4 py-2">${escapeHtml(desc)}</td>
        <td class="px-4 py-2 text-right">${isIn ? fmt(amt) : ''}</td>
        <td class="px-4 py-2 text-right">${!isIn ? fmt(amt) : ''}</td>
          <td class="px-4 py-2 text-right">${isCashAccount ? `<button class="btn btn-danger btn-sm ledger-del-btn" data-delete-ledger-cf="${t.id}" title="Delete transaction" aria-label="Delete transaction" style="min-width:34px;height:28px;padding:0 8px;display:inline-flex;align-items:center;justify-content:center;"><i class=\"fas fa-trash\"></i></button>` : ''}</td>
      </tr>
    `);
  }

  // Totals and closing
  lines.push(`
    <tr class="bg-gray-50">
      <td class="px-4 py-2" colspan="2"><span class="text-gray-700 font-semibold">Totals</span></td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(debitSum)}</td>
      <td class="px-4 py-2 text-right font-semibold">${fmt(creditSum)}</td>
      <td class="px-4 py-2 text-right"></td>
    </tr>
  `);

  tbody.innerHTML = lines.join('');
  if (sumEl) { sumEl.textContent = ''; }

  // Expose structured data for external consumers (e.g., printing daily summaries)
  try {
    window.__ledgerCurrentView = {
      accountId: accId,
      month: ym,
      day: dayVal || '',
  opening: 0,
  closing: 0,
      debitSum,
      creditSum,
      transactions: rows.map(t => {
        const rawDate = t.date || '';
        const normalized = rawDate.slice(0,10);
        return {
          id: t.id || null,
          date: normalized, // primary normalized date
          rawDate,          // original string
          type: t.type,
          amount: Number(t.amount||0),
          category: t.category || '',
            notes: t.notes || '',
          accountId: t.accountId
        };
      })
    };
  } catch {}
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}
