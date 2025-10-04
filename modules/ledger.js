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

  // Expose structured data for external consumers (e.g., printing daily summaries)
  try {
    window.__ledgerCurrentView = {
      accountId: accId,
      month: ym,
      day: dayVal || '',
      opening,
      closing: running,
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

// =============================
// Printing Support
// =============================
export function printLedger() {
  try {
    const view = window.__ledgerCurrentView;
    if (!view || !view.accountId) {
      alert('Select an account and month/day before printing.');
      return;
    }
    const account = _accounts.find(a => a.id === view.accountId);
    const accountName = account?.name || 'Account';
    const monthTitle = view.month || 'All Time';
    const html = buildLedgerPrintHtml({
      accountName,
      monthTitle,
      day: view.day,
      opening: view.opening,
      closing: view.closing,
      debitSum: view.debitSum,
      creditSum: view.creditSum,
      transactions: view.transactions || []
    });
    const w = window.open('', '_blank', 'noopener,noreferrer,width=980,height=900');
    if (!w) { alert('Popup blocked. Please allow popups to print.'); return; }
    w.document.write(html);
    w.document.close();
    w.addEventListener('load', () => {
      setTimeout(() => {
        try { w.print(); } catch {}
        setTimeout(() => { try { w.close(); } catch {} }, 500);
      }, 200);
    });
  } catch (e) {
    console.error('printLedger failed', e);
    alert('Failed to prepare ledger print.');
  }
}

function buildLedgerPrintHtml(data) {
  const { accountName, monthTitle, day, opening, closing, debitSum, creditSum, transactions } = data;
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  // Recompute running for print to show balance progression
  let running = opening;
  const rows = transactions.map(t => {
    const isIn = String(t.type).toLowerCase()==='in';
    const amt = Number(t.amount||0);
    if (isIn) running += amt; else running -= amt;
    const desc = t.category ? `${t.category}${t.notes ? ' — ' + t.notes : ''}` : (t.notes || '');
    return `
      <tr>
        <td>${escapeHtml(t.date)}</td>
        <td>${escapeHtml(desc)}</td>
        <td style="text-align:right">${isIn ? fmt(amt) : ''}</td>
        <td style="text-align:right">${!isIn ? fmt(amt) : ''}</td>
        <td style="text-align:right">${fmt(running)}</td>
      </tr>`;
  }).join('');

  return `<!DOCTYPE html><html><head>
  <meta charset="utf-8" />
  <title>Ledger - ${escapeHtml(accountName)} - ${escapeHtml(day || monthTitle)}</title>
  <style>
    @page { size: A4 portrait; margin: 15mm; }
    body { font-family: Inter, Arial, sans-serif; font-size: 12px; color: #1f2937; margin:0; }
    h1 { font-size: 20px; margin:0 0 4px; }
    h2 { font-size: 14px; margin:0 0 12px; color:#475569; font-weight:600; }
    .header { text-align:center; margin-bottom: 10px; }
    .meta { display:flex; flex-wrap:wrap; gap:12px; font-size:11px; margin-bottom:12px; }
    .meta div { background:#F1F5F9; padding:6px 10px; border-radius:4px; border:1px solid #E2E8F0; }
    table { width:100%; border-collapse:collapse; font-size:11px; }
    th, td { border:1px solid #E2E8F0; padding:6px 6px; }
    th { background:#EEF2FF; font-weight:600; text-align:left; }
    tbody tr:nth-child(even) { background:#F8FAFC; }
    tfoot td { font-weight:600; background:#F1F5F9; }
    .footer { margin-top:24px; text-align:center; font-size:10px; color:#64748B; }
    .watermark { position:fixed; top:50%; left:50%; transform:translate(-50%, -50%); font-size:70px; font-weight:800; color:rgba(99,102,241,0.06); pointer-events:none; }
    @media print { body { -webkit-print-color-adjust: exact; print-color-adjust: exact; } }
  </style>
  </head><body>
    <div class="header">
      <h1>Ledger Report</h1>
      <h2>${escapeHtml(accountName)}</h2>
      <div style="font-size:11px; color:#64748B;">Period: ${escapeHtml(day || monthTitle)}</div>
    </div>
    <div class="meta">
      <div><strong>Opening:</strong> ${fmt(opening)}</div>
      <div><strong>Total Debits:</strong> ${fmt(debitSum)}</div>
      <div><strong>Total Credits:</strong> ${fmt(creditSum)}</div>
      <div><strong>Closing:</strong> ${fmt(closing)}</div>
      <div><strong>Generated:</strong> ${escapeHtml(new Date().toLocaleString())}</div>
    </div>
    <table>
      <thead><tr><th style="width:14%">Date</th><th style="width:40%">Description</th><th style="width:14%;text-align:right">Debit</th><th style="width:14%;text-align:right">Credit</th><th style="width:18%;text-align:right">Balance</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="5" style="text-align:center;padding:12px;">No entries</td></tr>`}</tbody>
      <tfoot>
        <tr><td colspan="2">Totals</td><td style="text-align:right">${fmt(debitSum)}</td><td style="text-align:right">${fmt(creditSum)}</td><td style="text-align:right">${fmt(closing)}</td></tr>
      </tfoot>
    </table>
    <div class="footer">Printed by CRM System</div>
    <div class="watermark">CONFIDENTIAL</div>
    <script>window.addEventListener('load', () => { setTimeout(()=> { try { window.print(); } catch(e){} }, 150); });</script>
  </body></html>`;
}
