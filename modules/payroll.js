// Payroll module: table rendering, report frame, sorting, sub-tab handling, CSV export
import { maskAccount, formatDate } from './utils.js';
// Version bump after UI compaction tweaks
console.log('[PayrollModule] Loaded modules/payroll.js v20251008-17');

let payrollSortColumn = '';
let payrollSortOrder = 'asc';
let currentPayrollSubTab = 'table';

export function initPayroll(context) {
  // context supplies getters so we don't import state directly
  const {
    getEmployees,
    getTemporaryEmployees,
    getSearchQuery,
  } = context;

  // Wire sub-tab buttons
  const tabTableBtn = document.getElementById('payrollTabTableBtn');
  const tabReportBtn = document.getElementById('payrollTabReportBtn');
  if (tabTableBtn) tabTableBtn.addEventListener('click', () => setPayrollSubTab('table', { getEmployees, getTemporaryEmployees, getSearchQuery }));
  if (tabReportBtn) tabReportBtn.addEventListener('click', () => setPayrollSubTab('report', { getEmployees, getTemporaryEmployees, getSearchQuery }));
  setPayrollSubTab('table', { getEmployees, getTemporaryEmployees, getSearchQuery });

  // Export/Print controls (rewired for Month Report)
  const exportCsvBtn = document.getElementById('exportPayrollCsvBtn');
  const printBtn = document.getElementById('printPayrollBtn');
  if (exportCsvBtn) exportCsvBtn.addEventListener('click', async () => {
    try {
      const monthEl = document.getElementById('payrollMonth');
      const ym = monthEl?.value || '';
      await exportMonthReportCsv({ ym, getEmployees, getTemporaryEmployees });
    } catch (e) { console.warn('Export failed', e); }
  });
  if (printBtn) printBtn.addEventListener('click', () => printPayrollProfessional({ getEmployees, getTemporaryEmployees }));
}

export function sortPayroll(column, deps) {
  if (payrollSortColumn === column) {
    payrollSortOrder = payrollSortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    payrollSortColumn = column;
    payrollSortOrder = 'asc';
  }
  renderPayrollTable(deps);
}

export function renderPayrollTable({ getEmployees, getTemporaryEmployees, getSearchQuery }) {
  const tbody = document.getElementById('payrollTableBody');
  const emptyState = document.getElementById('payrollEmptyState');
  const totalEl = document.getElementById('totalPayroll');
  if (!tbody || !emptyState) return;

  const employees = getEmployees().filter(e => !e.terminated);
  const temporaryEmployees = getTemporaryEmployees().filter(e => !e.terminated);

  const combined = [
    ...employees.map(e => ({ ...e, _type: 'Employee' })),
    ...temporaryEmployees.map(e => ({ ...e, _type: 'Temporary' })),
  ];

  const filtered = combined.filter(emp => {
    const query = (getSearchQuery() || '').trim();
    const queryDigits = query.replace(/\D/g, '');
    const empQidDigits = String(emp.qid || '').replace(/\D/g, '');
    let matchesSearch = true;
    if (query) {
      if (queryDigits.length >= 4) {
        matchesSearch = empQidDigits.includes(queryDigits);
      } else {
        const text = `${emp.name} ${emp.email} ${emp.qid || ''} ${emp.phone || ''} ${emp.position} ${emp.department}`.toLowerCase();
        matchesSearch = text.includes(query.toLowerCase());
      }
    }
    return matchesSearch;
  });

  const sorted = [...filtered];
  if (payrollSortColumn) {
    sorted.sort((a, b) => {
      let va;
      let vb;
      switch (payrollSortColumn) {
        case 'salary':
          va = Number(a.salary);
          vb = Number(b.salary);
          break;
        case 'type':
          va = (a._type || '').toLowerCase();
          vb = (b._type || '').toLowerCase();
          break;
        default:
          va = (a[payrollSortColumn] ?? '').toString().toLowerCase();
          vb = (b[payrollSortColumn] ?? '').toString().toLowerCase();
      }
      if (va < vb) return payrollSortOrder === 'asc' ? -1 : 1;
      if (va > vb) return payrollSortOrder === 'asc' ? 1 : -1;
      return 0;
    });
  }

  const total = filtered.reduce((sum, e) => sum + Number(e.salary || 0), 0);
  if (totalEl) totalEl.textContent = `QAR ${total.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

  if (sorted.length === 0) {
    tbody.innerHTML = '';
    emptyState.classList.remove('hidden');
    return;
  }
  emptyState.classList.add('hidden');

  const typeBadge = (t) => {
    const isTemp = t === 'Temporary';
    const cls = isTemp ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800';
    const label = isTemp ? 'Temporary' : 'Permanent';
    return `<span class="px-1.5 py-0.5 rounded text-xs font-semibold ${cls}">${label}</span>`;
  };

  const rows = sorted.map(emp => `
      <tr class="hover:bg-gray-50">
        <td class="px-2 py-2 font-semibold text-gray-900 truncate">${emp.name}</td>
        <td class="px-2 py-2">${typeBadge(emp._type)}</td>
        <td class="px-2 py-2 truncate">${emp.department || '-'}</td>
        <td class="px-2 py-2 text-right tabular-nums">QAR ${Number(emp.salary || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
        <td class="px-2 py-2 text-right tabular-nums" data-balance-for="${emp.id}">—</td>
        <td class="px-1 py-2 whitespace-nowrap text-right">${formatDate(emp.joinDate)}</td>
        <td class="px-1 py-2 tabular-nums text-center" title="${emp.qid || '-'}">${emp.qid || '-'}</td>
        <td class="px-2 py-2 text-center">
          <div class="action-buttons">
            <button class="action-btn btn-primary" onclick="openPayslipForm('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')" title="Pay Advance">
              <i class="fas fa-sack-dollar"></i>
            </button>
            <button class="action-btn btn-secondary" onclick="openPaymentForm('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')" title="Pay">
              <i class="fas fa-money-bill-wave"></i>
            </button>
            <button class="action-btn btn-secondary" onclick="viewPayroll('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')" title="View">
              <i class="fas fa-eye"></i>
            </button>
          </div>
        </td>
      </tr>
    `).join('');
  tbody.innerHTML = rows;

  // After rendering, populate current salary balance values
  computeAndFillCurrentBalances(sorted).catch(err => console.warn('Balance compute failed', err));

  // Recompute on demand
  const handler = () => computeAndFillCurrentBalances(sorted).catch(()=>{});
  document.removeEventListener('payroll:recompute-balances', handler);
  document.addEventListener('payroll:recompute-balances', handler, { once: true });
}

// =========================
// Monthly Report — Data + UI + Print (Rewired)
// =========================

// Get YYYY-MM date range [start, end] inclusive in YYYY-MM-DD format
function getMonthRange(ym) {
  if (!/^\d{4}-\d{2}$/.test(ym)) return ['',''];
  const [y, m] = ym.split('-').map(Number);
  const start = `${ym}-01`;
  const lastDay = new Date(y, m, 0).getDate();
  const end = `${ym}-${String(lastDay).padStart(2,'0')}`;
  return [start, end];
}

// Build all month report data in one go (aggregated queries per collection)
async function buildMonthReportData({ ym, getEmployees, getTemporaryEmployees }) {
  const perm = (getEmployees?.() || []).filter(e => !e.terminated).map(e => ({ ...e, _type: 'Employee' }));
  const temps = (getTemporaryEmployees?.() || []).filter(e => !e.terminated).map(e => ({ ...e, _type: 'Temporary' }));
  const all = [...perm, ...temps];
  const byId = new Map(all.map(e => [e.id, e]));
  const [start, end] = getMonthRange(ym);
  const monthTitle = ym ? new Date(ym + '-01').toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Month';

  // Short-circuit empty
  if (!all.length) {
    return { ym, monthTitle, employees: [], perm: [], temps: [], summary: { count:0, perm:0, temps:0, salary:0, outstanding:0 } };
  }

  const { collection, getDocs, query, where, doc, getDoc, orderBy } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
  const { db } = await import('../firebase-config.js');

  // Fetch payslips for the month in one query
  let payslips = [];
  try {
    const psSnap = await getDocs(query(collection(db, 'payslips'), where('period', '==', ym)));
    payslips = psSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch {}

  // Fetch payments within month range in one query (string-based date range)
  let payments = [];
  try {
    if (start && end) {
      const paySnap = await getDocs(query(collection(db, 'payments'), where('date', '>=', start), where('date', '<=', end)));
      payments = paySnap.docs.map(d => ({ id: d.id, ...d.data() }));
    }
  } catch {}

  // Group payslips by employeeId
  const psByEmp = new Map();
  for (const ps of payslips) {
    const k = ps.employeeId || '';
    if (!k) continue;
    if (!psByEmp.has(k)) psByEmp.set(k, []);
    psByEmp.get(k).push(ps);
  }
  // Group payments by employeeId (exclude advances)
  const payByEmp = new Map();
  for (const p of payments) {
    const k = p.employeeId || '';
    if (!k) continue;
    if (p.isAdvance) continue; // exclude advances from payment sum
    const amt = Number(p.amount || 0) + Number(p.overtime || 0);
    if (!payByEmp.has(k)) payByEmp.set(k, 0);
    payByEmp.set(k, payByEmp.get(k) + (isFinite(amt) ? amt : 0));
  }

  // Compute carryover from previous month balances doc per employee
  const prev = (() => {
    const [y, m] = ym.split('-').map(Number);
    const prevDate = new Date(y, m - 2, 1);
    return `${prevDate.getFullYear()}-${String(prevDate.getMonth() + 1).padStart(2,'0')}`;
  })();
  const carryoverMap = new Map();
  await Promise.all(all.map(async (e) => {
    try {
      const ref = doc(db, 'balances', `${e.id}_${prev}`);
      const snap = await getDoc(ref);
      const bal = snap.exists() ? Number(snap.data().balance || 0) : 0;
      carryoverMap.set(e.id, isFinite(bal) ? bal : 0);
    } catch { carryoverMap.set(e.id, 0); }
  }));

  // Build enriched rows
  const rows = all.map(e => {
    const slips = (psByEmp.get(e.id) || []).slice().sort((a,b)=> (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0));
    const latestBasic = slips.length ? Number(slips[0].basic || e.salary || 0) : Number(e.salary || 0);
    const advances = (psByEmp.get(e.id) || []).reduce((s, ps) => s + (Number(ps.advance || 0) || 0), 0);
    const paid = Number(payByEmp.get(e.id) || 0);
    const carry = Number(carryoverMap.get(e.id) || 0);
    const balance = Math.max(0, (carry + latestBasic) - advances - paid);
    return {
      ...e,
      _basic: isFinite(latestBasic) ? latestBasic : 0,
      _advances: isFinite(advances) ? advances : 0,
      _paid: isFinite(paid) ? paid : 0,
      _carry: isFinite(carry) ? carry : 0,
      _balance: isFinite(balance) ? balance : 0,
    };
  }).sort((a,b)=> (a.name||'').localeCompare(b.name||''));

  const permRows = rows.filter(r => r._type === 'Employee');
  const tempRows = rows.filter(r => r._type === 'Temporary');
  const totalSalary = rows.reduce((s, r) => s + Number(r._basic || 0), 0);
  const totalOutstanding = rows.reduce((s, r) => s + Number(r._balance || 0), 0);

  return {
    ym,
    monthTitle,
    employees: rows,
    perm: permRows,
    temps: tempRows,
    summary: {
      count: rows.length,
      perm: permRows.length,
      temps: tempRows.length,
      salary: totalSalary,
      outstanding: totalOutstanding,
    }
  };
}

// Use non-breaking space after currency to avoid wrapping between prefix and number
function fmtCurrency(n) { return `QAR\u00A0${Number(n||0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`; }
function escHtml(s) { return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

function renderMonthReportInto(frame, data) {
  if (!frame) return;
  const { monthTitle, employees, perm, temps, summary } = data;
  const badge = (t) => t === 'Temporary' ? '<span class="compact-badge bg-amber-100 text-amber-800">TEMP</span>' : '<span class="compact-badge bg-emerald-100 text-emerald-800">PERM</span>';
  const row = (e, i) => `
    <tr class="border-b border-gray-100 align-top">
      <td class="px-2 py-1 text-gray-500 text-[11px]">${i+1}</td>
      <td class="px-2 py-1 font-semibold text-gray-900 truncate text-[12px]" title="${escHtml(e.name||'')}">${escHtml(e.name||'')}</td>
      <td class="px-2 py-1 text-[11px]">${badge(e._type)}</td>
      <td class="px-2 py-1 truncate text-[11px]" title="${escHtml(e.department||'-')}">${escHtml(e.department||'-')}</td>
      <td class="px-2 py-1 mono-text nowrap text-[11px]">${escHtml(e.qid||'-')}</td>
      <td class="px-2 py-1 truncate text-[11px]" title="${escHtml(e.bankName||'-')}">${escHtml(e.bankName||'-')}</td>
      <td class="px-2 py-1 mono-text nowrap text-[11px]" style="max-width:240px;" title="${escHtml(e.bankAccountNumber||'-')}">${escHtml(e.bankAccountNumber||'-')}</td>
      <td class="px-2 py-1 mono-text nowrap text-[11px]" style="max-width:320px;" title="${escHtml(e.bankIban||'-')}">${escHtml(e.bankIban||'-')}</td>
      <td class="px-2 py-1 text-right font-mono font-semibold nowrap text-[11px]" style="min-width:130px;">${fmtCurrency(e._basic)}</td>
      <td class="px-2 py-1 text-right font-mono font-semibold nowrap text-[11px]" style="min-width:130px;">${fmtCurrency(e._balance)}</td>
    </tr>`;
  const section = (title, list) => {
    if (!list.length) return '';
    return `
      <div class="mb-8">
        <h3 class="text-sm font-bold text-gray-800 mb-2 uppercase tracking-wide">${escHtml(title)} <span class="text-gray-500 font-normal">(${list.length})</span></h3>
        <div class="overflow-x-auto rounded-lg border border-gray-200 shadow-sm bg-white">
          <table class="w-full text-[12px]">
            <thead>
              <tr class="bg-gray-50 text-gray-700 uppercase font-semibold text-[10px]">
                <th class="text-left px-2 py-1" style="width:36px;">#</th>
                <th class="text-left px-2 py-1" style="width:170px;">Name</th>
                <th class="text-left px-2 py-1" style="width:60px;">Type</th>
                <th class="text-left px-2 py-1" style="width:120px;">Company</th>
                <th class="text-left px-2 py-1" style="width:130px;">QID</th>
                <th class="text-left px-2 py-1" style="width:120px;">Bank</th>
                <th class="text-left px-2 py-1" style="width:240px;">Account</th>
                <th class="text-left px-2 py-1" style="width:320px;">IBAN</th>
                <th class="text-right px-2 py-1" style="width:130px;">Monthly</th>
                <th class="text-right px-2 py-1" style="width:130px;">Balance</th>
              </tr>
            </thead>
            <tbody>
              ${list.map((e,i)=>row(e,i)).join('')}
            </tbody>
          </table>
        </div>
      </div>`;
  };

  frame.innerHTML = `
  <div class="payroll-report-expanded" data-version="v20251008-14">
    <div class="flex items-start justify-between mb-6">
      <div>
        <div class="text-sm font-semibold uppercase tracking-wider text-gray-500">Payroll Report</div>
        <div class="text-2xl font-extrabold text-gray-900 leading-tight">${escHtml(monthTitle)}</div>
      </div>
      <div class="text-right">
        <div class="text-sm font-semibold uppercase tracking-wider text-gray-500 mb-1">Totals</div>
        <div class="text-lg font-extrabold text-gray-900">Payroll ${fmtCurrency(summary.salary)}</div>
        <div class="text-lg font-extrabold text-indigo-700">Outstanding ${fmtCurrency(summary.outstanding)}</div>
      </div>
    </div>
    <div class="grid grid-cols-4 gap-4 mb-8 text-sm">
      <div class="border border-gray-200 rounded-xl p-4 bg-white shadow-sm"><div class="text-gray-500 text-xs uppercase mb-1 tracking-wide">Employees</div><div class="font-extrabold text-xl">${summary.count}</div></div>
      <div class="border border-gray-200 rounded-xl p-4 bg-white shadow-sm"><div class="text-gray-500 text-xs uppercase mb-1 tracking-wide">Permanent</div><div class="font-extrabold text-xl">${summary.perm}</div></div>
      <div class="border border-gray-200 rounded-xl p-4 bg-white shadow-sm"><div class="text-gray-500 text-xs uppercase mb-1 tracking-wide">Temporary</div><div class="font-extrabold text-xl">${summary.temps}</div></div>
      <div class="border border-gray-200 rounded-xl p-4 bg-white shadow-sm"><div class="text-gray-500 text-xs uppercase mb-1 tracking-wide">Generated</div><div class="font-extrabold text-base">${new Date().toLocaleString()}</div></div>
    </div>
    ${section('Permanent Employees', perm)}
    ${section('Temporary Employees', temps)}
  </div>
  <style>
  /* Compact Payroll Monthly Report sizing (v20251008-14) */
    #payrollFrame .payroll-report-expanded { font-size:12.5px !important; line-height:1.35 !important; }
    #payrollFrame .payroll-report-expanded h3 { font-size:15px !important; }
    #payrollFrame .payroll-report-expanded table { font-size:12px !important; }
    #payrollFrame .payroll-report-expanded thead th { font-size:10px !important; padding:6px 6px !important; letter-spacing:.35px; }
    #payrollFrame .payroll-report-expanded tbody td { padding:4px 6px !important; font-size:11px !important; }
    #payrollFrame .payroll-report-expanded .mono-text { font-size:11px !important; }
  #payrollFrame .payroll-report-expanded .nowrap { white-space:nowrap !important; }
  #payrollFrame .payroll-report-expanded td { vertical-align:top !important; }
    #payrollFrame .payroll-report-expanded .text-2xl { font-size:22px !important; }
    #payrollFrame .payroll-report-expanded .text-lg { font-size:16px !important; }
    #payrollFrame .payroll-report-expanded .grid > div .font-extrabold { font-size:18px !important; }
    #payrollFrame .payroll-report-expanded .grid.grid-cols-4 > div { padding:14px 12px !important; }
    #payrollFrame .payroll-report-expanded .overflow-x-auto { overscroll-behavior:contain; }
    #payrollFrame .payroll-report-expanded .compact-badge { font-size:10px !important; padding:2px 5px !important; font-weight:600 !important; border-radius:6px !important; }
    #payrollFrame .payroll-report-expanded .mb-8 { margin-bottom:2rem !important; }
    @media (max-width:1400px){
      #payrollFrame .payroll-report-expanded { font-size:12px !important; }
      #payrollFrame .payroll-report-expanded table { font-size:11.5px !important; }
      #payrollFrame .payroll-report-expanded thead th { font-size:10px !important; }
    }
    @media (max-width:1100px){
      #payrollFrame .payroll-report-expanded { font-size:11.5px !important; }
      #payrollFrame .payroll-report-expanded table { font-size:11px !important; }
      #payrollFrame .payroll-report-expanded thead th { font-size:9.5px !important; }
    }
  </style>
  `;
  console.log('[PayrollModule] renderMonthReportInto executed (enlarged v20251008-04, badge removed)');
}

// Build a professional, standalone HTML document for printing the payroll (A4 landscape) from precomputed data
function buildPayrollPrintHtmlFromData({ ym, monthTitle, employees, perm, temps, summary }) {
  const fmt = fmtCurrency;
  const esc = escHtml;
  const row = (emp, idx) => {
    const type = emp._type === 'Temporary' ? 'TEMP' : 'PERM';
    return `
      <tr>
        <td>${idx + 1}</td>
        <td class="emp">${esc(emp.name || '')}</td>
        <td><span class="badge ${type==='TEMP'?'temp':'perm'}">${type}</span></td>
        <td>${esc(emp.department || '-')}</td>
        <td class="mono">${esc(emp.qid || '-')}</td>
        <td>${esc(emp.bankName || '-')}</td>
        <td class="mono">${esc(emp.bankAccountNumber || '-')}</td>
        <td class="mono">${esc(emp.bankIban || '-')}</td>
        <td class="num">${fmt(emp._basic || 0)}</td>
        <td class="num">${fmt(emp._balance || 0)}</td>
      </tr>`;
  };
  const table = (title, list) => !list.length ? '' : `
    <div class="section">
      <div class="section-title">${esc(title)} <span class="muted">(${list.length})</span></div>
      <table class="grid">
        <thead>
          <tr>
            <th>#</th>
            <th>Name</th>
            <th>Type</th>
            <th>Company</th>
            <th>QID</th>
            <th>Bank</th>
            <th>Account</th>
            <th>IBAN</th>
            <th>Monthly</th>
            <th>Balance</th>
          </tr>
        </thead>
        <tbody>
          ${list.map((e,i)=>row(e,i)).join('')}
        </tbody>
      </table>
    </div>`;

  const html = `<!DOCTYPE html>
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Payroll Report — ${esc(monthTitle)}</title>
    <style>
      @page { size: A4 landscape; margin: 10mm; }
      html, body { margin:0; padding:0; }
      body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color:#111827; }
      .report { padding: 0; }
      .header { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom: 10px; }
      .brand { font-weight: 800; font-size: 18px; color:#4F46E5; letter-spacing: .2px; }
      .title-wrap { text-align:right; }
      .title { font-weight: 800; font-size: 16px; margin: 0; }
      .subtitle { font-size: 11px; color:#6B7280; margin-top: 2px; }
      .summary { display:grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin: 8px 0 10px; }
      .card { border:1px solid #e5e7eb; border-radius:6px; padding:8px; }
      .card .label { font-size:10px; color:#6B7280; text-transform:uppercase; letter-spacing:.02em; }
      .card .value { font-size:14px; font-weight:800; }
      .section { margin-top: 10px; }
      .section-title { font-size: 11px; font-weight: 800; text-transform: uppercase; color:#374151; margin-bottom: 4px; }
      .section-title .muted { color:#6B7280; font-weight:600; }
      table.grid { width:100%; border-collapse: collapse; font-size: 9px; }
      table.grid th, table.grid td { border:1px solid #E5E7EB; padding: 3px 4px; vertical-align: top; }
      table.grid thead th { background:#F3F4F6; color:#374151; text-align:left; }
      td.num { text-align: right; font-variant-numeric: tabular-nums; font-weight: 700; }
      td.emp { font-weight: 700; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 9px; letter-spacing: -0.2px; word-break: break-word; }
      .badge { display:inline-block; padding:1px 4px; font-size:8px; border-radius:3px; font-weight:700; }
      .badge.perm { background:#ECFDF5; color:#065F46; }
      .badge.temp { background:#FEF3C7; color:#92400E; }
      .signatures { display:flex; gap: 16px; margin-top: 12px; }
      .sign { flex:1; border-top:1px solid #E5E7EB; padding-top:6px; font-size:10px; }
      .footer { margin-top: 8px; color:#6B7280; font-size: 10px; text-align:right; }
      table.grid, table.grid tr, table.grid td, table.grid th { page-break-inside: avoid; }
    </style>
  </head>
  <body>
    <div class="report">
      <div class="header">
        <div class="brand">Payroll Report</div>
        <div class="title-wrap">
          <div class="title">${esc(monthTitle)}</div>
          <div class="subtitle">${summary.count} employees • Total Payroll ${fmt(summary.salary)} • Total Outstanding ${fmt(summary.outstanding)}</div>
        </div>
      </div>
      <div class="summary">
        <div class="card"><div class="label">Employees</div><div class="value">${summary.count}</div></div>
        <div class="card"><div class="label">Permanent</div><div class="value">${summary.perm}</div></div>
        <div class="card"><div class="label">Temporary</div><div class="value">${summary.temps}</div></div>
        <div class="card"><div class="label">Total Payroll</div><div class="value">${fmt(summary.salary)}</div></div>
      </div>
      ${table('Permanent Employees', perm)}
      ${table('Temporary Employees', temps)}
      <div class="signatures">
        <div class="sign">Prepared by</div>
        <div class="sign">Reviewed by</div>
        <div class="sign">Approved by</div>
      </div>
      <div class="footer">Generated on ${new Date().toLocaleString()}</div>
    </div>
    <script>
      (function(){
        function afterPaint(cb){ try{ requestAnimationFrame(()=>requestAnimationFrame(cb)); }catch(_){ setTimeout(cb,200);} }
        function doPrint(){ try{ window.focus(); }catch(_){} try{ window.print(); }catch(_){} }
        window.addEventListener('load', function(){ afterPaint(function(){ setTimeout(doPrint, 200); }); });
        window.onafterprint = function(){ try{ window.close(); }catch(_){} };
        setTimeout(function(){ try{ if (document.readyState==='complete') { afterPaint(function(){ setTimeout(doPrint,200); }); } }catch(_){} }, 1200);
      })();
    </script>
  </body>
  </html>`;
  return html;
}

// Build a professional, standalone HTML document for printing the payroll (A4 landscape)
function buildPayrollPrintHtml({ ym, monthTitle, combined, byType, balancesMap, totals }) {
  const fmt = (n) => `QAR ${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const esc = (s) => String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/'/g, '&#39;');
  const row = (emp, idx) => {
    const bal = balancesMap.get(emp.id) ?? 0;
    const type = emp._type === 'Temporary' ? 'TEMP' : 'PERM';
    return `
      <tr>
        <td>${idx + 1}</td>
        <td class="emp">${esc(emp.name || '')}</td>
        <td><span class="badge ${type==='TEMP'?'temp':'perm'}">${type}</span></td>
        <td>${esc(emp.department || '-')}</td>
        <td class="mono">${esc(emp.qid || '-')}</td>
        <td>${esc(emp.bankName || '-')}</td>
        <td class="mono">${esc(emp.bankAccountNumber || '-')}</td>
        <td class="mono">${esc(emp.bankIban || '-')}</td>
        <td class="num">${fmt(emp.salary || 0)}</td>
        <td class="num">${fmt(bal)}</td>
      </tr>`;
  };
  const table = (title, list) => !list.length ? '' : `
    <div class="section">
      <div class="section-title">${esc(title)} <span class="muted">(${list.length})</span></div>
      <table class="grid">
        <thead>
          <tr>
            <th>#</th>
            <th>Name</th>
            <th>Type</th>
            <th>Company</th>
            <th>QID</th>
            <th>Bank</th>
            <th>Account</th>
            <th>IBAN</th>
            <th>Monthly</th>
            <th>Balance</th>
          </tr>
        </thead>
        <tbody>
          ${list.map((e,i)=>row(e,i)).join('')}
        </tbody>
      </table>
    </div>`;

  const html = `<!DOCTYPE html>
  <html>
  <head>
    <meta charset="utf-8" />
    <title>Payroll Report — ${esc(monthTitle)}</title>
    <style>
      @page { size: A4 landscape; margin: 10mm; }
      html, body { margin:0; padding:0; }
      body { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; color:#111827; }
      .report { padding: 0; }
      .header { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom: 10px; }
      .brand { font-weight: 800; font-size: 18px; color:#4F46E5; letter-spacing: .2px; }
      .title-wrap { text-align:right; }
      .title { font-weight: 800; font-size: 16px; margin: 0; }
      .subtitle { font-size: 11px; color:#6B7280; margin-top: 2px; }
      .summary { display:grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin: 8px 0 10px; }
      .card { border:1px solid #e5e7eb; border-radius:6px; padding:8px; }
      .card .label { font-size:10px; color:#6B7280; text-transform:uppercase; letter-spacing:.02em; }
      .card .value { font-size:14px; font-weight:800; }
      .section { margin-top: 10px; }
      .section-title { font-size: 11px; font-weight: 800; text-transform: uppercase; color:#374151; margin-bottom: 4px; }
      .section-title .muted { color:#6B7280; font-weight:600; }
      table.grid { width:100%; border-collapse: collapse; font-size: 9px; }
      table.grid th, table.grid td { border:1px solid #E5E7EB; padding: 3px 4px; vertical-align: top; }
      table.grid thead th { background:#F3F4F6; color:#374151; text-align:left; }
      td.num { text-align: right; font-variant-numeric: tabular-nums; font-weight: 700; }
      td.emp { font-weight: 700; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; font-size: 9px; letter-spacing: -0.2px; word-break: break-word; }
      .badge { display:inline-block; padding:1px 4px; font-size:8px; border-radius:3px; font-weight:700; }
      .badge.perm { background:#ECFDF5; color:#065F46; }
      .badge.temp { background:#FEF3C7; color:#92400E; }
      .signatures { display:flex; gap: 16px; margin-top: 12px; }
      .sign { flex:1; border-top:1px solid #E5E7EB; padding-top:6px; font-size:10px; }
      .footer { margin-top: 8px; color:#6B7280; font-size: 10px; text-align:right; }
      /* Avoid breaking rows across pages */
      table.grid, table.grid tr, table.grid td, table.grid th { page-break-inside: avoid; }
    </style>
  </head>
  <body>
    <div class="report">
      <div class="header">
        <div class="brand">Payroll Report</div>
        <div class="title-wrap">
          <div class="title">${esc(monthTitle)}</div>
          <div class="subtitle">${combined.length} employees • Total Payroll ${fmt(totals.salary)} • Total Outstanding ${fmt(totals.balance)}</div>
        </div>
      </div>
      <div class="summary">
        <div class="card"><div class="label">Employees</div><div class="value">${combined.length}</div></div>
        <div class="card"><div class="label">Permanent</div><div class="value">${byType.Employee.length}</div></div>
        <div class="card"><div class="label">Temporary</div><div class="value">${byType.Temporary.length}</div></div>
        <div class="card"><div class="label">Total Payroll</div><div class="value">${fmt(totals.salary)}</div></div>
      </div>
      ${table('Permanent Employees', byType.Employee)}
      ${table('Temporary Employees', byType.Temporary)}
      <div class="signatures">
        <div class="sign">Prepared by</div>
        <div class="sign">Reviewed by</div>
        <div class="sign">Approved by</div>
      </div>
      <div class="footer">Generated on ${new Date().toLocaleString()}</div>
    </div>
    <script>
      // Robust auto-print after the document paints to avoid blank pages
      (function() {
        function afterPaint(cb) {
          try {
            requestAnimationFrame(function() { requestAnimationFrame(cb); });
          } catch (_) { setTimeout(cb, 200); }
        }
        function doPrint() {
          try { window.focus(); } catch(_) {}
          try { window.print(); } catch(_) {}
        }
        window.addEventListener('load', function() {
          afterPaint(function(){ setTimeout(doPrint, 200); });
        });
        window.onafterprint = function() { try { window.close(); } catch(_) {} };
        // Safety net: if load didn't fire for some reason, attempt after a short delay
        setTimeout(function(){ try { if (document.readyState === 'complete') { afterPaint(function(){ setTimeout(doPrint, 200); }); } } catch(_){} }, 1200);
      })();
    </script>
  </body>
  </html>`;
  return html;
}

// Compute balances for a given month and return a Map<employeeId, balance>
async function computeMonthlyBalancesForList(list, ym) {
  const out = new Map();
  if (!ym || !/^\d{4}-\d{2}$/.test(ym)) {
    list.forEach(e => out.set(e.id, 0));
    return out;
  }
  try {
    const [yStr, mStr] = ym.split('-');
    const y = Number(yStr), m = Number(mStr);
    const prev = new Date(y, m - 2, 1); // previous month
    const prevYm = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2,'0')}`;
    const { collection, getDocs, query, where, doc, getDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
    const { db } = await import('../firebase-config.js');
    for (const emp of list) {
      try {
        // Previous month carryover
        let carryover = 0;
        try {
          const prevRef = doc(db, 'balances', `${emp.id}_${prevYm}`);
          const prevDoc = await getDoc(prevRef);
          if (prevDoc.exists()) carryover = Number(prevDoc.data().balance || 0) || 0;
        } catch {}

        // Payslips for selected month
        const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
        const slipsThisMonth = psSnap.docs.map(d => d.data()).filter(d => (d.period || '') === ym);
        const basic = slipsThisMonth.length
          ? Number(slipsThisMonth.sort((a,b) => (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0].basic || emp.salary || 0)
          : Number(emp.salary || 0);
        const advances = slipsThisMonth.reduce((s, p) => s + Number(p.advance || 0), 0);

        // Payments within selected month (exclude advances)
        const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId', '==', emp.id)));
        const paymentsThisMonth = paySnap.docs
          .map(d => d.data())
          .filter(p => (p.date || '').startsWith(ym + '-') && !Boolean(p.isAdvance))
          .reduce((s, p) => s + Number(p.amount || 0) + Number(p.overtime || 0), 0);

        const balance = Math.max(0, Number(carryover) + basic - advances - paymentsThisMonth);
        out.set(emp.id, balance);
      } catch {
        out.set(emp.id, 0);
      }
    }
  } catch {
    list.forEach(e => out.set(e.id, 0));
  }
  return out;
}

// Open a dedicated window and print the professional report
export async function printPayrollProfessional({ getEmployees, getTemporaryEmployees }) {
  const btn = document.getElementById('printPayrollBtn') || document.querySelector('[onclick*="printPayrollProfessional"]');
  if (btn) btn.disabled = true;

  try {
    // Ensure a month is selected
    const monthEl = document.getElementById('payrollMonth');
    const ym = monthEl?.value || '';
    if (!ym) { alert('Please select a month'); return; }
    const monthTitle = new Date(ym + '-01').toLocaleDateString(undefined, { year: 'numeric', month: 'long' });

    // Build data first (single pass)
    const data = await buildMonthReportData({ ym, getEmployees, getTemporaryEmployees });

    // Open the print window immediately (sync with user gesture) to avoid popup blockers
    const printWindow = window.open('about:blank', 'payroll-print', 'width=1200,height=800');
    if (!printWindow) { alert('Please allow pop-ups to print the payroll report'); return; }
    try {
      const preDoc = printWindow.document;
      preDoc.open();
      preDoc.write(`<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Preparing Payroll…</title>
        <style>html,body{height:100%}body{margin:0;display:flex;align-items:center;justify-content:center;font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;color:#334155;background:#f8fafc} .box{padding:16px 18px;border:1px solid #e5e7eb;border-radius:10px;background:#fff;box-shadow:0 8px 20px rgba(0,0,0,.06)} .spin{width:14px;height:14px;border:2px solid #e5e7eb;border-top-color:#4f46e5;border-radius:50%;display:inline-block;animation:spin .7s linear infinite;margin-right:8px;vertical-align:middle}@keyframes spin{to{transform:rotate(360deg)}} </style>
      </head><body><div class="box"><span class="spin"></span>Preparing payroll report…</div></body></html>`);
      preDoc.close();
    } catch {}

    // Build full HTML document from precomputed data
    const htmlContent = buildPayrollPrintHtmlFromData({
      ym,
      monthTitle: data.monthTitle,
      employees: data.employees,
      perm: data.perm,
      temps: data.temps,
      summary: data.summary,
    });

    try {
      const doc = printWindow.document;
      doc.open();
      doc.write(htmlContent);
      doc.close();
    } catch (_) {
      // Fallback to data URL load if direct write fails
      try {
        const dataUrl = 'data:text/html;charset=utf-8,' + encodeURIComponent(htmlContent);
        printWindow.location.href = dataUrl;
      } catch {}
    }

    // Opener-side fallback: if the inline auto-print didn’t fire, poll until DOM is ready then print.
    try {
      const started = Date.now();
      const timer = setInterval(() => {
        try {
          const d = printWindow.document;
          if (!d) return;
          const ready = d.readyState === 'complete';
          const hasBody = d.body && d.body.innerHTML && d.body.innerHTML.length > 100;
          if (ready && hasBody) {
            clearInterval(timer);
            try { printWindow.focus(); } catch {}
            try { printWindow.print(); } catch {}
            // Best-effort close shortly after
            setTimeout(() => { try { printWindow.close(); } catch {} }, 1500);
          } else if (Date.now() - started > 7000) {
            clearInterval(timer);
            try { printWindow.focus(); } catch {}
            try { printWindow.print(); } catch {}
          }
        } catch {}
      }, 200);
    } catch {}
  } catch (error) {
    console.error('Print failed:', error);
    alert('Failed to print payroll report. Please try again.');
  } finally {
    if (btn) btn.disabled = false;
  }
}

function buildPayrollPrintInnerHtml({ ym, monthTitle, byType, balancesMap, totals }) {
  const esc = (s) => String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  const fmt = (n) => `QAR ${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const section = (title, list) => {
    if (!list || !list.length) return '';
    return `
      <h2>${esc(title)} (${list.length})</h2>
      <table>
        <thead>
          <tr>
            <th>#</th>
            <th>Name</th>
            <th>Company</th>
            <th>QID</th>
            <th>Bank</th>
            <th>Account</th>
            <th>IBAN</th>
            <th class="text-right">Monthly</th>
            <th class="text-right">Balance</th>
          </tr>
        </thead>
        <tbody>
          ${list.map((e,i)=>`
            <tr>
              <td>${i+1}</td>
              <td>${esc(e.name||'')}</td>
              <td>${esc(e.department||'-')}</td>
              <td class="mono">${esc(e.qid||'-')}</td>
              <td>${esc(e.bankName||'-')}</td>
              <td class="mono">${esc(e.bankAccountNumber||'-')}</td>
              <td class="mono">${esc(e.bankIban||'-')}</td>
              <td class="text-right">${fmt(e.salary||0)}</td>
              <td class="text-right">${fmt(balancesMap.get(e.id)||0)}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>`;
  };
  return `
    <div class="payroll-report">
      <div style="display:flex;align-items:flex-start;justify-content:space-between;">
        <div>
          <h1>Payroll Report</h1>
          <div style="font-size:11px;color:#6b7280;">${esc(monthTitle)} • Total Payroll ${fmt(totals.salary)} • Total Outstanding ${fmt(totals.balance)}</div>
        </div>
      </div>
      <div class="payroll-summary">
        <div class="payroll-card"><div class="label">Employees</div><div class="value">${(byType.permanent.length + byType.temporary.length)}</div></div>
        <div class="payroll-card"><div class="label">Permanent</div><div class="value">${byType.permanent.length}</div></div>
        <div class="payroll-card"><div class="label">Temporary</div><div class="value">${byType.temporary.length}</div></div>
        <div class="payroll-card"><div class="label">Total Payroll</div><div class="value">${fmt(totals.salary)}</div></div>
      </div>
      ${section('Permanent Employees', byType.permanent)}
      ${section('Temporary Employees', byType.temporary)}
      <div class="footer">Generated on ${new Date().toLocaleString()}</div>
    </div>`;
}

// Compute current salary balance for each employee based on payslips minus payments of the current month
async function computeAndFillCurrentBalances(list) {
  try {
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    // Lazy-load Firestore API from global (script.js imports the SDK in the main bundle)
  const { collection, getDocs, query, where, doc, getDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
    const { db } = await import('../firebase-config.js');
  const fmt = (n) => `QAR ${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
    for (const emp of list) {
      try {
        // Payslips for the employee (used to determine basic for the period and advances)
        const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
        const slipsThisMonth = psSnap.docs.map(d => d.data()).filter(d => (d.period || '') === ym);
        const basic = slipsThisMonth.length
          ? Number(slipsThisMonth.sort((a,b) => (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0].basic || emp.salary || 0)
          : Number(emp.salary || 0);
        const advances = slipsThisMonth.reduce((s, p) => s + Number(p.advance || 0), 0);

        // Salary payments for the employee within the current month (exclude advances if any)
        const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId', '==', emp.id)));
        const paymentsThisMonth = paySnap.docs
          .map(d => d.data())
          .filter(p => (p.date || '').startsWith(ym + '-') && !Boolean(p.isAdvance))
          .reduce((s, p) => s + Number(p.amount || 0) + Number(p.overtime || 0), 0);
        // Carryover from previous month (balances collection snapshot)
        let carryover = 0;
        try {
          const prevDate = new Date(now.getFullYear(), now.getMonth() - 1, 1);
          const prevYm = `${prevDate.getFullYear()}-${String(prevDate.getMonth() + 1).padStart(2, '0')}`;
          const prevRef = doc(db, 'balances', `${emp.id}_${prevYm}`);
          const prevDoc = await getDoc(prevRef);
          if (prevDoc.exists()) carryover = Number(prevDoc.data().balance || 0) || 0;
        } catch {}

        const balance = Math.max(0, Number(carryover) + basic - advances - paymentsThisMonth);
        const cell = document.querySelector(`[data-balance-for="${emp.id}"]`);
        if (cell) cell.textContent = fmt(balance);
      } catch (e) {
        const cell = document.querySelector(`[data-balance-for="${emp.id}"]`);
        if (cell) cell.textContent = '—';
      }
    }
  } catch (e) {
    console.warn('computeAndFillCurrentBalances overall failure', e);
  }
}

export function renderPayrollFrame({ getEmployees, getTemporaryEmployees, month = null }) {
  const frame = document.getElementById('payrollFrame');
  if (!frame) return;
  const monthEl = document.getElementById('payrollMonth');
  let ym = month || (monthEl && monthEl.value ? monthEl.value : '');
  // If still no month, fallback to current month and persist to input if present
  if (!ym) {
    const now = new Date();
    const defaultYm = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    if (monthEl) monthEl.value = defaultYm;
    ym = defaultYm;
  }
  // Show skeleton while loading
  frame.innerHTML = `
    <div class="flex items-center gap-2 text-gray-600 text-sm"><span class="inline-block w-3 h-3 border-2 border-gray-300 border-t-indigo-600 rounded-full animate-spin"></span> Building month report…</div>
  `;
  // Build and render data
  buildMonthReportData({ ym, getEmployees, getTemporaryEmployees })
    .then((data) => {
      renderMonthReportInto(frame, data);
    })
    .catch((e) => {
      console.warn('Month report build failed', e);
      frame.innerHTML = `<div class="text-sm text-rose-600">Failed to build month report. Please try again.</div>`;
    });
}

// Compute balances for the report frame for a specific month (ym = YYYY-MM)
async function computeAndFillReportBalancesForMonth(list, ym) {
  if (!ym || !/^\d{4}-\d{2}$/.test(ym)) return;
  try {
    const [yStr, mStr] = ym.split('-');
    const y = Number(yStr), m = Number(mStr);
    const prev = new Date(y, m - 2, 1); // previous month
    const prevYm = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, '0')}`;
    const { collection, getDocs, query, where, doc, getDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
    const { db } = await import('../firebase-config.js');
  const fmt = (n) => `QAR ${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
    for (const emp of list) {
      try {
        // Previous month carryover
        let carryover = 0;
        try {
          const prevRef = doc(db, 'balances', `${emp.id}_${prevYm}`);
          const prevDoc = await getDoc(prevRef);
          if (prevDoc.exists()) carryover = Number(prevDoc.data().balance || 0) || 0;
        } catch {}

        // Payslips for selected month
        const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
        const slipsThisMonth = psSnap.docs.map(d => d.data()).filter(d => (d.period || '') === ym);
        const basic = slipsThisMonth.length
          ? Number(slipsThisMonth.sort((a,b) => (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0].basic || emp.salary || 0)
          : Number(emp.salary || 0);
        const advances = slipsThisMonth.reduce((s, p) => s + Number(p.advance || 0), 0);

        // Payments within selected month (exclude advances)
        const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId', '==', emp.id)));
        const paymentsThisMonth = paySnap.docs
          .map(d => d.data())
          .filter(p => (p.date || '').startsWith(ym + '-') && !Boolean(p.isAdvance))
          .reduce((s, p) => s + Number(p.amount || 0) + Number(p.overtime || 0), 0);

        const balance = Math.max(0, Number(carryover) + basic - advances - paymentsThisMonth);
        const cell = document.querySelector(`[data-report-balance-for="${emp.id}"]`);
        if (cell) cell.textContent = fmt(balance);
      } catch (e) {
        const cell = document.querySelector(`[data-report-balance-for="${emp.id}"]`);
        if (cell) cell.textContent = '—';
      }
    }
  } catch (e) {
    console.warn('computeAndFillReportBalancesForMonth failure', e);
  }
}

export function setPayrollSubTab(which, deps) {
  const tablePane = document.getElementById('payrollTabTable');
  const reportPane = document.getElementById('payrollTabReport');
  const tableBtn = document.getElementById('payrollTabTableBtn');
  const reportBtn = document.getElementById('payrollTabReportBtn');
  if (!tablePane || !reportPane || !tableBtn || !reportBtn) return;

  const activateBtn = (btn, active) => {
    if (active) {
      btn.classList.add('font-semibold', 'text-indigo-600', 'border-b-2', 'border-indigo-600');
      btn.classList.remove('text-gray-600');
    } else {
      btn.classList.remove('font-semibold', 'text-indigo-600', 'border-b-2', 'border-indigo-600');
      btn.classList.add('text-gray-600');
    }
  };

  if (which === 'report') {
    tablePane.style.display = 'none';
    reportPane.style.display = '';
    activateBtn(tableBtn, false);
    activateBtn(reportBtn, true);
    // Ensure month input has a sensible default
    try {
      const monthEl = document.getElementById('payrollMonth');
      if (monthEl && !monthEl.value) {
        const now = new Date();
        monthEl.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
      }
      renderPayrollFrame({ ...deps, month: monthEl ? monthEl.value : null });
    } catch {
      // Fallback without month if something goes wrong
      renderPayrollFrame(deps);
    }
    currentPayrollSubTab = 'report';
  } else {
    tablePane.style.display = '';
    reportPane.style.display = 'none';
    activateBtn(tableBtn, true);
    activateBtn(reportBtn, false);
    renderPayrollTable(deps);
    currentPayrollSubTab = 'table';
  }
}

export async function exportMonthReportCsv({ ym, getEmployees, getTemporaryEmployees }) {
  try {
    const monthEl = document.getElementById('payrollMonth');
    const selYm = ym || monthEl?.value || '';
    if (!/^\d{4}-\d{2}$/.test(selYm)) { alert('Please select a valid month'); return; }
    const data = await buildMonthReportData({ ym: selYm, getEmployees, getTemporaryEmployees });
    const headers = ['Name','Type','Company','QID','Bank Name','Account Number','IBAN','Monthly Basic','Advances','Paid','Carryover Prev','Outstanding Balance'];
    const rows = data.employees.map(e => [
      quoteCsv(e.name || ''),
      quoteCsv(e._type || ''),
      quoteCsv(e.department || ''),
      quoteCsv(e.qid || ''),
      quoteCsv(e.bankName || ''),
      quoteCsv(e.bankAccountNumber || ''),
      quoteCsv(e.bankIban || ''),
      String(Number(e._basic || 0)),
      String(Number(e._advances || 0)),
      String(Number(e._paid || 0)),
      String(Number(e._carry || 0)),
      String(Number(e._balance || 0))
    ].join(','));
    const csv = [headers.join(','), ...rows].join('\r\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `payroll-${selYm}.csv`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  } catch (e) {
    console.warn('CSV export failed', e);
    alert('Failed to export CSV.');
  }
}

// Backward-compatible export for existing imports (script.js imports exportPayrollCsv)
export function exportPayrollCsv(employees, temporaryEmployees) {
  try {
    const monthEl = document.getElementById('payrollMonth');
    const ym = monthEl?.value || '';
    return exportMonthReportCsv({ ym, getEmployees: () => employees || [], getTemporaryEmployees: () => temporaryEmployees || [] });
  } catch (e) {
    console.warn('exportPayrollCsv fallback failed', e);
  }
}

function quoteCsv(val) {
  const s = String(val ?? '');
  if (/[",\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}
