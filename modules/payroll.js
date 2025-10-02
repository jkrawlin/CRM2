// Payroll module: table rendering, report frame, sorting, sub-tab handling, CSV export
import { maskAccount, formatDate } from './utils.js';

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

  // Export/Print controls
  const exportCsvBtn = document.getElementById('exportPayrollCsvBtn');
  const printBtn = document.getElementById('printPayrollBtn');
  if (exportCsvBtn) exportCsvBtn.addEventListener('click', () => exportPayrollCsv(getEmployees(), getTemporaryEmployees()));
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
  if (totalEl) totalEl.textContent = `$${total.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

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
        <td class="px-2 py-1 font-semibold text-gray-900 truncate">${emp.name}</td>
        <td class="px-2 py-1">${typeBadge(emp._type)}</td>
        <td class="px-2 py-1 truncate">${emp.department || '-'}</td>
        <td class="px-2 py-1 text-right tabular-nums">$${Number(emp.salary || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
        <td class="px-2 py-1 text-right tabular-nums" data-balance-for="${emp.id}">—</td>
  <td class="px-2 py-1 whitespace-nowrap w-[130px] min-w-[130px]">${formatDate(emp.joinDate)}</td>
        <td class="px-2 py-1">${emp.qid || '-'}</td>
        <td class="px-2 py-1 text-center">
          <div class="flex flex-wrap items-center justify-center gap-1">
            <button class="btn btn-primary btn-compact" onclick="openPayslipForm('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')">
              <i class="fas fa-file-invoice"></i> Generate Payslip
            </button>
            <button class="btn btn-secondary btn-compact" onclick="openPaymentForm('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')">
              <i class="fas fa-money-bill-wave"></i> Pay Salary
            </button>
            <button class="btn btn-secondary btn-compact" onclick="viewPayroll('${emp.id}', '${emp._type === 'Temporary' ? 'temporary' : 'employees'}')">
              <i class="fas fa-eye"></i> View
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

// Build a professional, standalone HTML document for printing the payroll (A4 landscape)
function buildPayrollPrintHtml({ ym, monthTitle, combined, byType, balancesMap, totals }) {
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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
          <div class="subtitle">${combined.length} employees • Total Payroll ${fmt(totals.totalSalaries)} • Total Outstanding ${fmt(totals.totalBalances)}</div>
        </div>
      </div>
      <div class="summary">
        <div class="card"><div class="label">Employees</div><div class="value">${combined.length}</div></div>
        <div class="card"><div class="label">Permanent</div><div class="value">${byType.Employee.length}</div></div>
        <div class="card"><div class="label">Temporary</div><div class="value">${byType.Temporary.length}</div></div>
        <div class="card"><div class="label">Total Payroll</div><div class="value">${fmt(totals.totalSalaries)}</div></div>
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
      window.addEventListener('load', function() {
        setTimeout(function(){ window.print(); setTimeout(function(){ window.close(); }, 300); }, 50);
      });
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
  try {
    // Determine month
    const monthEl = document.getElementById('payrollMonth');
    let ym = monthEl && monthEl.value ? monthEl.value : '';
    if (!ym) {
      const now = new Date();
      ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2,'0')}`;
      if (monthEl) monthEl.value = ym;
    }
    const [yr, mo] = ym.split('-');
    const monthTitle = ym ? new Date(Number(yr), Number(mo) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Month';

    // Build data sets
    const combined = [
      ...getEmployees().map(e => ({ ...e, _type: 'Employee' })),
      ...getTemporaryEmployees().map(e => ({ ...e, _type: 'Temporary' })),
    ].sort((a,b)=> (a.name||'').localeCompare(b.name||''));

    const byType = {
      Employee: combined.filter(e => e._type === 'Employee'),
      Temporary: combined.filter(e => e._type === 'Temporary')
    };

    const balancesMap = await computeMonthlyBalancesForList(combined, ym);
    const totals = {
      totalSalaries: combined.reduce((s,e)=> s + Number(e.salary||0), 0),
      totalBalances: combined.reduce((s,e)=> s + Number(balancesMap.get(e.id) || 0), 0),
    };

    const html = buildPayrollPrintHtml({ ym, monthTitle, combined, byType, balancesMap, totals });
    const w = window.open('', 'payrollPrint', 'noopener,noreferrer,width=1200,height=800');
    if (!w) { window.print(); return; }
    w.document.open();
    w.document.write(html);
    w.document.close();
  } catch (e) {
    try { console.warn('Payroll print failed, falling back to window.print()', e); } catch {}
    window.print();
  }
}

// Compute current salary balance for each employee based on payslips minus payments of the current month
async function computeAndFillCurrentBalances(list) {
  try {
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    // Lazy-load Firestore API from global (script.js imports the SDK in the main bundle)
  const { collection, getDocs, query, where, doc, getDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
    const { db } = await import('../firebase-config.js');
    const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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
  const [yr, mo] = ym ? ym.split('-') : ['',''];

  const combined = [
    ...getEmployees().map(e => ({ ...e, _type: 'Employee' })),
    ...getTemporaryEmployees().map(e => ({ ...e, _type: 'Temporary' })),
  ].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  const total = combined.reduce((sum, e) => sum + Number(e.salary || 0), 0);
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const monthTitle = ym ? new Date(Number(yr), Number(mo) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Month';

  const byType = {
    Employee: combined.filter(e => e._type === 'Employee'),
    Temporary: combined.filter(e => e._type === 'Temporary')
  };

  const badge = (t) => {
    const isTemp = t === 'Temporary';
    const cls = isTemp ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800';
    return `<span class="compact-badge ${cls}">${isTemp ? 'TEMP' : 'PERM'}</span>`;
  };

  const renderGroup = (label, items) => {
    if (!items.length) return '';
    return `
      <div class="mb-4">
        <h3 class="text-xs font-bold text-gray-700 mb-2 uppercase">${label} (${items.length})</h3>
        <div class="overflow-x-hidden">
          <table class="w-full" style="font-size:9px;">
            <thead>
              <tr class="bg-gray-50 text-gray-600 uppercase font-semibold" style="font-size:8px;">
                <th class="text-left" style="width:20px;padding:2px">#</th>
                <th class="text-left" style="width:120px;padding:2px">NAME</th>
                <th class="text-left" style="width:50px;padding:2px">TYPE</th>
                <th class="text-left" style="width:60px;padding:2px">DEPT</th>
                <th class="text-left" style="width:70px;padding:2px">QID</th>
                <th class="text-left" style="width:60px;padding:2px">BANK</th>
                <th class="text-left" style="padding:2px">ACCOUNT</th>
                <th class="text-left" style="padding:2px">IBAN</th>
                <th class="text-right" style="width:70px;padding:2px">SALARY</th>
                <th class="text-right" style="width:70px;padding:2px">BALANCE</th>
              </tr>
            </thead>
            <tbody>
              ${items.map((emp, idx) => {
                const showAccount = emp.bankAccountNumber || '-';
                const showIban = emp.bankIban || '-';
                return `
                  <tr class="border-b border-gray-100 hover:bg-gray-50">
                    <td class="text-gray-500" style="padding:2px;font-size:8px">${idx + 1}</td>
                    <td style="padding:2px;font-size:8px">
                      <div class="font-semibold text-gray-900 truncate" title="${emp.name}">${emp.name}</div>
                    </td>
                    <td style="padding:2px">${badge(emp._type)}</td>
                    <td class="text-gray-600 truncate" style="padding:2px;font-size:8px" title="${emp.department || '-'}">${emp.department || '-'}</td>
                    <td class="text-gray-600 mono-text" style="padding:2px;font-size:8px">${emp.qid || '-'}</td>
                    <td class="text-gray-600 truncate" style="padding:2px;font-size:8px" title="${emp.bankName || '-'}">${emp.bankName || '-'}</td>
                    <td class="text-gray-600 mono-text" style="padding:2px;font-size:8px;word-break:break-all">${showAccount}</td>
                    <td class="text-gray-600 mono-text" style="padding:2px;font-size:8px;word-break:break-all">${showIban}</td>
                    <td class="text-right font-mono font-semibold" style="padding:2px;font-size:8px">${fmt(emp.salary || 0)}</td>
                    <td class="text-right font-mono font-semibold" style="padding:2px;font-size:8px" data-report-balance-for="${emp.id}">-</td>
                  </tr>
                `;
              }).join('')}
            </tbody>
          </table>
        </div>
      </div>
    `;
  };

  const groupsHtml = [
    renderGroup('Permanent Employees', byType.Employee),
    renderGroup('Temporary Employees', byType.Temporary)
  ].join('');

  frame.innerHTML = `
    <div class="print:hidden flex items-start justify-between mb-3">
      <div>
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Payroll Report</div>
        <div class="text-lg font-extrabold text-gray-900">${monthTitle}</div>
      </div>
      <div class="text-right">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Total</div>
        <div class="text-lg font-extrabold text-gray-900">${fmt(total)}</div>
      </div>
    </div>
    <div class="hidden print:block text-center mb-2">
      <h1 class="text-sm font-bold">PAYROLL REPORT - ${monthTitle.toUpperCase()}</h1>
      <div class="text-xs">Total Payroll: ${fmt(total)}</div>
    </div>
    ${groupsHtml}
    <div class="print:hidden text-xs text-gray-500 mt-4">
      Generated on ${new Date().toLocaleDateString()} • ${combined.length} Total Employees
    </div>
  `;
  // After rendering, compute and populate balances for the selected month
  try { computeAndFillReportBalancesForMonth(combined, ym).catch(()=>{}); } catch {}
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
    const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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

export function exportPayrollCsv(employees, temporaryEmployees) {
  const combined = [
    ...employees.map(e => ({ ...e, _type: 'Employee' })),
    ...temporaryEmployees.map(e => ({ ...e, _type: 'Temporary' })),
  ].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  const headers = ['Name','Type','Company','QID','Bank Name','Account Number','IBAN','Monthly Salary'];
  const rows = combined.map(e => [
    quoteCsv(e.name || ''),
    quoteCsv(e._type || ''),
    quoteCsv(e.department || ''),
    quoteCsv(e.qid || ''),
    quoteCsv(e.bankName || ''),
    quoteCsv(e.bankAccountNumber || ''),
    quoteCsv(e.bankIban || ''),
    String(Number(e.salary || 0))
  ].join(','));

  const csv = [headers.join(','), ...rows].join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const now = new Date();
  const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  a.href = url;
  a.download = `payroll-${ym}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function quoteCsv(val) {
  const s = String(val ?? '');
  if (/[",\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}
