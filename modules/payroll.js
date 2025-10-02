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
  if (printBtn) printBtn.addEventListener('click', () => printPayrollProfessional({ getEmployees, getTemporaryEmployees, getSearchQuery }));
}

function buildPayrollPrintHtml({ ym, combined, total }) {
  const fmt = (n)=> `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const [yr, mo] = ym && ym.includes('-') ? ym.split('-') : [];
  const monthTitle = ym ? new Date(Number(yr), Number(mo)-1, 1).toLocaleDateString(undefined,{year:'numeric', month:'long'}) : 'Current Month';
  const header = `
    <div class="report-header">
      <div>
        <div class="brand">CRM</div>
        <h1 class="report-title">Payroll Report</h1>
        <div class="report-subtitle">${monthTitle}</div>
      </div>
      <div style="text-align:right">
        <div class="report-subtitle">Total Payroll</div>
        <div class="report-title">${fmt(total)}</div>
      </div>
    </div>
    <div class="meta">
      <div><div class="label">Generated</div><div class="value">${new Date().toLocaleString()}</div></div>
      <div><div class="label">Employees</div><div class="value">${combined.length}</div></div>
      <div><div class="label">Period</div><div class="value">${monthTitle}</div></div>
    </div>`;
  const group = (arr,label)=>{
    if (!arr.length) return '';
    const rows = arr.map((emp,idx)=>{
      return `<tr>
        <td>${idx+1}</td>
        <td><div style="max-width:160px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${emp.name}">${emp.name}</div></td>
        <td>${emp._type==='Temporary'?'TEMP':'PERM'}</td>
        <td><div style="max-width:80px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${emp.department||'-'}">${emp.department||'-'}</div></td>
        <td class="mono-text">${emp.qid||'-'}</td>
        <td><div style="max-width:80px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${emp.bankName||'-'}">${emp.bankName||'-'}</div></td>
        <td class="mono-text" style="word-break:break-word">${emp.bankAccountNumber||'-'}</td>
        <td class="mono-text" style="word-break:break-word">${emp.bankIban||'-'}</td>
        <td class="right mono-text">${fmt(emp.salary||0)}</td>
        <td class="right mono-text" data-report-balance-for="${emp.id}">-</td>
      </tr>`;
    }).join('');
    return `
      <h3 style="margin:8px 0 4px 0;font-size:11px;font-weight:800;color:#374151;text-transform:uppercase">${label} (${arr.length})</h3>
      <table>
        <colgroup>
          <col /><col /><col /><col /><col /><col /><col /><col /><col /><col />
        </colgroup>
        <thead>
          <tr>
            <th>#</th><th>NAME</th><th>TYPE</th><th>DEPT</th><th>QID</th><th>BANK</th><th>ACCOUNT</th><th>IBAN</th><th>SALARY</th><th>BALANCE</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>`;
  };
  const byType = { Employee: combined.filter(x=>x._type==='Employee'), Temporary: combined.filter(x=>x._type==='Temporary') };
  const body = `
    <section class="report">
      ${header}
      ${group(byType.Employee, 'Permanent Employees')}
      ${group(byType.Temporary, 'Temporary Employees')}
      <div class="foot">CRM • Confidential</div>
    </section>`;
  const styles = `
    <style>
      @page { size: A4 landscape; margin: 10mm; }
      html, body { margin:0; padding:0; }
      body { font-family: Inter, Arial, sans-serif; color:#111827; }
      .report-header { display:flex; align-items:flex-start; justify-content:space-between; margin-bottom: 12px; }
      .brand { font-weight: 800; color:#4F46E5; font-size: 16px; }
      .report-title { font-size: 16px; font-weight: 800; margin: 0; }
      .report-subtitle { color:#6B7280; font-size: 10px; margin-top: 2px; }
      .meta { display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 6px 12px; margin: 6px 0 10px; font-size: 10px; }
      .meta .label { color:#6B7280; }
      .meta .value { font-weight:600; color:#111827; }
      table { width: 100%; border-collapse: collapse; font-size: 9px; }
      thead th { background:#F3F4F6; color:#374151; text-transform:uppercase; letter-spacing: 0.02em; font-weight:700; font-size:8px; padding: 4px 3px; border: 1px solid #E5E7EB; }
      tbody td { border: 1px solid #E5E7EB; padding: 3px; vertical-align: top; font-size: 9px; }
      .right { text-align: right; }
      .mono-text { font-family: 'Consolas','Monaco','Courier New',monospace; font-size: 8px; letter-spacing: -0.3px; }
      .foot { margin-top: 10px; color:#6B7280; font-size: 10px; text-align: right; }
    </style>`;
  return `<!doctype html><html><head><meta charset="utf-8">${styles}<title>Payroll Report - ${monthTitle}</title></head><body>${body}</body></html>`;
}

async function printPayrollProfessional({ getEmployees, getTemporaryEmployees, getSearchQuery }) {
  try {
    // Ensure data and month
    const monthEl = document.getElementById('payrollMonth');
    let ym = monthEl ? monthEl.value : '';
    if (!/^\d{4}-\d{2}$/.test(ym)) {
      const now = new Date();
      ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    }
    const employees = getEmployees().map(e => ({ ...e, _type: 'Employee' }));
    const temps = getTemporaryEmployees().map(e => ({ ...e, _type: 'Temporary' }));
    const combined = [...employees, ...temps].sort((a,b)=> (a.name||'').localeCompare(b.name||''));
    const total = combined.reduce((s,e)=> s + Number(e.salary||0), 0);

    // Precompute balances for selected month to avoid async fill after print
    const mapBalances = new Map();
    try {
      const [yStr, mStr] = ym.split('-');
      const y = Number(yStr), m = Number(mStr);
      const prev = new Date(y, m - 2, 1);
      const prevYm = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, '0')}`;
      const { collection, getDocs, query, where, doc, getDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
      const { db } = await import('../firebase-config.js');
      for (const emp of combined) {
        let carryover = 0;
        try {
          const prevRef = doc(db, 'balances', `${emp.id}_${prevYm}`);
          const prevDoc = await getDoc(prevRef);
          if (prevDoc.exists()) carryover = Number(prevDoc.data().balance || 0) || 0;
        } catch {}
        const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
        const slipsThisMonth = psSnap.docs.map(d => d.data()).filter(d => (d.period || '') === ym);
        const basic = slipsThisMonth.length
          ? Number(slipsThisMonth.sort((a,b) => (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0].basic || emp.salary || 0)
          : Number(emp.salary || 0);
        const advances = slipsThisMonth.reduce((s, p) => s + Number(p.advance || 0), 0);
        const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId', '==', emp.id)));
        const paymentsThisMonth = paySnap.docs
          .map(d => d.data())
          .filter(p => (p.date || '').startsWith(ym + '-') && !Boolean(p.isAdvance))
          .reduce((s, p) => s + Number(p.amount || 0) + Number(p.overtime || 0), 0);
        const balance = Math.max(0, Number(carryover) + basic - advances - paymentsThisMonth);
        mapBalances.set(emp.id, balance);
      }
    } catch {}

    // Inject precomputed balances into a cloned list for printing
    const withBalances = combined.map(e => ({ ...e, __balance: mapBalances.has(e.id) ? mapBalances.get(e.id) : null }));
    const html = buildPayrollPrintHtml({ ym, combined: withBalances, total });

    // Open a dedicated print window and write content
    const w = window.open('', '_blank', 'noopener,noreferrer,width=1200,height=800');
    if (!w) { alert('Pop-up blocked. Please allow pop-ups to print.'); return; }
    w.document.open();
    // Enhance: inject balances if precomputed
    const enhanced = html.replace(/data-report-balance-for=\"(.*?)\">-</g, (m, id) => {
      const val = withBalances.find(x=>x.id===id)?.__balance;
      if (val==null) return m;
      const fmt = (n)=> `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      return `data-report-balance-for="${id}">${fmt(val)}`;
    });
    w.document.write(enhanced);
    w.document.close();
    // Ensure print after content loads
    const printAfterLoad = () => { try { w.focus(); } catch {} try { w.print(); } catch {} try { w.close(); } catch {} };
    if (w.document.readyState === 'complete') {
      setTimeout(printAfterLoad, 100);
    } else {
      w.onload = () => setTimeout(printAfterLoad, 100);
    }
  } catch (e) {
    console.warn('printPayrollProfessional failed', e);
    try { window.print(); } catch {}
  }
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
