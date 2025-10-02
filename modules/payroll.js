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
  if (printBtn) printBtn.addEventListener('click', () => window.print());
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

  const badge = (t) => t === 'Temporary'
    ? '<span class="px-2 py-1 rounded text-xs font-semibold bg-amber-100 text-amber-800">Temporary</span>'
    : '<span class="px-2 py-1 rounded text-xs font-semibold bg-emerald-100 text-emerald-800">Permanent</span>';

  const renderGroup = (label, items) => {
    if (!items.length) return '';
    return `
      <div class="mb-6">
        <h3 class="text-sm font-bold text-amber-700 mb-3">${label} (${items.length})</h3>
        <div class="overflow-x-hidden">
          <table class="w-full" style="font-size:10px;">
            <thead>
              <tr class="bg-gray-50 text-gray-600 uppercase font-semibold" style="font-size:9px;">
                <th class="text-left" style="width:20px;padding:4px">#</th>
                <th class="text-left" style="width:150px;padding:4px">NAME</th>
                <th class="text-left" style="width:56px;padding:4px">TYPE</th>
                <th class="text-left" style="width:64px;padding:4px">COMPANY</th>
                <th class="text-left" style="width:72px;padding:4px">QID</th>
                <th class="text-left" style="width:64px;padding:4px">BANK</th>
                <th class="text-left" style="padding:4px">ACCOUNT</th>
                <th class="text-left" style="padding:4px">IBAN</th>
                <th class="text-right" style="width:100px;padding:4px">MONTHLY<br/>SALARY</th>
                <th class="text-right" style="width:108px;padding:4px">CURRENT<br/>SALARY<br/>BALANCE</th>
              </tr>
            </thead>
            <tbody>
              ${items.map((emp, idx) => {
                const maskAcc = (acc) => {
                  if (!acc) return '-';
                  const s = String(acc).replace(/\s+/g, '');
                  if (s.length <= 4) return s;
                  // Show first 2 and last 4 digits, fixed bullets in between
                  return `${s.slice(0,2)}••••${s.slice(-4)}`;
                };
                const maskIban = (iban) => {
                  if (!iban) return '-';
                  const s = String(iban).replace(/\s+/g, '').toUpperCase();
                  const cc = s.slice(0,2);
                  const tail = s.slice(-6);
                  // Country code + 6 bullets + last 6
                  return `${cc}••••••${tail}`;
                };
                return `
                  <tr class="border-b border-gray-100 hover:bg-gray-50">
                    <td class="text-gray-500" style="padding:4px">${idx + 1}</td>
                    <td style="padding:4px">
                      <div class="font-semibold text-gray-900" style="max-width: 150px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;" title="${emp.name}">${emp.name}</div>
                    </td>
                    <td style="padding:4px">
                      ${badge(emp._type)}
                    </td>
                    <td class="text-gray-600" style="max-width: 64px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding:4px" title="${emp.department || '-'}">${emp.department || '-'}</td>
                    <td class="text-gray-600" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size:10px; width: 72px; padding:4px">${emp.qid || '-'}</td>
                    <td class="text-gray-600" style="max-width: 64px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; padding:4px" title="${emp.bankName || '-'}">${emp.bankName || '-'}</td>
                    <td class="text-gray-600" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size:10px; white-space: nowrap; padding:4px" title="${emp.bankAccountNumber || '-'}">${maskAcc(emp.bankAccountNumber)}</td>
                    <td class="text-gray-600" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; font-size:10px; white-space: nowrap; padding:4px" title="${emp.bankIban || '-'}">${maskIban(emp.bankIban)}</td>
                    <td class="text-right font-mono tabular-nums font-semibold" style="padding:4px">${fmt(emp.salary || 0)}</td>
                    <td class="text-right font-mono tabular-nums font-semibold" style="padding:4px" data-report-balance-for="${emp.id}">-</td>
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
    <div class="flex items-start justify-between mb-4">
      <div>
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Payroll</div>
        <div class="text-xl font-extrabold text-gray-900">${monthTitle}</div>
      </div>
      <div class="text-right">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Total</div>
        <div class="text-xl font-extrabold text-gray-900">${fmt(total)}</div>
      </div>
    </div>
    ${groupsHtml}
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
