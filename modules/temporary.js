// Temporary employees module
import { formatDate, getEmployeeStatus } from './utils.js';

let tempSortColumn = '';
let tempSortOrder = 'asc';

export function sortTemporary(column, deps) {
  if (tempSortColumn === column) {
    tempSortOrder = tempSortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    tempSortColumn = column;
    tempSortOrder = 'asc';
  }
  renderTemporaryTable(deps);
}

export function renderTemporaryTable({ getTemporaryEmployees, getSearchQuery, getDepartmentFilter, getShowTerminated }) {
  ensureTemporaryHeader();
  const tbody = document.getElementById('tempEmployeeTableBody');
  const emptyState = document.getElementById('tempEmptyState');
  if (!tbody || !emptyState) return;

  const list = getTemporaryEmployees();
  const currentSearch = (getSearchQuery && getSearchQuery()) || '';
  const currentDepartmentFilter = (getDepartmentFilter && getDepartmentFilter()) || '';
  const showTerminated = (getShowTerminated && getShowTerminated()) || true;

  const filtered = list.filter(emp => {
    const matchesDept = !currentDepartmentFilter || emp.department === currentDepartmentFilter;
    const query = (currentSearch || '').trim();
    const queryDigits = query.replace(/\D/g, '');
    const empQidDigits = String(emp.qid || '').replace(/\D/g, '');
    let matchesSearch = true;
    if (query) {
      if (queryDigits.length >= 4) {
        matchesSearch = empQidDigits.includes(queryDigits);
      } else {
  const text = `${emp.name} ${emp.email} ${emp.qid || ''} ${emp.phone || ''} ${emp.position} ${emp.department} ${emp.nationality || ''}`.toLowerCase();
        matchesSearch = text.includes(query.toLowerCase());
      }
    }
    const matchesTerm = showTerminated ? true : !emp.terminated;
    return matchesDept && matchesSearch && matchesTerm;
  });

  const sorted = [...filtered];
  if (tempSortColumn) {
    sorted.sort((a, b) => {
      if (tempSortColumn === 'status') {
        const orderMap = { active:0, valid:0, 'expiring-soon':1, expiring:1, 'expiring_soon':1, pending:2, unknown:3 };
        const derive = (emp) => {
          const raw = (emp.status || '').toString().toLowerCase();
          if (raw) return raw;
          const info = getEmployeeStatus(emp);
          return info.status === 'expiring' ? 'expiring' : 'active';
        };
        const sa = derive(a), sb = derive(b);
        const da = orderMap[sa] ?? 99, db = orderMap[sb] ?? 99;
        if (da === db) return 0;
        return tempSortOrder === 'asc' ? da - db : db - da;
      }
      let va = a[tempSortColumn];
      let vb = b[tempSortColumn];
      if (tempSortColumn === 'salary') { va = Number(va); vb = Number(vb); }
      else { va = (va ?? '').toString().toLowerCase(); vb = (vb ?? '').toString().toLowerCase(); }
      if (va < vb) return tempSortOrder === 'asc' ? -1 : 1;
      if (va > vb) return tempSortOrder === 'asc' ? 1 : -1;
      return 0;
    });
  }

  if (sorted.length === 0) {
    tbody.innerHTML = '';
    emptyState.classList.remove('hidden');
    return;
  }
  emptyState.classList.add('hidden');

  const sanitizeInline = (val) => {
    try {
      return String(val ?? '')
        .replace(/<br\s*\/?>(\r?\n)?/gi, ' ')
        .replace(/[\r\n]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
    } catch { return String(val ?? ''); }
  };

  function deptClass(dept) {
    if (!dept) return '';
    const normalized = String(dept).toLowerCase().replace(/\s+/g, '-');
    switch (normalized) {
      case 'hr':
      case 'human-resources': return 'human-resources';
      case 'engineering': return 'engineering';
      case 'sales': return 'sales';
      case 'marketing': return 'marketing';
      case 'finance': return 'finance';
      case 'operations': return 'operations';
      default: return 'engineering';
    }
  }

  tbody.innerHTML = sorted.map((employee) => {
    const nameTxt = sanitizeInline(employee.name);
    const posTxt = sanitizeInline(employee.position);
    const deptTxt = sanitizeInline(employee.department);
    const statusInfo = getEmployeeStatus(employee);
    const backendStatus = (employee.status || '').toString().toLowerCase();
    let logical = backendStatus || statusInfo.status;
    if (logical === 'valid') logical = 'active';
    if (logical === 'expiring-soon' || logical === 'expiring_soon') logical = 'expiring';
    let colorClass = 'valid';
    if (logical === 'expiring') colorClass = 'expiring';
    if (!logical) { logical = 'pending'; colorClass = 'gray'; }
    const label = logical === 'expiring' ? 'Expiring Soon' : (logical === 'pending' ? 'Pending Review' : 'Active');
    const inlineColor = colorClass === 'expiring' ? '#EF4444' : (colorClass === 'valid' ? '#10B981' : '#9CA3AF');
  const statusDot = `<span class="status-dot ${colorClass}" aria-label="Status: ${label}" title="${label}" style="background:${inlineColor};display:inline-block;width:10px;height:10px;border-radius:50%;vertical-align:middle;"></span>`;
    const termRow = !!employee.terminated;
    return `
    <tr class="employee-row ${termRow ? 'terminated-row' : ''}" ${termRow ? 'style=\"background-color:#fff1f2;\"' : ''}>
      <td data-label="Name" class="col-name cell-name px-2 py-2 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer" onclick="viewEmployee('${employee.id}', 'temporary')">${nameTxt}</td>
      <td data-label="Email" class="col-email px-2 py-2">${employee.email}</td>
      <td data-label="Phone" class="col-phone px-2 py-2 text-center">${employee.phone || '-'}</td>
      <td data-label="Qatar ID" class="col-qid px-2 py-2 text-center">${employee.qid || '-'}</td>
      <td data-label="Nationality" class="col-nationality px-2 py-2">${employee.nationality || '-'}</td>
      <td data-label="Status" class="col-status px-2 py-2 text-center">${statusDot}</td>
      <td data-label="Position" class="col-position px-2 py-2">${posTxt}</td>
  <td data-label="Company" class="col-company px-2 py-2"><span class="department-badge ${deptClass(deptTxt)}" style="white-space:nowrap;display:inline-block;max-width:100%;overflow:hidden;text-overflow:ellipsis;vertical-align:middle;font-size:10px;padding:2px 6px;" title="${deptTxt}">${deptTxt}</span></td>
  <td data-label="Salary" class="col-salary px-2 py-2 text-right whitespace-nowrap tabular-nums" style="font-size:11px; line-height:1.15;">${(typeof CURRENCY_PREFIX!=='undefined'?CURRENCY_PREFIX:'QAR ')}${Number(employee.salary ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td data-label="Join Date" class="col-join px-2 py-2 whitespace-nowrap text-right">${formatDate(employee.joinDate)}</td>
      <td data-label="Actions" class="col-actions px-2 py-2 text-center">
        <div class="action-buttons">
          <button class="action-btn view-btn" onclick="viewEmployee('${employee.id}', 'temporary')"><i class="fas fa-eye"></i></button>
          <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}', 'temporary')"><i class="fas fa-edit"></i></button>
          <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}', 'temporary')"><i class="fas fa-trash"></i></button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function ensureTemporaryHeader() {
  const table = document.querySelector('#temporarySection table');
  if (!table) return;
  let thead = table.querySelector('thead');
  if (!thead) {
    thead = document.createElement('thead');
    table.prepend(thead);
  }
  const cols = [
    { key:'name', label:'Name', sortable:true, align:'text-left', pad:'px-3 py-2' },
    { key:'email', label:'Email', sortable:true, align:'text-left', pad:'px-3 py-2' },
    { key:'phone', label:'Phone', sortable:false, align:'text-center', pad:'px-2 py-2' },
    { key:'qid', label:'Qatar ID', sortable:false, align:'text-center', pad:'px-2 py-2' },
    { key:'nationality', label:'Nationality', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'status', label:'Status', sortable:true, align:'text-center', pad:'px-2 py-2' },
    { key:'position', label:'Position', sortable:true, align:'text-left', pad:'px-3 py-2' },
    { key:'department', label:'Company', sortable:true, align:'text-left', pad:'px-3 py-2' },
  { key:'salary', label:'Salary', sortable:true, align:'text-right', pad:'px-2 py-2' },
    { key:'joinDate', label:'Join Date', sortable:true, align:'text-right', pad:'px-3 py-2' },
    { key:'actions', label:'Actions', sortable:false, align:'text-center', pad:'px-2 py-2' }
  ];
  const arrowFor = (key) => {
    if (tempSortColumn !== key) return '<i class="fas fa-sort text-gray-400"></i>';
    return tempSortOrder === 'asc' ? '<i class="fas fa-sort-up text-indigo-600"></i>' : '<i class="fas fa-sort-down text-indigo-600"></i>';
  };
  thead.className = 'bg-gray-50 text-gray-600 text-xs uppercase tracking-wide';
  thead.innerHTML = `<tr>${cols.map(c => {
    if (!c.sortable) return `<th scope="col" class="${c.pad} font-semibold ${c.align} whitespace-nowrap">${c.label}</th>`;
    return `<th scope="col" class="cursor-pointer select-none ${c.pad} font-semibold ${c.align} whitespace-nowrap" onclick="import('./modules/temporary.js').then(m=>m.sortTemporary('${c.key}', { getTemporaryEmployees: window.__getTemporaryEmployees, getSearchQuery: window.__getTempSearch, getDepartmentFilter: window.__getTempDeptFilter, getShowTerminated: window.__getTempShowTerminated }))">${c.label} <span class="inline-block ml-1">${arrowFor(c.key)}</span></th>`;
  }).join('')}</tr>`;
}
