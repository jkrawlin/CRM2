// Employees module: table rendering and sorting
import { formatDate, getEmployeeStatus } from './utils.js';

let sortColumn = '';
let sortOrder = 'asc';

export function sortEmployees(column, deps) {
  if (sortColumn === column) {
    sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    sortColumn = column;
    sortOrder = 'asc';
  }
  renderEmployeeTable(deps);
}

export function renderEmployeeTable({ getEmployees, getSearchQuery, getDepartmentFilter, getShowTerminated }) {
  ensureEmployeeHeader();
  const tbody = document.getElementById('employeeTableBody');
  const emptyState = document.getElementById('emptyState');
  if (!tbody || !emptyState) return;

  const employees = (typeof getEmployees === 'function') ? getEmployees() : [];
  const currentSearch = (typeof getSearchQuery === 'function' && getSearchQuery()) || '';
  const currentDepartmentFilter = (typeof getDepartmentFilter === 'function' && getDepartmentFilter()) || '';
  const showTerminated = (typeof getShowTerminated === 'function' && getShowTerminated()) || true;

  const filtered = employees.filter(emp => {
    const matchesDept = !currentDepartmentFilter || emp.department === currentDepartmentFilter;
    const query = (currentSearch || '').trim();
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
    const matchesTerm = showTerminated ? true : !emp.terminated;
    return matchesDept && matchesSearch && matchesTerm;
  });

  const sorted = [...filtered];
  if (sortColumn) {
    sorted.sort((a, b) => {
      if (sortColumn === 'status') {
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
        return sortOrder === 'asc' ? da - db : db - da;
      }
      let va = a[sortColumn];
      let vb = b[sortColumn];
      if (sortColumn === 'salary') { va = Number(va); vb = Number(vb); }
      else { va = (va ?? '').toString().toLowerCase(); vb = (vb ?? '').toString().toLowerCase(); }
      if (va < vb) return sortOrder === 'asc' ? -1 : 1;
      if (va > vb) return sortOrder === 'asc' ? 1 : -1;
      return 0;
    });
  }

  if (sorted.length === 0) {
    tbody.innerHTML = '';
    emptyState.classList.remove('hidden');
    return;
  }
  emptyState.classList.add('hidden');

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
  const statusDot = `<span class="status-dot ${colorClass}" aria-label="Status: ${label}" title="${label}" style="background:${inlineColor};display:inline-block;width:14px;height:14px;border-radius:50%;vertical-align:middle;"></span>`;
    const termRow = !!employee.terminated;
    return `
    <tr class=\"employee-row ${termRow ? 'terminated-row' : ''}\" ${termRow ? 'style=\\\"background-color:#fff1f2;\\\"' : ''}>
      <td data-label=\"Name\" class=\"col-name cell-name px-4 py-5 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer\" onclick=\"viewEmployee('${employee.id}', 'employees')\">${employee.name}</td>
      <td data-label=\"Email\" class=\"col-email px-4 py-5\">${employee.email}</td>
      <td data-label=\"Phone\" class=\"col-phone px-3 py-5 text-center\">${employee.phone || '-'} </td>
      <td data-label=\"Qatar ID\" class=\"col-qid px-3 py-5 text-center\">${employee.qid || '-'} </td>
  <td data-label=\"Status\" class=\"col-status px-2 py-5 text-center\">${statusDot}</td>
      <td data-label=\"Position\" class=\"col-position px-4 py-5\">${employee.position}</td>
      <td data-label=\"Company\" class=\"col-company px-4 py-5\"><span class=\"department-badge ${deptClass(employee.department)}\">${employee.department}</span></td>
      <td data-label=\"Salary\" class=\"col-salary px-3 py-5 text-center tabular-nums\">$${Number(employee.salary ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td data-label=\"Join Date\" class=\"col-join px-4 py-5 whitespace-nowrap text-right\">${formatDate(employee.joinDate)}</td>
      <td data-label=\"Actions\" class=\"col-actions px-2 py-5 text-center\">
        <div class=\"action-buttons\">
          <button class=\"action-btn view-btn\" onclick=\"viewEmployee('${employee.id}', 'employees')\"><i class=\"fas fa-eye\"></i></button>
          <button class=\"action-btn edit-btn\" onclick=\"editEmployee('${employee.id}', 'employees')\"><i class=\"fas fa-edit\"></i></button>
          <button class=\"action-btn delete-btn\" onclick=\"openDeleteModal('${employee.id}', 'employees')\"><i class=\"fas fa-trash\"></i></button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function ensureEmployeeHeader() {
  // Find the employees table thead (present but empty in index.html)
  const table = document.querySelector('#employeesSection table');
  if (!table) return;
  let thead = table.querySelector('thead');
  if (!thead) {
    thead = document.createElement('thead');
    table.prepend(thead);
  }
  const cols = [
    { key:'name', label:'Name', sortable:true, align:'text-left', pad:'px-4 py-3' },
    { key:'email', label:'Email', sortable:true, align:'text-left', pad:'px-4 py-3' },
    { key:'phone', label:'Phone', sortable:false, align:'text-center', pad:'px-3 py-3' },
    { key:'qid', label:'Qatar ID', sortable:false, align:'text-center', pad:'px-3 py-3' },
    { key:'status', label:'Status', sortable:true, align:'text-center', pad:'px-2 py-3' },
    { key:'position', label:'Position', sortable:true, align:'text-left', pad:'px-4 py-3' },
    { key:'department', label:'Company', sortable:true, align:'text-left', pad:'px-4 py-3' },
    { key:'salary', label:'Salary', sortable:true, align:'text-center', pad:'px-3 py-3' },
    { key:'joinDate', label:'Join Date', sortable:true, align:'text-right', pad:'px-4 py-3' },
    { key:'actions', label:'Actions', sortable:false, align:'text-center', pad:'px-2 py-3' }
  ];
  const arrowFor = (key) => {
    if (sortColumn !== key) return '<i class="fas fa-sort text-gray-400"></i>';
    return sortOrder === 'asc' ? '<i class="fas fa-sort-up text-indigo-600"></i>' : '<i class="fas fa-sort-down text-indigo-600"></i>';
  };
  thead.className = 'bg-gray-50 text-gray-600 text-xs uppercase tracking-wide';
  thead.innerHTML = `<tr>${cols.map(c => {
    if (!c.sortable) return `<th scope="col" class="${c.pad} font-semibold ${c.align}">${c.label}</th>`;
    return `<th scope="col" class="cursor-pointer select-none ${c.pad} font-semibold ${c.align}" onclick="import('./modules/employees.js').then(m=>m.sortEmployees('${c.key}', { getEmployees: window.__getEmployees, getSearchQuery: window.__getEmployeeSearch, getDepartmentFilter: window.__getEmployeeDeptFilter, getShowTerminated: window.__getEmployeeShowTerminated }))">${c.label} <span class="inline-block ml-1">${arrowFor(c.key)}</span></th>`;
  }).join('')}</tr>`;
}
