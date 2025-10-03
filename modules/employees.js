// Employees module: table rendering and sorting
import { formatDate, getExpiryIndicator } from './utils.js';

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
      let va = a[sortColumn];
      let vb = b[sortColumn];
      if (sortColumn === 'salary') {
        va = Number(va);
        vb = Number(vb);
      } else {
        va = (va ?? '').toString().toLowerCase();
        vb = (vb ?? '').toString().toLowerCase();
      }
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
    const indicator = getExpiryIndicator(employee);
    const dotClass = indicator.color === 'red' ? 'expiry-dot red' : 'expiry-dot green';
    const title = indicator.title;
    const termRow = !!employee.terminated;
    return `
    <tr class="hover:bg-gray-50 ${termRow ? 'terminated-row' : ''}" ${termRow ? 'style="background-color:#fff1f2;"' : ''}>
      <td class="px-3 py-2 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer" onclick="viewEmployee('${employee.id}', 'employees')">
        <span class="inline-flex items-center"><span class="${dotClass}" title="${title}" aria-label="${title}"></span>&nbsp;<span class="employee-name-text">${employee.name}</span></span>
        ${termRow ? '<span class="ml-2 inline-flex items-center gap-1 px-1.5 py-0.5 rounded text-[10px] font-semibold bg-rose-100 text-rose-800"><i class=\"fas fa-user-slash\"></i> Terminated</span>' : ''}
      </td>
      <td class="px-3 py-2">${employee.email}</td>
      <td class="px-3 py-2">${employee.phone || '-'}</td>
      <td class="px-3 py-2">${employee.qid || '-'}</td>
      <td class="px-3 py-2">${employee.position}</td>
      <td class="px-3 py-2">
        <span class="department-badge ${deptClass(employee.department)}">${employee.department}</span>
      </td>
      <td class="px-3 py-2 text-right tabular-nums">$${Number(employee.salary ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td class="px-3 py-2 whitespace-nowrap">${formatDate(employee.joinDate)}</td>
      <td class="px-3 py-2 text-center">
        <div class="action-buttons">
          <button class="action-btn view-btn" onclick="viewEmployee('${employee.id}', 'employees')"><i class="fas fa-eye"></i></button>
          <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}', 'employees')"><i class="fas fa-edit"></i></button>
          <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}', 'employees')"><i class="fas fa-trash"></i></button>
        </div>
      </td>
    </tr>`;
  }).join('');
}
