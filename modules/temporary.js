// Temporary employees module
import { formatDate, getExpiryIndicator } from './utils.js';

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
        const text = `${emp.name} ${emp.email} ${emp.qid || ''} ${emp.phone || ''} ${emp.position} ${emp.department}`.toLowerCase();
        matchesSearch = text.includes(query);
      }
    }
    const matchesTerm = showTerminated ? true : !emp.terminated;
    return matchesDept && matchesSearch && matchesTerm;
  });

  const sorted = [...filtered];
  if (tempSortColumn) {
    sorted.sort((a, b) => {
      let va = a[tempSortColumn];
      let vb = b[tempSortColumn];
      if (tempSortColumn === 'salary') {
        va = Number(va);
        vb = Number(vb);
      } else {
        va = (va ?? '').toString().toLowerCase();
        vb = (vb ?? '').toString().toLowerCase();
      }
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

  function deptClass(dept) {
    if (!dept) return '';
    const normalized = dept.toLowerCase().replace(/\s+/g, '-');
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
    return `
    <tr class="hover:bg-gray-50 ${employee.terminated ? 'terminated-row' : ''}">
      <td class="px-3 py-2 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer" onclick="viewEmployee('${employee.id}', 'temporary')">
        <span class="inline-flex items-center"><span class="${dotClass}" title="${title}" aria-label="${title}"></span>&nbsp;<span class="employee-name-text">${employee.name}</span></span>
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
          <button class="action-btn view-btn" onclick="viewEmployee('${employee.id}', 'temporary')"><i class="fas fa-eye"></i></button>
          <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}', 'temporary')"><i class="fas fa-edit"></i></button>
          <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}', 'temporary')"><i class="fas fa-trash"></i></button>
        </div>
      </td>
    </tr>
  `;}).join('');
}
