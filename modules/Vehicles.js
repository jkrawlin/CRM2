// Contractors module: table rendering and sorting
import { formatDate, getEmployeeStatus } from './utils.js';

let sortColumn = '';
let sortOrder = 'asc';

export function sortContractors(column, deps) {
  if (sortColumn === column) {
    sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    sortColumn = column;
    sortOrder = 'asc';
  }
  renderContractorTable(deps);
}

export function renderContractorTable({ getContractors, getSearchQuery, getDepartmentFilter, getShowTerminated }) {
  ensureContractorHeader();
  const tbody = document.getElementById('contractorTableBody');
  const emptyState = document.getElementById('contractorEmptyState');
  if (!tbody || !emptyState) return;

  const contractors = (typeof getContractors === 'function') ? getContractors() : [];
  const currentSearch = (typeof getSearchQuery === 'function' && getSearchQuery()) || '';
  const currentDepartmentFilter = (typeof getDepartmentFilter === 'function' && getDepartmentFilter()) || '';
  const showTerminated = (typeof getShowTerminated === 'function' && getShowTerminated()) || true;

  const filtered = contractors.filter(emp => {
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

  tbody.innerHTML = sorted.map((vehicle) => {
  const modelTxt = sanitizeInline(vehicle.model || '');
  const brandTxt = sanitizeInline(vehicle.brand || '');
  const ownerTxt = sanitizeInline(vehicle.owner || '');
  
  // Check expiry status
  const today = new Date();
  const thirtyDaysFromNow = new Date(today);
  thirtyDaysFromNow.setDate(today.getDate() + 30);
  
  let isExpiring = false;
  let expiryTooltip = [];
  
  if (vehicle.expiryDate) {
    const expiryDate = new Date(vehicle.expiryDate);
    if (!isNaN(expiryDate.getTime()) && expiryDate <= thirtyDaysFromNow) {
      isExpiring = true;
      const daysUntil = Math.ceil((expiryDate - today) / (1000 * 60 * 60 * 24));
      if (daysUntil < 0) {
        expiryTooltip.push(`Expired ${Math.abs(daysUntil)} day${Math.abs(daysUntil) === 1 ? '' : 's'} ago`);
      } else {
        expiryTooltip.push(`Expires in ${daysUntil} day${daysUntil === 1 ? '' : 's'}`);
      }
    }
  }
  
  // Status handling for vehicles
  const backendStatus = (vehicle.status || 'active').toString().toLowerCase();
  let colorClass = 'valid';
  let label = 'Active';
  
  // Override with expiry status if expiring/expired
  if (isExpiring) {
    colorClass = 'expiring';
    label = expiryTooltip.join('; ');
  } else if (backendStatus === 'inactive') {
    colorClass = 'gray';
    label = 'Inactive';
  } else if (backendStatus === 'maintenance') {
    colorClass = 'expiring';
    label = 'Maintenance';
  }
  
  const inlineColor = colorClass === 'expiring' ? '#EF4444' : (colorClass === 'valid' ? '#10B981' : '#9CA3AF');
  const statusDot = `<span class="status-dot ${colorClass}" aria-label="Status: ${label}" title="${label}" style="background:${inlineColor};display:inline-block;width:10px;height:10px;border-radius:50%;vertical-align:middle;"></span>`;
  
  // Format expiry date with red color if expiring
  const expiryStyle = isExpiring ? 'color: #EF4444; font-weight: 600;' : '';
  const expiryText = vehicle.expiryDate ? formatDate(vehicle.expiryDate) : '-';
  const expiryCell = isExpiring ? `<span style="${expiryStyle}" title="${expiryTooltip.join('; ')}">${expiryText}</span>` : expiryText;
  
    return `
    <tr class=\"employee-row\">
    <td data-label=\"Model\" class=\"col-name px-2 py-2 font-semibold text-indigo-600 text-left\">${modelTxt}</td>
    <td data-label=\"Brand\" class=\"col-email px-2 py-2 text-left\">${brandTxt}</td>
    <td data-label=\"Year\" class=\"col-phone px-2 py-2 text-center\">${vehicle.year || '-'}</td>
    <td data-label=\"Owner\" class=\"col-qid px-2 py-2 text-center\">${ownerTxt}</td>
  <td data-label=\"Status\" class=\"col-status px-2 py-2 text-center\">${statusDot}</td>
    <td data-label=\"Expiry\" class=\"col-position px-2 py-2 text-left\">${expiryCell}</td>
  <td data-label=\"Renew\" class=\"col-company px-2 py-2 text-left\">${formatDate(vehicle.renewDate)}</td>
  <td data-label=\"Reg Date\" class=\"col-salary px-2 py-2 text-left whitespace-nowrap\">${formatDate(vehicle.registrationDate)}</td>
    <td data-label=\"Actions\" class=\"col-actions px-2 py-2 text-center\">
        <div class=\"action-buttons\">
          <button class=\"action-btn view-btn\" onclick=\"viewVehicle('${vehicle.id}')\"><i class=\"fas fa-eye\"></i></button>
          <button class=\"action-btn edit-btn\" onclick=\"editVehicle('${vehicle.id}')\"><i class=\"fas fa-edit\"></i></button>
          <button class=\"action-btn delete-btn\" onclick=\"deleteVehicle('${vehicle.id}')\"><i class=\"fas fa-trash\"></i></button>
        </div>
      </td>
    </tr>`;
  }).join('');
}

function ensureContractorHeader() {
  // Find the contractors table thead
  const table = document.querySelector('#contractorsSection table');
  if (!table) return;
  let thead = table.querySelector('thead');
  if (!thead) {
    thead = document.createElement('thead');
    table.prepend(thead);
  }
  const cols = [
    { key:'model', label:'Model', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'brand', label:'Brand', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'year', label:'Year', sortable:true, align:'text-center', pad:'px-2 py-2' },
    { key:'owner', label:'Owner', sortable:true, align:'text-center', pad:'px-2 py-2' },
    { key:'status', label:'Status', sortable:true, align:'text-center', pad:'px-2 py-2' },
    { key:'expiryDate', label:'Expiry', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'renewDate', label:'Renew', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'registrationDate', label:'Reg Date', sortable:true, align:'text-left', pad:'px-2 py-2' },
    { key:'actions', label:'Actions', sortable:false, align:'text-center', pad:'px-2 py-2' }
  ];
  const arrowFor = (key) => {
    if (sortColumn !== key) return '<i class="fas fa-sort text-gray-400"></i>';
    return sortOrder === 'asc' ? '<i class="fas fa-sort-up text-indigo-600"></i>' : '<i class="fas fa-sort-down text-indigo-600"></i>';
  };
  thead.className = 'bg-gray-50 text-gray-600 text-xs uppercase tracking-wide';
  thead.innerHTML = `<tr>${cols.map(c => {
    const inlineStyle = 'style="font-weight: 700 !important; font-size: 11px !important; color: #334155 !important;"';
    if (!c.sortable) return `<th scope="col" class="${c.pad} font-semibold ${c.align} whitespace-nowrap" ${inlineStyle}>${c.label}</th>`;
    return `<th scope="col" class="cursor-pointer select-none ${c.pad} font-semibold ${c.align} whitespace-nowrap" ${inlineStyle} onclick="import('./modules/Vehicles.js?v=20251023-16').then(m=>m.sortContractors('${c.key}', { getContractors: window.__getContractors, getSearchQuery: window.__getContractorSearch, getDepartmentFilter: window.__getContractorDeptFilter, getShowTerminated: window.__getContractorShowTerminated }))">${c.label} <span class="inline-block ml-1">${arrowFor(c.key)}</span></th>`;
  }).join('')}</tr>`;
}
