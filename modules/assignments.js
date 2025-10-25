// Assignments module: link employees to clients

let _deps = null;
let _assignments = [];
let _unsub = null;

export function initAssignments(deps) {
  _deps = deps; // { db, collection, query, onSnapshot, addDoc, updateDoc, doc, serverTimestamp, showToast, cleanData, getEmployees, getTemporaryEmployees, getClients }

  const open1 = document.getElementById('openAssignmentModalBtn');
  const open2 = document.getElementById('openAssignmentModalBtn2');
  if (open1) open1.addEventListener('click', () => openAssignmentModal());
  if (open2) open2.addEventListener('click', () => openAssignmentModal());
  const form = document.getElementById('assignmentForm');
  if (form) form.addEventListener('submit', handleAssignmentFormSubmit);
  // Wire QID input to lookup employee name
  const empInput = document.getElementById('asEmployee');
  if (empInput && !empInput.__wired) {
    empInput.setAttribute('inputmode','numeric');
    empInput.setAttribute('autocomplete','off');
    empInput.addEventListener('input', handleQidLookup);
    empInput.__wired = true;
  }

  // Expose for inline handlers
  window.closeAssignmentModal = closeAssignmentModal;
  window.openAssignmentModal = openAssignmentModal;

  // End button (delegation)
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('[data-end-assignment]');
    if (!btn) return;
    const id = btn.getAttribute('data-end-assignment');
    if (!id) return;
    try {
      const today = new Date();
      const ymd = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
      const { db, updateDoc, doc, serverTimestamp, cleanData, showToast } = _deps;
      await updateDoc(doc(db, 'assignments', id), cleanData({ endDate: ymd, updatedAt: serverTimestamp() }));
      showToast && showToast('Assignment ended', 'success');
    } catch (err) {
      console.warn('End assignment failed', err);
      _deps.showToast && _deps.showToast('Failed to end assignment', 'error');
    }
  });
}

export function subscribeAssignments() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot, showToast } = _deps;
  if (_unsub) _unsub();
  const q = query(collection(db, 'assignments'));
  _unsub = onSnapshot(
    q,
    (snapshot) => {
      _assignments = snapshot.docs.map(d => ({ id: d.id, ...d.data() }));
      renderAssignmentsTable();
      // Notify listeners that assignments updated (used by Client Billing)
      try {
        const evt = new CustomEvent('assignments:updated', { detail: _assignments.slice() });
        document.dispatchEvent(evt);
      } catch {}
    },
    (err) => {
      console.error('Error loading assignments:', err);
      showToast && showToast('Error loading assignments', 'error');
    }
  );
}

export function stopAssignments() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function getAssignments() { return _assignments; }

export function renderAssignmentsTable() {
  const tbody = document.getElementById('assignmentsTableBody');
  const empty = document.getElementById('assignmentsEmptyState');
  if (!tbody || !empty) return;
  if (!_assignments.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmt = (d) => d ? new Date(d).toLocaleDateString() : '-';
  tbody.innerHTML = _assignments
    .slice()
    .sort((a,b)=> (a.employeeName||'').localeCompare(b.employeeName||''))
    .map(a => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2 font-semibold text-gray-900">${a.employeeName || ''}</td>
        <td class="px-4 py-2">${a.clientName || ''}</td>
        <td class="px-4 py-2">${fmt(a.startDate)}</td>
        <td class="px-4 py-2">${fmt(a.endDate)}</td>
  <td class="px-4 py-2">${a.rate ? `QAR ${Number(a.rate).toLocaleString(undefined,{maximumFractionDigits:2})}/${a.rateType||'monthly'}` : '-'}</td>
        <td class="px-4 py-2 text-center">
          ${a.endDate ? '<span class="text-xs text-gray-500">Ended</span>' : `<button class="btn btn-secondary btn-sm" data-end-assignment="${a.id}"><i class="fas fa-stop"></i> End</button>`}
        </td>
      </tr>
    `).join('');
}

function openAssignmentModal() {
  const modal = document.getElementById('assignmentModal');
  if (!modal) return;
  const { getEmployees, getTemporaryEmployees, getClients } = _deps;
  const empInput = document.getElementById('asEmployee');
  const cliSel = document.getElementById('asClient');
  const clients = (getClients && getClients()) || [];
  // Reset QID input and any helper name label
  if (empInput) { empInput.value = ''; setEmployeeNameHelper(''); empInput.dataset.empId = ''; empInput.dataset.empWhich = ''; }
  if (cliSel) {
    cliSel.innerHTML = '<option value="" disabled selected>Select client</option>' +
      clients.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
        .map(c => `<option value="${c.id}">${c.name}${c.company?` • ${c.company}`:''}</option>`).join('');
  }

  const today = new Date();
  const ymd = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
  const asStart = document.getElementById('asStart');
  if (asStart) asStart.value = ymd;
  const asEnd = document.getElementById('asEnd');
  if (asEnd) asEnd.value = '';
  const asRate = document.getElementById('asRate');
  if (asRate) asRate.value = '';
  const asRateType = document.getElementById('asRateType');
  if (asRateType) asRateType.value = 'monthly';
  const asNotes = document.getElementById('asNotes');
  if (asNotes) asNotes.value = '';

  modal.classList.add('show');
}

function closeAssignmentModal() {
  const modal = document.getElementById('assignmentModal');
  if (modal) modal.classList.remove('show');
}

async function handleAssignmentFormSubmit(e) {
  e.preventDefault();
  const { db, collection, addDoc, serverTimestamp, showToast, cleanData, getEmployees, getTemporaryEmployees, getClients } = _deps || {};
  const empSel = document.getElementById('asEmployee');
  const cliSel = document.getElementById('asClient');
  const asStart = document.getElementById('asStart');
  const asEnd = document.getElementById('asEnd');
  const asRate = document.getElementById('asRate');
  const asRateType = document.getElementById('asRateType');
  const asNotes = document.getElementById('asNotes');

  const cliId = cliSel?.value || '';
  const empId = empSel?.dataset?.empId || '';
  const whichKey = empSel?.dataset?.empWhich || '';
  if (!empId || !whichKey || !cliId) {
    showToast && showToast('Please select employee and client', 'warning');
    return;
  }
  const perm = (getEmployees && getEmployees()) || [];
  const temps = (getTemporaryEmployees && getTemporaryEmployees()) || [];
  const list = whichKey === 'temporary' ? temps : perm;
  const emp = list.find(e => e.id === empId);
  const clients = (getClients && getClients()) || [];
  const cli = clients.find(c => c.id === cliId);

  if (!emp) { showToast && showToast('Employee not found', 'error'); return; }
  if (!cli) { showToast && showToast('Client not found', 'error'); return; }

  const payload = cleanData({
    employeeId: emp.id,
    employeeType: whichKey === 'temporary' ? 'Temporary' : 'Permanent',
    employeeName: emp.name,
    department: emp.department,
    position: emp.position,
    qid: emp.qid,
    clientId: cli.id,
    clientName: cli.name,
    clientEmail: cli.email,
    startDate: asStart?.value,
    endDate: asEnd?.value || null,
    rate: asRate?.value ? Number(asRate.value) : null,
    rateType: asRateType?.value || 'monthly',
    notes: asNotes?.value || null,
    createdAt: serverTimestamp(),
  });

  try {
    await addDoc(collection(db, 'assignments'), payload);
    showToast && showToast('Assignment saved', 'success');
    closeAssignmentModal();
  } catch (err) {
    console.error('Save assignment failed', err);
    showToast && showToast('Failed to save assignment', 'error');
  }
}

function handleQidLookup() {
  try {
    const input = document.getElementById('asEmployee');
    if (!input) return;
    const q = String(input.value||'').trim();
    if (!q) { setEmployeeNameHelper(''); input.dataset.empId=''; input.dataset.empWhich=''; return; }
    const perm = (_deps.getEmployees && _deps.getEmployees()) || [];
    const temps = (_deps.getTemporaryEmployees && _deps.getTemporaryEmployees()) || [];
    const matchIn = (arr) => arr.find(e => String(e.qid||'').replace(/\s+/g,'') === q.replace(/\s+/g,''));
    let emp = matchIn(perm);
    let which = 'employees';
    if (!emp) { emp = matchIn(temps); which = emp ? 'temporary' : ''; }
    if (emp && which) {
      input.dataset.empId = emp.id;
      input.dataset.empWhich = which;
      setEmployeeNameHelper(`${emp.name || ''}${emp.department?` • ${emp.department}`:''}`);
    } else {
      input.dataset.empId = '';
      input.dataset.empWhich = '';
      setEmployeeNameHelper('No match');
    }
  } catch {}
}

function setEmployeeNameHelper(text) {
  let el = document.getElementById('asEmployeeNameHelper');
  if (!el) {
    const input = document.getElementById('asEmployee');
    if (!input || !input.parentElement) return;
    el = document.createElement('div');
    el.id = 'asEmployeeNameHelper';
    el.className = 'text-xs text-gray-500 mt-1';
    input.parentElement.appendChild(el);
  }
  el.textContent = text || '';
}
