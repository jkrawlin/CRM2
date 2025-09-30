// Clients module: company management
// Keeps state, realtime subscription, rendering, and modal handlers

let _deps = null;
let _clients = [];
let _unsub = null;

export function initClients(deps) {
  _deps = deps; // { db, collection, query, onSnapshot, addDoc, serverTimestamp, showToast, cleanData }

  // Wire UI events
  const openClientModalBtn = document.getElementById('openClientModalBtn');
  if (openClientModalBtn) openClientModalBtn.addEventListener('click', () => openClientModal());
  const clientForm = document.getElementById('clientForm');
  if (clientForm) clientForm.addEventListener('submit', handleClientFormSubmit);

  // Expose for inline onclick in HTML
  window.closeClientModal = closeClientModal;
  window.openClientModal = openClientModal;

  // Initial render to show empty state even before first snapshot
  try { renderClientsTable(); } catch {}
}

export function subscribeClients() {
  if (!_deps) return;
  const { db, collection, query, onSnapshot, showToast } = _deps;
  if (_unsub) _unsub();
  const q = query(collection(db, 'customers'));
  _unsub = onSnapshot(
    q,
    (snapshot) => {
      _clients = snapshot.docs.map(d => ({ id: d.id, ...d.data() }));
      renderClientsTable();
      // Notify listeners that clients data updated (for Client Billing and others)
      try {
        const evt = new CustomEvent('clients:updated', { detail: _clients.slice() });
        document.dispatchEvent(evt);
      } catch {}
    },
    (err) => {
      console.error('Error loading clients:', err);
      showToast && showToast('Error loading clients', 'error');
    }
  );
}

export function stopClients() {
  if (_unsub) { try { _unsub(); } catch {} _unsub = null; }
}

export function getClients() {
  return _clients;
}

export function renderClientsTable() {
  const tbody = document.getElementById('clientsTableBody');
  const empty = document.getElementById('clientsEmptyState');
  if (!tbody || !empty) return;
  if (!_clients.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmtDateTime = (iso) => { try { return new Date(iso).toLocaleString(); } catch { return iso || ''; } };
  const rows = _clients
    .slice()
    .sort((a,b)=> (a.name||'').localeCompare(b.name||''))
    .map(c => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2 font-semibold text-gray-900">${c.name || ''}</td>
        <td class="px-4 py-2">${c.email || ''}</td>
        <td class="px-4 py-2">${c.phone || '-'}</td>
        <td class="px-4 py-2">${c.company || '-'}</td>
        <td class="px-4 py-2">${c.address || '-'}</td>
      </tr>
    `).join('');
  tbody.innerHTML = rows;
}

function openClientModal() {
  const modal = document.getElementById('clientModal');
  const form = document.getElementById('clientForm');
  if (form) form.reset();
  if (modal) modal.classList.add('show');
}

function closeClientModal() {
  const modal = document.getElementById('clientModal');
  if (modal) modal.classList.remove('show');
}

async function handleClientFormSubmit(e) {
  e.preventDefault();
  const { db, collection, addDoc, showToast, cleanData } = _deps || {};
  const name = document.getElementById('clientName')?.value.trim();
  const email = document.getElementById('clientEmail')?.value.trim();
  const phone = document.getElementById('clientPhone')?.value.trim();
  const company = document.getElementById('clientCompany')?.value.trim();
  const address = document.getElementById('clientAddress')?.value.trim();
  if (!name || !email || !address) {
    showToast && showToast('Company Name, Email, and Address are required', 'warning');
    return;
  }
  const payload = cleanData ? cleanData({
    name,
    email,
    phone,
    company,
    address,
    createdAt: new Date().toISOString(),
  }) : { name, email, phone, company, createdAt: new Date().toISOString() };
  try {
    const ref = await addDoc(collection(db, 'customers'), payload);
    showToast && showToast('Client added', 'success');
    // Optimistically update local state so the new client shows immediately
    try {
      const exists = _clients.some(c => c.id === ref.id);
      if (!exists) {
        _clients = _clients.concat([{ id: ref.id, ...payload }]);
        renderClientsTable();
      }
    } catch {}
    closeClientModal();
  } catch (err) {
    console.error('Add client failed', err);
    showToast && showToast('Failed to add client', 'error');
  }
}
