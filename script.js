import { db, auth, storage } from './firebase-config.js?v=20251004-01';
import {
  collection,
  getDocs,
  getDoc,
  addDoc,
  setDoc,
  doc,
  updateDoc,
  deleteDoc,
  onSnapshot,
  query,
  where,
  orderBy,
  serverTimestamp
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import {
  onAuthStateChanged,
  signOut,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import {
  ref as storageRef,
  uploadBytes,
  getDownloadURL,
  getBlob
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";

// Modules
import {
  initPayroll,
  renderPayrollTable as payrollRenderTable,
  renderPayrollFrame as payrollRenderFrame,
  setPayrollSubTab as payrollSetPayrollSubTab,
  sortPayroll as payrollSort,
  exportPayrollCsv as payrollExportPayrollCsv,
} from './modules/payroll.js?v=20251003-03';
// Utilities used in this file (masking account numbers in Payroll modal)
import { renderEmployeeTable as employeesRenderTable, sortEmployees } from './modules/employees.js?v=20251004-10';
import { renderTemporaryTable as temporaryRenderTable, sortTemporary } from './modules/temporary.js?v=20251004-10';
import { initClients, subscribeClients, renderClientsTable, getClients, forceRebuildClientsFilter } from './modules/clients.js?v=20251001-12';
import { initAssignments, subscribeAssignments, renderAssignmentsTable, getAssignments } from './modules/assignments.js?v=20251002-03';
import { initAccounts, subscribeAccounts, renderAccountsTable } from './modules/accounts.js?v=20250929-01';
import { initCashflow, subscribeCashflow, renderCashflowTable } from './modules/cashflow.js?v=20250930-03';
import { initLedger, subscribeLedger, renderLedgerTable, refreshLedgerAccounts } from './modules/ledger.js?v=20250929-01';

let employees = [];
let temporaryEmployees = [];
// Clients and Assignments state moved into modules; keep thin getters
// Sorting is handled by modules (employees/temporary)
let deleteEmployeeId = null;
let currentSearch = '';
let currentDepartmentFilter = '';
// Removed per UX decision: always show terminated with visual cue
let unsubscribeEmployees = null;
let unsubscribeTemporary = null;
// Subscriptions for clients/assignments are managed by their modules
let authed = false;
let authInitialized = false;
let payrollInited = false;
// Track current View modal context to lazy-load documents and manage blob URLs
let currentViewCtx = { id: null, which: 'employees', docsLoaded: false, revoke: [] };
// Track which payroll sub-tab is active: 'table' or 'report' (always default to table)
let currentPayrollSubTab = 'table';
// Track current payroll view data for payslip
let currentPayrollView = null;
// Track last month we ensured balances snapshot to avoid redundant writes
let lastBalanceEnsureMonth = null;
// Shadow accounts list for cashflow module filters
let __accountsShadow = [];
// Fund computation caches and unsubscribers (snapshot-based, independent of UI filters)
let __fundAccountsCache = [];
let __fundCashflowsCache = [];
let __unsubFundAccounts = null;
let __unsubFundCashflows = null;
// Bank-style fund: opening is stored in stats/fund; keep a live cache and unsub
let __fundOpening = 0;
let __unsubFundStats = null;

// =====================
// Notifications (expiring documents)
// =====================
let __notifications = []; // { id, type:'expiry', message, employeeId, which }
let __notifPanelOpen = false;

function __injectNotificationStylesOnce() {
  if (document.getElementById('notificationsStyles')) return;
  const style = document.createElement('style');
  style.id = 'notificationsStyles';
  style.textContent = `
    .notifications-wrapper { position: relative; }
    #notificationsPanel { position: absolute; top: 110%; right: 0; width: 320px; background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; box-shadow:0 8px 28px rgba(0,0,0,0.15); padding:10px 10px 12px; z-index:9999; flex-direction:column; gap:8px; }
    /* Hidden state handled by Tailwind 'hidden' class; show state when not hidden */
    #notificationsPanel.hidden { display:none !important; }
    #notificationsPanel:not(.hidden) { display:flex; }
    #notificationsPanel:focus { outline: 2px solid #6366F1; outline-offset:2px; }
    .notif-header { display:flex; align-items:center; justify-content:space-between; padding:2px 4px 4px; }
    .notif-title { font-size:13px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:#334155; margin:0; }
    .notif-clear { background:transparent; border:none; color:#64748B; cursor:pointer; padding:4px; border-radius:6px; }
    .notif-clear:hover { background:#F1F5F9; color:#334155; }
    .notif-list { max-height:360px; overflow:auto; display:flex; flex-direction:column; gap:6px; }
    .notif-item { display:flex; align-items:flex-start; gap:8px; background:#F8FAFC; border:1px solid #E2E8F0; padding:8px 10px; border-radius:10px; font-size:12.5px; line-height:1.3; position:relative; }
    .notif-item.expiry { border-color:#FECACA; background:#FEF2F2; }
    .notif-icon { width:26px; height:26px; border-radius:8px; background:#FEE2E2; color:#B91C1C; display:flex; align-items:center; justify-content:center; font-size:13px; flex-shrink:0; }
    .notif-item.expiry .notif-icon { background:#FEE2E2; color:#B91C1C; }
    .notif-msg { flex:1; color:#334155; }
    .notif-meta { font-size:11px; color:#64748B; margin-top:2px; }
    .notif-emp-link { color:#2563EB; font-weight:600; cursor:pointer; text-decoration:none; }
    .notif-emp-link:hover { text-decoration:underline; }
    .notif-empty { text-align:center; padding:12px 4px; font-size:12px; }
    #notificationsBadge { transition: transform .25s ease, opacity .25s ease; }
    .notif-dismiss { position:absolute; top:6px; right:6px; background:transparent; border:none; color:#64748B; cursor:pointer; padding:2px 4px; border-radius:6px; font-size:11px; }
    .notif-dismiss:hover { background:#E2E8F0; color:#334155; }
  `;
  document.head.appendChild(style);
}

function __setNotifications(list) {
  __notifications = list;
  __renderNotifications();
}

function __addOrUpdateExpiryNotification(emp, which, tooltip) {
  if (!emp || !emp.id) return;
  const id = `expiry-${which}-${emp.id}`;
  const days = /expires in ([-\d]+) day/.exec(tooltip || '') || /expired (\d+) day/.exec(tooltip || '');
  const existingIdx = __notifications.findIndex(n => n.id === id);
  const message = tooltip || 'Document expiring soon';
  if (existingIdx >= 0) {
    __notifications[existingIdx].message = message;
  } else {
    __notifications.push({ id, type:'expiry', message, employeeId: emp.id, which });
  }
}

function __pruneExpiryNotifications(validIds) {
  __notifications = __notifications.filter(n => !(n.type === 'expiry' && !validIds.has(n.id)));
}

function __renderNotifications() {
  const badge = document.getElementById('notificationsBadge');
  const panel = document.getElementById('notificationsPanel');
  const listEl = document.getElementById('notificationsList');
  if (!badge || !panel || !listEl) return;
  const count = __notifications.length;
  if (count > 0) {
    badge.classList.remove('hidden');
    badge.setAttribute('aria-label', `${count} notification${count===1?'':'s'}`);
  } else {
    badge.classList.add('hidden');
  }
  listEl.innerHTML = '';
  if (count === 0) {
    listEl.classList.add('empty');
    listEl.innerHTML = '<div class="notif-empty">No alerts</div>';
    return;
  }
  listEl.classList.remove('empty');
  __notifications.forEach(n => {
    const div = document.createElement('div');
    div.className = `notif-item ${n.type}`;
    const icon = n.type === 'expiry' ? '<i class="fas fa-exclamation-triangle"></i>' : '<i class="fas fa-info-circle"></i>';
    div.innerHTML = `
      <div class="notif-icon" aria-hidden="true">${icon}</div>
      <div class="notif-msg">
        ${n.message.replace(/(Qatar ID|Passport)/g, '<strong>$1</strong>')}<div class="notif-meta"><a data-notif-view href="#" class="notif-emp-link" data-which="${n.which}" data-eid="${n.employeeId}">View</a></div>
      </div>
      <button class="notif-dismiss" data-notif-dismiss="${n.id}" title="Dismiss" aria-label="Dismiss">✕</button>`;
    listEl.appendChild(div);
  });
}

function __toggleNotificationsPanel(force) {
  const btn = document.getElementById('notificationsBtn');
  const panel = document.getElementById('notificationsPanel');
  if (!btn || !panel) return;
  const wantOpen = force !== undefined ? force : !__notifPanelOpen;
  __notifPanelOpen = wantOpen;
  if (wantOpen) {
    panel.classList.remove('hidden');
    btn.setAttribute('aria-expanded','true');
    setTimeout(()=> panel.focus(), 0);
  } else {
    panel.classList.add('hidden');
    btn.setAttribute('aria-expanded','false');
  }
}

document.addEventListener('click', (e) => {
  const btn = document.getElementById('notificationsBtn');
  const panel = document.getElementById('notificationsPanel');
  if (!btn || !panel) return;
  if (e.target === btn || btn.contains(e.target)) {
    e.preventDefault();
    __toggleNotificationsPanel();
    return;
  }
  if (panel.contains(e.target)) {
    const dismissId = e.target.getAttribute('data-notif-dismiss');
    if (dismissId) {
      __notifications = __notifications.filter(n => n.id !== dismissId);
      __renderNotifications();
    }
    const link = e.target.closest('[data-notif-view]');
    if (link) {
      const eid = link.getAttribute('data-eid');
      const which = link.getAttribute('data-which');
      if (eid) {
        window.viewEmployee && window.viewEmployee(eid, which);
        __toggleNotificationsPanel(false);
      }
    }
    return;
  }
  // click outside
  if (__notifPanelOpen) __toggleNotificationsPanel(false);
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && __notifPanelOpen) {
    __toggleNotificationsPanel(false);
  }
});

function __rebuildExpiryNotifications() {
  const utils = window.__utils || {};
  // Fallback: import function via dynamic if not already collected; but we rely on modules already imported
  // We'll reconstruct statuses using the same logic in modules: getEmployeeStatus
  // We can call getEmployeeStatus via a cached reference from modules by creating a lightweight mirror using a global injection if needed; simpler: duplicate minimal logic here
  // Instead, leverage employeesRenderTable side-effect? Too indirect. We'll compute statuses directly below replicating minimal logic.
  const today = new Date();
  const thirtyDaysFromNow = new Date(today);
  thirtyDaysFromNow.setDate(today.getDate() + 30);
  const all = [...employees, ...temporaryEmployees.map(e => ({ ...e, __which:'temporary'}))];
  const validIds = new Set();
  all.forEach(emp => {
    const which = emp.__which ? 'temporary' : 'employees';
    let tooltipParts = [];
    let status = 'valid';
    const check = (val, label) => {
      if (!val) return; const d = new Date(val); if (isNaN(d.getTime())) return; const daysUntil = Math.ceil((d - today)/(1000*60*60*24)); if (d <= thirtyDaysFromNow) { status='expiring'; const msg = daysUntil < 0 ? `${label} expired ${Math.abs(daysUntil)} day${Math.abs(daysUntil)===1?'':'s'} ago` : `${label} expires in ${daysUntil} day${daysUntil===1?'':'s'}`; tooltipParts.push(msg);} };
    check(emp.qidExpiry || emp.qid_expiry || emp.QIDExpiry || emp.qidExpire || emp.qidExpireDate, 'Qatar ID');
    check(emp.passportExpiry || emp.passport_expiry || emp.PassportExpiry || emp.passportExpire || emp.passportExpireDate, 'Passport');
    if (status === 'expiring') {
      const id = `expiry-${which}-${emp.id}`;
      validIds.add(id);
      __addOrUpdateExpiryNotification(emp, which, tooltipParts.join('; '));
    }
  });
  __pruneExpiryNotifications(validIds);
  __renderNotifications();
}

__injectNotificationStylesOnce();

// =====================
// Generic Grid Engine (Excel-like) for Accounts tables
// =====================
let __gridCache = (window.__gridCache ||= {}); // { [tableId]: { [rowKey]: {field:value} } }
let __gridCtx = null; // { tbody, tableId, opts }
let __gridSel = null; // { sr, sc, er, ec }
let __gridEditing = null; // { r,c,cell,field,original }
let __gridActive = false;

function __grid_injectStyles() {
  if (document.getElementById('grid-styles-global')) return;
  const s = document.createElement('style');
  s.id = 'grid-styles-global';
  s.textContent = `
    /* Global grid styles (Accounts) */
    td.grid-cell, th.grid-cell { border: 1px solid #e5e7eb; }
    table:has(td.grid-cell) { border-collapse: separate; border-spacing: 0; }
    td.grid-cell { position: relative; cursor: cell; }
    td.grid-cell.readonly { background: #fafafa; color: #475569; }
    td.grid-cell.grid-active { outline: 2px solid #4f46e5; outline-offset: -2px; background: #eef2ff; }
    td.grid-cell.grid-selected { background: #eef2ff; }
    td.grid-cell.editing { outline: 2px solid #22c55e; background: #ecfdf5; }
    td.grid-cell[contenteditable="true"] { caret-color: #111827; }
  `;
  document.head.appendChild(s);
}
function __grid_fmtCurrency(n) { return `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`; }
function __grid_parseNumber(s) { if (typeof s!=='string') return Number(s||0); const t=s.replace(/[^0-9.\-]/g,''); const v=Number(t); return isFinite(v)?v:0; }
function __grid_isEditableCol(c) { const cols = __gridCtx?.opts?.editableCols || []; return cols.includes(c); }
function __grid_isNumericCol(c) { const cols = __gridCtx?.opts?.numericCols || []; return cols.includes(c); }
function __grid_getCell(r,c) { return __gridCtx?.tbody?.querySelector?.(`td.grid-cell[data-row="${r}"][data-col="${c}"]`)||null; }
function __grid_clearSel() { if (!__gridCtx?.tbody) return; __gridCtx.tbody.querySelectorAll('td.grid-cell.grid-active, td.grid-cell.grid-selected').forEach(el=>el.classList.remove('grid-active','grid-selected')); }
function __grid_applySel() {
  if (!__gridCtx?.tbody || !__gridSel) return;
  const { sr, sc, er, ec } = __gridSel; const r0=Math.min(sr,er), r1=Math.max(sr,er), c0=Math.min(sc,ec), c1=Math.max(sc,ec);
  for (let r=r0;r<=r1;r++){ for(let c=c0;c<=c1;c++){ const cell=__grid_getCell(r,c); if (cell) cell.classList.add('grid-selected'); }}
  const active=__grid_getCell(sr,sc); if (active) active.classList.add('grid-active');
}
function __grid_setActive(r,c,extend=false) {
  if (!__gridCtx) return;
  const maxR = (__gridCtx.tbody?.querySelectorAll('tr')?.length||1)-1;
  const maxC = (__gridCtx.opts?.maxCols ?? 0);
  r = Math.max(0, Math.min(maxR, r)); c = Math.max(0, Math.min(maxC, c));
  if (!extend || !__gridSel) __gridSel = { sr:r, sc:c, er:r, ec:c }; else { __gridSel.er=r; __gridSel.ec=c; }
  __grid_clearSel(); __grid_applySel();
}
function __grid_stopEdit(save) {
  if (!__gridEditing) return;
  const { r,c,cell,field,original } = __gridEditing;
  const text = cell.textContent || '';
  let display = text, toStore = text;
  if (__grid_isNumericCol(c)) { const num = __grid_parseNumber(text); display = __grid_fmtCurrency(num); toStore = String(num); }
  if (save) {
    cell.setAttribute('data-raw', toStore);
    const key = __gridCtx.opts.getRowKey(r);
    const bucket = (__gridCache[__gridCtx.tableId] ||= {});
    const row = (bucket[key] ||= {});
    if (field) row[field] = __grid_isNumericCol(c) ? __grid_parseNumber(toStore) : text;
    if (typeof __gridCtx.opts.recompute === 'function') { try { __gridCtx.opts.recompute(r, { rowKey:key, row }); } catch {} }
  } else {
    const d = __grid_isNumericCol(c) ? __grid_fmtCurrency(__grid_parseNumber(original)) : original; cell.textContent = d;
  }
  cell.removeAttribute('contenteditable'); cell.classList.remove('editing'); __gridEditing = null;
}
function __grid_startEdit(r,c) {
  if (!__grid_isEditableCol(c)) return; const cell = __grid_getCell(r,c); if (!cell) return;
  if (__gridEditing) __grid_stopEdit(false);
  const field = cell.getAttribute('data-field'); const raw = cell.getAttribute('data-raw') || cell.textContent;
  __gridEditing = { r,c,cell,field,original: raw };
  cell.classList.add('editing'); cell.setAttribute('contenteditable','true');
  if (__grid_isNumericCol(c)) { const v = __grid_parseNumber(raw); cell.textContent = String(v||0); }
  const range=document.createRange(); range.selectNodeContents(cell); const sel=window.getSelection(); sel.removeAllRanges(); sel.addRange(range);
}
function makeTableGrid(tbody, opts) {
  __grid_injectStyles(); if (!tbody) return;
  const rows = Array.from(tbody.querySelectorAll('tr'));
  if (!rows.length) return;
  const maxCols = rows.reduce((m,tr)=>Math.max(m, tr.children.length-1), 0);
  opts.maxCols = maxCols;
  // Annotate cells
  rows.forEach((tr, r) => {
    Array.from(tr.children).forEach((td, c) => {
      td.classList.add('grid-cell'); td.setAttribute('data-row', String(r)); td.setAttribute('data-col', String(c));
      const field = opts.fields?.[c] || ''; if (field) td.setAttribute('data-field', field);
      if ((opts.numericCols||[]).includes(c)) {
        const raw = td.getAttribute('data-raw') || __grid_parseNumber(td.textContent||'');
        td.setAttribute('data-raw', String(raw));
      }
      if (!(opts.editableCols||[]).includes(c)) td.classList.add('readonly');
    });
  });
  // Context
  __gridCtx = { tbody, tableId: opts.tableId, opts };
  __gridActive = true; __grid_setActive(0,0);
  // Wire events once per tbody
  if (!tbody.__gridWired) {
    tbody.addEventListener('mousedown', (e) => { const td = e.target.closest('td.grid-cell'); if (!td) return; __gridActive = true; const r=Number(td.getAttribute('data-row')||0), c=Number(td.getAttribute('data-col')||0); __grid_setActive(r,c, e.shiftKey); });
    tbody.addEventListener('dblclick', (e) => { const td = e.target.closest('td.grid-cell'); if (!td) return; const r=Number(td.getAttribute('data-row')||0), c=Number(td.getAttribute('data-col')||0); __grid_startEdit(r,c); });
    tbody.__gridWired = true;
  }
  if (!document.__gridKeyWired) {
    document.addEventListener('keydown', (e) => {
      if (!__gridActive) return; if (__gridEditing) { if (e.key==='Enter'){ e.preventDefault(); __grid_stopEdit(true); return;} if(e.key==='Escape'){ e.preventDefault(); __grid_stopEdit(false); return;} return; }
      if (!__gridSel) return; const { sr, sc } = __gridSel;
      if (e.key==='F2'){ e.preventDefault(); __grid_startEdit(sr,sc); return; }
      if (e.key==='Enter'){ e.preventDefault(); __grid_setActive(sr+1, sc); return; }
      if (e.key==='Tab'){ e.preventDefault(); __grid_setActive(sr, sc + (e.shiftKey?-1:1)); return; }
      if (e.key==='ArrowLeft'){ e.preventDefault(); __grid_setActive(sr, sc-1); return; }
      if (e.key==='ArrowRight'){ e.preventDefault(); __grid_setActive(sr, sc+1); return; }
      if (e.key==='ArrowUp'){ e.preventDefault(); __grid_setActive(sr-1, sc); return; }
      if (e.key==='ArrowDown'){ e.preventDefault(); __grid_setActive(sr+1, sc); return; }
      if (typeof e.key === 'string' && e.key.length===1 && !e.ctrlKey && !e.metaKey && !e.altKey) {
        if (__grid_isEditableCol(sc)) {
          __grid_startEdit(sr,sc);
          setTimeout(()=>{ try { if (__gridEditing?.cell) __gridEditing.cell.textContent = e.key; } catch {} }, 0);
          e.preventDefault();
        }
      }
    });
    document.addEventListener('copy', (e) => { if (!__gridActive || !__gridSel) return; const {sr,sc,er,ec}=__gridSel; const r0=Math.min(sr,er), r1=Math.max(sr,er), c0=Math.min(sc,ec), c1=Math.max(sc,ec); const out=[]; for(let r=r0;r<=r1;r++){const row=[]; for(let c=c0;c<=c1;c++){const cell=__grid_getCell(r,c); row.push(cell ? (cell.getAttribute('data-raw')||cell.textContent||'') : '');} out.push(row.join('\t'));} try{ e.clipboardData.setData('text/plain', out.join('\n')); e.preventDefault(); }catch{} });
    document.addEventListener('paste', (e) => { if (!__gridActive || !__gridSel) return; const text=e.clipboardData?.getData('text/plain'); if (!text) return; const {sr,sc}=__gridSel; const rows=text.split(/\r?\n/); for (let i=0;i<rows.length;i++){ if (rows[i]==='') continue; const cols=rows[i].split('\t'); for (let j=0;j<cols.length;j++){ const r=sr+i, c=sc+j; if (!__grid_isEditableCol(c)) continue; const cell=__grid_getCell(r,c); if (!cell) continue; const field=cell.getAttribute('data-field'); const key=__gridCtx.opts.getRowKey(r); const bucket=(__gridCache[__gridCtx.tableId] ||= {}); const row=(bucket[key] ||= {}); const val=cols[j]; if (__grid_isNumericCol(c)) { const num=__grid_parseNumber(val); row[field]=num; cell.setAttribute('data-raw', String(num)); cell.textContent=__grid_fmtCurrency(num); } else { row[field]=val; cell.setAttribute('data-raw', val); cell.textContent=val; } if (typeof __gridCtx.opts.recompute==='function') { try { __gridCtx.opts.recompute(r,{rowKey:key,row}); } catch {} } } } e.preventDefault(); });
    document.addEventListener('mousedown', (e) => { const accSec=document.getElementById('accountsSection'); if (accSec && !accSec.contains(e.target)) { if (__gridEditing) __grid_stopEdit(true); __gridActive=false; } });
    document.__gridKeyWired = true;
  }
}

// Initialize the app
document.addEventListener('DOMContentLoaded', () => {
  // IMPORTANT: Hide app and show login immediately on load
  const loginPage = document.getElementById('loginPage');
  const appRoot = document.getElementById('appRoot');
  if (loginPage) loginPage.style.display = '';
  if (appRoot) appRoot.style.display = 'none';

  // Theme toggle removed
  setupEventListeners();
  setDefaultJoinDate();
  // Wire Accounts sub-tab buttons explicitly (in addition to delegated handler)
  try { wireAccountsTabButtons(); } catch {}
  // Always-available fallback to open cash txn modal even if module wiring hasn't attached yet
  try {
    window.__openCashTxnFallback = function(kind) {
      const modal = document.getElementById('cashTxnModal');
      if (!modal) return false;
      const form = document.getElementById('cashTxnForm');
      if (form) try { form.reset(); } catch {}
      const d = document.getElementById('cfDate');
      if (d) { try { d.valueAsDate = new Date(); } catch {} }
      const typeSel = document.getElementById('cfType');
      if (typeSel) {
        const v = (kind === 'out') ? 'out' : 'in';
        typeSel.value = v;
        typeSel.disabled = true;
      }
      // Focus amount quickly
      setTimeout(() => { try { document.getElementById('cfAmount')?.focus(); } catch {} }, 0);
      modal.classList.add('show');
      return true;
    }
  } catch {}
  // Expose setter for modules to update accounts shadow
  window.__setAccountsShadow = (arr) => { __accountsShadow = Array.isArray(arr) ? arr.slice() : []; };

  // Stronger modal fix: ensure #editFundModal is appended to body to avoid stacking/scope issues
  try {
    const efm = document.getElementById('editFundModal');
    if (efm && efm.parentElement !== document.body) {
      document.body.appendChild(efm);
      try { console.debug('[Fund] editFundModal moved to body'); } catch {}
    }
  } catch {}
  // When accounts update, refresh cashflow/ledger dropdowns if visible
  window.addEventListener('accounts:updated', () => {
    try { renderCashflowTable?.(); } catch {}
    try { refreshLedgerAccounts?.(); } catch {}
    try { updateAccountsFundCard(); } catch {}
    // If Overview tab is active, refresh it to reflect account name changes
    try {
      const ovBtn = document.getElementById('accountsSubTabOverviewBtn');
      const ovTab = document.getElementById('accountsTabOverview');
      if ((ovBtn && ovBtn.classList.contains('border-b-2')) || (ovTab && ovTab.style.display !== 'none')) {
        renderAccountsOverview();
      }
    } catch {}
  });
  // Keep a cashflow shadow array to compute fund total
  window.addEventListener('cashflow:updated', (e) => {
    try { __cashflowShadow = Array.isArray(e.detail) ? e.detail.slice() : []; } catch { __cashflowShadow = []; }
    try { updateAccountsFundCard(); } catch {}
    // Update Overview metrics and Transactions filters/live table if those tabs are visible
    try {
      const ovTab = document.getElementById('accountsTabOverview');
      if (ovTab && ovTab.style.display !== 'none') renderAccountsOverview();
    } catch {}
  });

  // Auth state listener
  onAuthStateChanged(auth, (user) => {
    authInitialized = true;
    authed = !!user;
    updateAuthUI(user);

    // Explicitly toggle views based on authentication
    if (user) {
      // User is signed in
      if (loginPage) loginPage.style.display = 'none';
      if (appRoot) appRoot.style.display = '';
  // Default to dashboard section on sign-in
  setActiveSection('dashboard');
  loadEmployeesRealtime();
  loadTemporaryRealtime();
  // Init clients/assignments modules and subscribe
  try {
  initClients({ db, collection, query, onSnapshot, addDoc, updateDoc, deleteDoc, doc, serverTimestamp, showToast, cleanData,
    getAssignments: () => getAssignments(),
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
  });
    subscribeClients();
    // Independent controller for Clients filter to avoid timing issues
    try {
      if (!window.__clientsFilterControllerWired) {
        window.__clientsFilterId = '';
        // When first clients update arrives, populate and enable the dropdown
        document.addEventListener('clients:updated', (e) => {
          const sel = document.getElementById('clientsFilterSelect');
          if (!sel) return;
          const list = Array.isArray(e.detail) ? e.detail : (typeof getClients==='function'?getClients():[]);
          const prev = window.__clientsFilterId || sel.value || '';
          if (!list.length) {
            sel.innerHTML = '<option value="" disabled selected>No clients available</option>';
            sel.disabled = true;
          } else {
            sel.disabled = false;
            const opts = ['<option value="">All clients…</option>']
              .concat(list.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
                .map(c => `<option value="${c.id}">${escapeHtml(c.name||c.company||'')}</option>`));
            sel.innerHTML = opts.join('');
            if (prev) { try { sel.value = prev; } catch {} }
          }
        }, { once: true });
        const sel = document.getElementById('clientsFilterSelect');
        if (sel && !sel.__wired2) {
          sel.addEventListener('change', () => {
            window.__clientsFilterId = sel.value || '';
            try { renderClientsTable(); } catch {}
          });
          sel.__wired2 = true;
        }
        // Also keep options updated on subsequent clients updates (preserving selection)
        document.addEventListener('clients:updated', (e) => {
          const sel2 = document.getElementById('clientsFilterSelect');
          if (!sel2) return;
          const list = Array.isArray(e.detail) ? e.detail : (typeof getClients==='function'?getClients():[]);
          const prev = window.__clientsFilterId || sel2.value || '';
          if (!list.length) {
            sel2.innerHTML = '<option value="" disabled selected>No clients available</option>';
            sel2.disabled = true;
          } else {
            sel2.disabled = false;
            const opts = ['<option value="">All clients…</option>']
              .concat(list.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
                .map(c => `<option value="${c.id}">${escapeHtml(c.name||c.company||'')}</option>`));
            sel2.innerHTML = opts.join('');
            if (prev) { try { sel2.value = prev; } catch {} }
          }
        });
        window.__clientsFilterControllerWired = true;
      }
    } catch {}
  } catch (e) { console.warn('Clients init failed', e); }
  try {
    initAssignments({ db, collection, query, onSnapshot, addDoc, updateDoc, doc, serverTimestamp, showToast, cleanData,
      getEmployees: () => employees,
      getTemporaryEmployees: () => temporaryEmployees,
      getClients: () => getClients(),
    });
    subscribeAssignments();
  } catch (e) { console.warn('Assignments init failed', e); }
  try {
    initAccounts({ db, collection, query, onSnapshot, addDoc, showToast, cleanData });
    subscribeAccounts();
  } catch (e) { console.warn('Accounts init failed', e); }
  try {
    // Provide accounts to cashflow for dropdowns
  initCashflow({ db, collection, query, onSnapshot, addDoc, orderBy, where, showToast, cleanData, getAccounts: () => __getLocalAccounts(), ensureAssetAccount });
    subscribeCashflow();
  } catch (e) { console.warn('Cashflow init failed', e); }
  // Start dedicated fund watchers (accounts + cashflows snapshots) to keep fund card in sync
  try { subscribeFundCardSnapshots(); } catch (e) { console.warn('Fund snapshot subscribe failed', e); }
  try {
    // Provide accounts to ledger as well
    initLedger({ db, collection, query, onSnapshot, orderBy, where, showToast, cleanData, getAccounts: () => __getLocalAccounts() });
    subscribeLedger();
  } catch (e) { console.warn('Ledger init failed', e); }
  // Initialize payroll module and initial render
  if (!payrollInited) {
    try {
      initPayroll({
        getEmployees: () => employees,
        getTemporaryEmployees: () => temporaryEmployees,
        getSearchQuery: () => currentSearch,
      });
      payrollInited = true;
    } catch (e) { console.warn('Payroll init failed', e); }
  }
  renderPayrollTable();
  // Ensure monthly balances snapshot exists for the current month
  try { ensureCurrentMonthBalances(); } catch {}
  // Reconcile all balances shortly after sign-in to ensure carryovers are persisted
  try { setTimeout(() => { try { reconcileAllBalances(); } catch {} }, 5000); } catch {}
    } else {
      // User is signed out
      if (loginPage) loginPage.style.display = '';
      if (appRoot) appRoot.style.display = 'none';

      // Clean up
      if (unsubscribeEmployees) {
        unsubscribeEmployees();
        unsubscribeEmployees = null;
      }
      if (unsubscribeTemporary) {
        unsubscribeTemporary();
        unsubscribeTemporary = null;
      }
      // Cleanup fund snapshot listeners
      try { if (__unsubFundAccounts) { __unsubFundAccounts(); __unsubFundAccounts = null; } } catch {}
      try { if (__unsubFundCashflows) { __unsubFundCashflows(); __unsubFundCashflows = null; } } catch {}
      try { if (__unsubFundStats) { __unsubFundStats(); __unsubFundStats = null; } } catch {}
    employees = [];
    temporaryEmployees = [];
      renderEmployeeTable();
      renderTemporaryTable();
    try { renderClientsTable(); } catch {}
    try { renderAssignmentsTable(); } catch {}
  renderPayrollTable();
      updateStats();
      updateDepartmentFilter();
      // Reset section visibility for next sign-in
      setActiveSection('dashboard');
      try { ensureCurrentMonthBalances(); } catch {}
    }
  });
});

// Real-time listener for employees
function loadEmployeesRealtime() {
  const q = query(collection(db, "employees"), orderBy("name"));
  if (unsubscribeEmployees) unsubscribeEmployees();
  unsubscribeEmployees = onSnapshot(
    q,
    (snapshot) => {
  employees = snapshot.docs.map((d) => ({ id: d.id, ...d.data() }));
  renderEmployeeTable();
  __rebuildExpiryNotifications();
      updateStats();
      updateDepartmentFilter();
      // Keep payroll in sync as data arrives
      renderPayrollTable();
      try { ensureCurrentMonthBalances(); } catch {}
    },
    (error) => {
      console.error("Error loading employees: ", error);
      showToast('Error loading employees', 'error');
    }
  );
}


// Real-time listener for temporary employees
function loadTemporaryRealtime() {
  const q = query(collection(db, "temporaryEmployees"), orderBy("name"));
  if (unsubscribeTemporary) unsubscribeTemporary();
  unsubscribeTemporary = onSnapshot(
    q,
    (snapshot) => {
  temporaryEmployees = snapshot.docs.map((d) => ({ id: d.id, ...d.data() }));
  renderTemporaryTable();
  __rebuildExpiryNotifications();
      // Keep payroll in sync as data arrives
      renderPayrollTable();
      try { ensureCurrentMonthBalances(); } catch {}
    },
    (error) => {
      console.error("Error loading temporary employees: ", error);
      showToast('Error loading temporary employees', 'error');
    }
  );
}


// Remove keys with undefined values (Firestore does not allow undefined)
function cleanData(obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    // Firestore rules often expect either a valid value or the key omitted entirely.
    // Drop undefined and null so optional fields don't violate validators.
    if (v !== undefined && v !== null) out[k] = v;
  }
  return out;
}

// Normalize various joinDate shapes into strict YYYY-MM-DD to satisfy rules
function toYMD(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeJoinDate(jd) {
  try {
    if (!jd) return null;
    if (typeof jd === 'string') {
      if (/^\d{4}-\d{2}-\d{2}$/.test(jd)) return jd; // already correct
      const d = new Date(jd);
      if (!isNaN(d.getTime())) return toYMD(d);
      return null;
    }
    if (jd instanceof Date) {
      if (!isNaN(jd.getTime())) return toYMD(jd);
      return null;
    }
    if (typeof jd === 'object') {
      if (typeof jd.seconds === 'number') {
        const d = new Date(jd.seconds * 1000);
        if (!isNaN(d.getTime())) return toYMD(d);
      }
      if (typeof jd.toDate === 'function') {
        try {
          const d = jd.toDate();
          if (d instanceof Date && !isNaN(d.getTime())) return toYMD(d);
        } catch {}
      }
    }
  } catch {}
  return null;
}

// Add or update employee in Firestore
async function saveEmployee(employee, isNew = false) {
  try {
    if (employee.id) {
      const employeeRef = doc(db, "employees", employee.id);
      const { id, ...updateData } = employee;
      await updateDoc(employeeRef, cleanData(updateData));
      showToast(isNew ? 'Employee added successfully' : 'Employee updated successfully', 'success');
    } else {
      const { id, ...newEmployee } = employee;
  await addDoc(collection(db, "employees"), cleanData(newEmployee));
  showToast('Employee added successfully', 'success');
    }
    clearForm();
    // Close the modal after a successful save
    if (document.getElementById('employeeModal')?.classList.contains('show')) {
      closeEmployeeModal();
    }
  } catch (error) {
    console.error("Error saving employee: ", error);
    showToast('Error saving employee', 'error');
  }
}

// Add or update temporary employee in Firestore
async function saveTemporaryEmployee(employee, isNew = false) {
  try {
    if (employee.id) {
      const employeeRef = doc(db, "temporaryEmployees", employee.id);
      const { id, ...updateData } = employee;
      await updateDoc(employeeRef, cleanData(updateData));
      showToast(isNew ? 'Temporary employee added successfully' : 'Temporary employee updated successfully', 'success');
    } else {
      const { id, ...newEmployee } = employee;
  await addDoc(collection(db, "temporaryEmployees"), cleanData(newEmployee));
  showToast('Temporary employee added successfully', 'success');
    }
    clearForm();
    if (document.getElementById('employeeModal')?.classList.contains('show')) {
      closeEmployeeModal();
    }
  } catch (error) {
    console.error("Error saving temporary employee: ", error);
    showToast('Error saving temporary employee', 'error');
  }
}

// Delete employee from Firestore
async function deleteEmployeeFromDB(employeeId) {
  try {
    await deleteDoc(doc(db, "employees", employeeId));
    showToast('Employee deleted successfully', 'success');
  } catch (error) {
    console.error("Error deleting employee: ", error);
    showToast('Error deleting employee', 'error');
  }
}

// Delete temporary employee from Firestore
async function deleteTemporaryFromDB(employeeId) {
  try {
    await deleteDoc(doc(db, "temporaryEmployees", employeeId));
    showToast('Temporary employee deleted successfully', 'success');
  } catch (error) {
    console.error("Error deleting temporary employee: ", error);
    showToast('Error deleting temporary employee', 'error');
  }
}

// Setup event listeners
function setupEventListeners() {
  const formEl = document.getElementById('employeeForm');
  if (formEl) formEl.addEventListener('submit', handleFormSubmit);
  // File inputs listeners
  const qidPdf = document.getElementById('qidPdf');
  const passportPdf = document.getElementById('passportPdf');
  const profileImage = document.getElementById('profileImage');
  if (qidPdf) qidPdf.addEventListener('change', () => markPending('qid'));
  if (passportPdf) passportPdf.addEventListener('change', () => markPending('passport'));
  if (profileImage) profileImage.addEventListener('change', () => markPending('profile'));
  const searchEl = document.getElementById('searchInput');
  if (searchEl) searchEl.addEventListener('input', handleSearch);
  // Department filter removed from UI
  const confirmDeleteEl = document.getElementById('confirmDelete');
  if (confirmDeleteEl) confirmDeleteEl.addEventListener('click', handleConfirmDelete);
  // No terminated toggles; terminated are always visible with a visual indicator

  // Header sign-out button removed; sidebar sign-out remains
  const signOutBtnSidebar = document.getElementById('signOutBtnSidebar');
  if (signOutBtnSidebar) signOutBtnSidebar.addEventListener('click', handleSignOut);

  // Open employee modal button
  const openEmployeeModalBtn = document.getElementById('openEmployeeModalBtn');
  if (openEmployeeModalBtn) openEmployeeModalBtn.addEventListener('click', () => openEmployeeModal('employees', 'add'));
  const openTempEmployeeModalBtn = document.getElementById('openTempEmployeeModalBtn');
  if (openTempEmployeeModalBtn) openTempEmployeeModalBtn.addEventListener('click', () => openEmployeeModal('temporary', 'add'));

  // Sidebar navigation
  document.querySelectorAll('[data-nav]').forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.getAttribute('data-nav');
      setActiveSection(target);
      if (target === 'payroll') {
        renderPayrollTable();
      } else if (target === 'clients') {
        // Render live clients table
        try { renderClientsTable(); } catch {}
      } else if (target === 'clients-billing') {
        // Default month to current if empty and render client transactions view
        try {
          const m = document.getElementById('clientBillingMonth');
          if (m && !m.value) {
            const now = new Date();
            m.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
          }
        } catch {}
        // Always repopulate the client dropdown on navigation to ensure it's fresh
        try {
          const sel = document.getElementById('clientBillingClient');
          if (sel) {
            const prev = sel.value || '';
            const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
            const opts = ['<option value="">Select client…</option>']
              .concat(list.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
                .map(c => `<option value="${c.id}">${escapeHtml(c.name||'')}</option>`));
            sel.innerHTML = opts.join('');
            if (prev) { try { sel.value = prev; } catch {} }
            // Update header label
            try {
              const nameEl = document.getElementById('clientBillingSelectedName');
              const cur = list.find(x => x.id === (sel.value || ''));
              if (nameEl) nameEl.textContent = cur ? `— ${cur.name}` : '';
              const btn = document.getElementById('openClientPaymentBtn');
              if (btn) btn.disabled = !sel.value;
            } catch {}
          }
        } catch {}
        try { renderClientTransactions(); } catch {}
      } else if (target === 'assignments') {
        renderAssignmentsTable();
      } else if (target === 'accounts') {
        renderAccountsTable();
        renderCashflowTable?.();
        try { renderLedgerTable?.(); } catch {}
        try { setupLedgerGrid(); } catch {}
        // ensure default sub-tab shown and controls visibility updated
        setAccountsSubTab('overview');
        // re-wire in case this section was hidden before
        try { wireAccountsTabButtons(); } catch {}
        try { updateAccountsFundCard(); } catch {}
      }
    });
  });

  // Set Current Fund button (direct, no window dependency)
  const setFundBtn = document.getElementById('setCurrentFundBtn');
  if (setFundBtn) {
    setFundBtn.addEventListener('click', (e) => {
      e.preventDefault();
      e.stopPropagation();
      try { console.debug('[Fund] Set Current Fund button clicked'); } catch {}
      if (typeof openSetFundDialog === 'function') {
        openSetFundDialog();
      } else {
        console.error('[Fund] openSetFundDialog function not found');
        showToast && showToast('Fund dialog not available', 'error');
      }
    });
  } else {
    try { console.warn('[Fund] setCurrentFundBtn not found in DOM at startup'); } catch {}
  }
  // Delegated fallback: capture clicks anywhere for #setCurrentFundBtn
  document.addEventListener('click', (e) => {
    const btn = e.target && e.target.closest ? e.target.closest('#setCurrentFundBtn') : null;
    if (!btn) return;
    e.preventDefault();
    try { console.debug('[Fund] Delegated click for Set Current Fund'); } catch {}
    if (typeof openSetFundDialog === 'function') openSetFundDialog();
  }, true);
  const editFundForm = document.getElementById('editFundForm');
  if (editFundForm) {
    editFundForm.addEventListener('submit', (e) => {
      try { console.debug('[Fund] editFundForm submit'); } catch {}
      handleEditFundSubmit(e);
    });
  } else {
    try { console.warn('[Fund] editFundForm not found in DOM at startup'); } catch {}
  }

  // Sidebar collapse/expand controls
  const sidebarEl = document.getElementById('sidebar');
  const collapseBtn = document.getElementById('collapseSidebarBtn');
  const openSidebarBtn = document.getElementById('openSidebarBtn');
  const applySidebarState = (hidden) => {
    const body = document.body;
    if (!body) return;
    if (hidden) {
      body.classList.add('sidebar-collapsed');
    } else {
      body.classList.remove('sidebar-collapsed');
    }
  };
  // Load persisted state
  try {
    const saved = localStorage.getItem('crm_sidebar_hidden');
    applySidebarState(saved === '1');
  } catch {}
  if (collapseBtn) collapseBtn.addEventListener('click', () => {
    applySidebarState(true);
    try { localStorage.setItem('crm_sidebar_hidden', '1'); } catch {}
  });
  if (openSidebarBtn) openSidebarBtn.addEventListener('click', () => {
    applySidebarState(false);
    try { localStorage.setItem('crm_sidebar_hidden', '0'); } catch {}
  });

  // Login page controls
  const loginSignInBtn = document.getElementById('loginSignInBtn');
  const loginSignUpBtn = document.getElementById('loginSignUpBtn');
  const loginResetBtn = document.getElementById('loginResetBtn');
  const loginGoogleBtn = document.getElementById('loginGoogleBtn');
  if (loginSignInBtn) loginSignInBtn.addEventListener('click', emailPasswordSignIn);
  if (loginSignUpBtn) loginSignUpBtn.addEventListener('click', emailPasswordSignUp);
  if (loginResetBtn) loginResetBtn.addEventListener('click', emailPasswordReset);
  // Remove or hide Google sign-in since it's disabled
  if (loginGoogleBtn) loginGoogleBtn.style.display = 'none';

  // Close modals when clicking outside the dialog
  ['employeeModal', 'deleteModal', 'viewModal', 'payrollModal', 'payslipModal', 'paymentModal', 'editFundModal', 'clientViewModal'].forEach((id) => {
  // include payslipPreviewModal later via global fallback
    const modal = document.getElementById(id);
    if (modal) {
      modal.addEventListener('click', (e) => {
        // Close only when clicking the overlay itself (not inside dialog content)
        if (e.target === modal) {
          if (id === 'employeeModal') closeEmployeeModal();
          if (id === 'deleteModal') closeModal();
          if (id === 'viewModal') closeViewModal();
          if (id === 'payrollModal') closePayrollModal();
          if (id === 'payslipModal') closePayslipModal();
          if (id === 'paymentModal') closePaymentModal();
        }
      });
    }
  });

  // Global capture listener as a fallback to guarantee overlay clicks close modals
  document.addEventListener('click', (e) => {
  const employeeModalEl = document.getElementById('employeeModal');
  const deleteModalEl = document.getElementById('deleteModal');
  const viewModalEl = document.getElementById('viewModal');
  const payrollModalEl = document.getElementById('payrollModal');
  const payslipModalEl = document.getElementById('payslipModal');
  const paymentModalEl = document.getElementById('paymentModal');
  const payslipPreviewModalEl = document.getElementById('payslipPreviewModal');
  const editFundModalEl = document.getElementById('editFundModal');
  const clientViewModalEl = document.getElementById('clientViewModal');
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl) && !anyOpen(paymentModalEl) && !anyOpen(payslipPreviewModalEl) && !anyOpen(editFundModalEl) && !anyOpen(clientViewModalEl)) return;

    // If the click landed on an overlay (exact target is the overlay div), close it
    if (anyOpen(employeeModalEl) && e.target === employeeModalEl) {
      closeEmployeeModal();
    } else if (anyOpen(deleteModalEl) && e.target === deleteModalEl) {
      closeModal();
    } else if (anyOpen(viewModalEl) && e.target === viewModalEl) {
      closeViewModal();
    } else if (anyOpen(payrollModalEl) && e.target === payrollModalEl) {
      closePayrollModal();
    } else if (anyOpen(payslipModalEl) && e.target === payslipModalEl) {
      closePayslipModal();
    } else if (anyOpen(payslipPreviewModalEl) && e.target === payslipPreviewModalEl) {
      closePayslipPreviewModal();
    } else if (anyOpen(editFundModalEl) && e.target === editFundModalEl) {
      closeEditFundModal();
    } else if (anyOpen(clientViewModalEl) && e.target === clientViewModalEl) {
      try { window.closeClientViewModal && window.closeClientViewModal(); } catch {}
    }
  }, true);

  // Use pointerdown (captures mouse/touch/pen) to close even if click is prevented
  document.addEventListener('pointerdown', (e) => {
  const employeeModalEl = document.getElementById('employeeModal');
  const deleteModalEl = document.getElementById('deleteModal');
  const viewModalEl = document.getElementById('viewModal');
  const payrollModalEl = document.getElementById('payrollModal');
  const payslipModalEl = document.getElementById('payslipModal');
  const paymentModalEl = document.getElementById('paymentModal');
  const payslipPreviewModalEl = document.getElementById('payslipPreviewModal');
  const editFundModalEl = document.getElementById('editFundModal');
  const clientViewModalEl = document.getElementById('clientViewModal');
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl) && !anyOpen(paymentModalEl) && !anyOpen(payslipPreviewModalEl) && !anyOpen(editFundModalEl) && !anyOpen(clientViewModalEl)) return;

    // Close when pointerdown lands on overlay element itself
    if (anyOpen(employeeModalEl) && e.target === employeeModalEl) {
      closeEmployeeModal();
    } else if (anyOpen(deleteModalEl) && e.target === deleteModalEl) {
      closeModal();
    } else if (anyOpen(viewModalEl) && e.target === viewModalEl) {
      closeViewModal();
    } else if (anyOpen(payrollModalEl) && e.target === payrollModalEl) {
      closePayrollModal();
    } else if (anyOpen(payslipModalEl) && e.target === payslipModalEl) {
      closePayslipModal();
    } else if (anyOpen(paymentModalEl) && e.target === paymentModalEl) {
      closePaymentModal();
    } else if (anyOpen(payslipPreviewModalEl) && e.target === payslipPreviewModalEl) {
      closePayslipPreviewModal();
    } else if (anyOpen(editFundModalEl) && e.target === editFundModalEl) {
      closeEditFundModal();
    } else if (anyOpen(clientViewModalEl) && e.target === clientViewModalEl) {
      try { window.closeClientViewModal && window.closeClientViewModal(); } catch {}
    }
  }, true);

  // Close open modal on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const deleteModalEl = document.getElementById('deleteModal');
      const employeeModalEl = document.getElementById('employeeModal');
      const viewModalEl = document.getElementById('viewModal');
      const payrollModalEl = document.getElementById('payrollModal');
      const payslipModalEl = document.getElementById('payslipModal');
  const paymentModalEl2 = document.getElementById('paymentModal');
  const payslipPreviewModalEl2 = document.getElementById('payslipPreviewModal');
  const editFundModalEl2 = document.getElementById('editFundModal');
  const clientViewModalEl2 = document.getElementById('clientViewModal');
      if (deleteModalEl && deleteModalEl.classList.contains('show')) closeModal();
      if (employeeModalEl && employeeModalEl.classList.contains('show')) closeEmployeeModal();
      if (viewModalEl && viewModalEl.classList.contains('show')) closeViewModal();
      if (payrollModalEl && payrollModalEl.classList.contains('show')) closePayrollModal();
      if (payslipModalEl && payslipModalEl.classList.contains('show')) closePayslipModal();
      if (paymentModalEl2 && paymentModalEl2.classList.contains('show')) closePaymentModal();
      if (payslipPreviewModalEl2 && payslipPreviewModalEl2.classList.contains('show')) closePayslipPreviewModal();
      if (editFundModalEl2 && editFundModalEl2.classList.contains('show')) closeEditFundModal();
      if (clientViewModalEl2 && clientViewModalEl2.classList.contains('show')) { try { window.closeClientViewModal && window.closeClientViewModal(); } catch {} }
    }
  });

  // Track modal visibility to toggle body.modal-open for overlay/scroll control
  try {
  const body = document.body;
    const modalIds = ['employeeModal','deleteModal','viewModal','payrollModal','payslipModal','paymentModal','editFundModal','payslipPreviewModal','transferModal','cashTxnModal','clientModal','assignmentModal'];
  // Include Client View modal in global open-state tracking so backdrop/body class work
  if (!modalIds.includes('clientViewModal')) modalIds.push('clientViewModal');
    const isAnyOpen = () => modalIds.some(id => {
      const el = document.getElementById(id);
      return !!(el && el.classList && el.classList.contains('show'));
    });
    const syncBodyClass = () => {
      if (!body) return;
      if (isAnyOpen()) body.classList.add('modal-open'); else body.classList.remove('modal-open');
      const scrim = document.getElementById('globalBackdrop');
      if (scrim) scrim.style.display = isAnyOpen() ? 'block' : 'none';
    };
    // Observe class changes on each modal
    const obs = new MutationObserver(syncBodyClass);
    modalIds.forEach(id => {
      const el = document.getElementById(id);
      if (el) obs.observe(el, { attributes: true, attributeFilter: ['class'] });
    });
    // Also run on window focus/blur and after any click to be safe
    document.addEventListener('click', syncBodyClass, true);
    document.addEventListener('keydown', syncBodyClass, true);
    setInterval(syncBodyClass, 1000); // safety net
  } catch {}

  // Payroll month input is wired in setupEventListeners
  // Client Transactions controls
  const clientBillingMonthEl = document.getElementById('clientBillingMonth');
  if (clientBillingMonthEl && !clientBillingMonthEl.__wired) {
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
    clientBillingMonthEl.value = clientBillingMonthEl.value || ym;
    clientBillingMonthEl.addEventListener('change', () => {
      try { renderClientTransactions(); } catch {}
    });
    clientBillingMonthEl.__wired = true;
  }
  // Client selector for Client Transactions
  const clientBillingClientEl = document.getElementById('clientBillingClient');
  if (clientBillingClientEl && !clientBillingClientEl.__wired) {
    const populate = () => {
      try {
        const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
        const opts = ['<option value="">Select client…</option>']
          .concat(list.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
            .map(c => `<option value="${c.id}">${escapeHtml(c.name||'')}</option>`));
        clientBillingClientEl.innerHTML = opts.join('');
      } catch {}
    };
    populate();
    clientBillingClientEl.addEventListener('change', () => {
      try {
        const sel = document.getElementById('clientBillingClient');
        const nameEl = document.getElementById('clientBillingSelectedName');
        const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
        const c = list.find(x => x.id === (sel?.value || ''));
        if (nameEl) nameEl.textContent = c ? `— ${c.name}` : '';
        // Toggle Record Payment button
        const btn = document.getElementById('openClientPaymentBtn');
        if (btn) btn.disabled = !sel?.value;
      } catch {}
      try { renderClientTransactions(); } catch {}
    });
    clientBillingClientEl.__wired = true;
  }

  // Enable/disable Record Payment on month change
  if (clientBillingMonthEl && !clientBillingMonthEl.__wired2) {
    clientBillingMonthEl.addEventListener('change', () => {
      try {
        const btn = document.getElementById('openClientPaymentBtn');
        const sel = document.getElementById('clientBillingClient');
        if (btn) btn.disabled = !(sel?.value);
      } catch {}
    });
    clientBillingMonthEl.__wired2 = true;
  }

  // Wire Record Payment button
  const openClientPaymentBtn = document.getElementById('openClientPaymentBtn');
  if (openClientPaymentBtn && !openClientPaymentBtn.__wired) {
    openClientPaymentBtn.addEventListener('click', (e) => {
      e.preventDefault();
      openClientPaymentModal();
    });
    openClientPaymentBtn.__wired = true;
  }

  // Auto-refresh Client Transactions when clients/assignments change
  document.addEventListener('clients:updated', () => {
    // Rebuild Client Transactions dropdown and update title/button when clients update
    try {
      const sel = document.getElementById('clientBillingClient');
      if (sel) {
        const selected = sel.value || '';
        const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
        const opts = ['<option value="">Select client…</option>']
          .concat(list.slice().sort((a,b)=> (a.name||'').localeCompare(b.name||''))
            .map(c => `<option value="${c.id}">${escapeHtml(c.name||'')}</option>`));
        sel.innerHTML = opts.join('');
        if (selected) { try { sel.value = selected; } catch {} }
        // Update header name and button
        try {
          const nameEl = document.getElementById('clientBillingSelectedName');
          const cur = list.find(x => x.id === (sel.value || ''));
          if (nameEl) nameEl.textContent = cur ? `— ${cur.name}` : '';
          const btn = document.getElementById('openClientPaymentBtn');
          if (btn) btn.disabled = !sel.value;
        } catch {}
      }
    } catch {}
    // If section visible, re-render the table
    const sec = document.getElementById('clientsBillingSection');
    if (sec && sec.style.display !== 'none') {
      try { renderClientTransactions(); } catch {}
    }
  });
  document.addEventListener('assignments:updated', () => {
    const sec = document.getElementById('clientsBillingSection');
    if (sec && sec.style.display !== 'none') {
      try { renderClientTransactions(); } catch {}
    }
  });
  // Export/Print and tab buttons are wired by the payroll module during init

  // Accordion toggles inside View modal
  document.addEventListener('click', (e) => {
    const btn = e.target.closest('[data-acc-toggle]');
    if (!btn) return;
    const targetSel = btn.getAttribute('data-acc-target');
    if (!targetSel) return;
    const pane = document.querySelector(targetSel);
    if (!pane) return;
    const isHidden = pane.classList.contains('hidden');
    // Collapse other sections? We'll keep independent for now.
    pane.classList.toggle('hidden', !isHidden);
    // Rotate chevron
    const chev = btn.querySelector('.fa-chevron-down');
    if (chev) chev.style.transform = isHidden ? 'rotate(180deg)' : '';
    // ARIA state
    btn.setAttribute('aria-expanded', String(isHidden));
    // Lazy-load documents when opening Documents section
    if (isHidden && targetSel === '#acc-docs' && currentViewCtx && currentViewCtx.id && !currentViewCtx.docsLoaded) {
      loadCurrentViewDocuments().catch(() => {});
    }
  });

  // Global delegated handlers for quick Income/Expense buttons (robust to rewiring)
  document.addEventListener('click', (e) => {
    const btn = e.target && e.target.closest && e.target.closest('#openIncomeTxnBtn, #openExpenseTxnBtn, #openIncomeTxnBtn2, #openExpenseTxnBtn2');
    if (!btn) return;
    e.preventDefault();
    const isIncome = btn.id.includes('Income');
    try {
      if (window.openCashTxnModal) {
        window.openCashTxnModal({ type: isIncome ? 'in' : 'out' });
      }
    } catch {}
  });

  // Removed: header quick access button to open Documents section

  // On large screens, auto-expand all sections by default when modal mounts
  const mq = window.matchMedia('(min-width: 1024px)');
  const ensureExpandedOnDesktop = () => {
    if (!mq.matches) return;
    ['#acc-overview', '#acc-contact', '#acc-employment', '#acc-bank', '#acc-docs'].forEach(sel => {
      const pane = document.querySelector(sel);
      const btn = document.querySelector(`[data-acc-target="${sel}"]`);
      if (!pane || !btn) return;
      pane.classList.remove('hidden');
      btn.setAttribute('aria-expanded', 'true');
      const chev = btn.querySelector('.fa-chevron-down');
      if (chev) chev.style.transform = 'rotate(180deg)';
    });
  };
  mq.addEventListener?.('change', ensureExpandedOnDesktop);
  ensureExpandedOnDesktop();

  // View modal action bar handlers
  const viewCloseIcon = document.getElementById('viewCloseIcon');
  if (viewCloseIcon) viewCloseIcon.addEventListener('click', () => closeViewModal());
  const viewActionClose = document.getElementById('viewActionClose');
  if (viewActionClose) viewActionClose.addEventListener('click', () => closeViewModal());
  const viewActionEdit = document.getElementById('viewActionEdit');
  if (viewActionEdit) viewActionEdit.addEventListener('click', () => {
    const id = currentViewCtx?.id;
    const which = currentViewCtx?.which === 'temporary' ? 'temporary' : 'employees';
    if (!id) return;
    closeViewModal();
    editEmployee(id, which);
  });
  const viewActionDelete = document.getElementById('viewActionDelete');
  if (viewActionDelete) viewActionDelete.addEventListener('click', () => {
    const id = currentViewCtx?.id;
    const which = currentViewCtx?.which === 'temporary' ? 'temporary' : 'employees';
    if (!id) return;
    closeViewModal();
    openDeleteModal(id, which);
  });
  const viewActionTerminate = document.getElementById('viewActionTerminate');
  if (viewActionTerminate) viewActionTerminate.addEventListener('click', async () => {
    const id = currentViewCtx?.id;
    const which = currentViewCtx?.which === 'temporary' ? 'temporary' : 'employees';
    if (!id) return;
    const ok = confirm('Terminate this employee? This will disable payroll actions but keep their record.');
    if (!ok) return;
    try {
      const base = which === 'temporary' ? 'temporaryEmployees' : 'employees';
      // Normalize joinDate robustly (string variants, timestamp, Date)
      const list = which === 'temporary' ? temporaryEmployees : employees;
      const emp = list.find(e => e.id === id);
      const joinDateNorm = normalizeJoinDate(emp?.joinDate);
      const payload = cleanData({ terminated: true, terminatedAt: serverTimestamp(), joinDate: joinDateNorm ?? undefined });
      await updateDoc(doc(db, base, id), payload);
      showToast('Employee terminated', 'success');
      closeViewModal();
      // Refresh payroll UI
      renderPayrollTable();
      // Immediate local update for instant visual feedback
      try {
        if (which === 'temporary') {
          const idx = temporaryEmployees.findIndex(e => e.id === id);
          if (idx !== -1) temporaryEmployees[idx] = { ...temporaryEmployees[idx], terminated: true };
          renderTemporaryTable();
        } else {
          const idx = employees.findIndex(e => e.id === id);
          if (idx !== -1) employees[idx] = { ...employees[idx], terminated: true };
          renderEmployeeTable();
        }
      } catch {}
    } catch (e) {
      console.error('Terminate failed', e);
      const msg = (e && e.code === 'permission-denied')
        ? 'Failed to terminate: Start Date format is invalid. Please edit the employee, re-select a valid Start Date, and try again.'
        : 'Failed to terminate employee';
      showToast(msg, 'error');
    }
  });
  const viewActionReinstate = document.getElementById('viewActionReinstate');
  if (viewActionReinstate) viewActionReinstate.addEventListener('click', async () => {
    const id = currentViewCtx?.id;
    const which = currentViewCtx?.which === 'temporary' ? 'temporary' : 'employees';
    if (!id) return;
    const ok = confirm('Reinstate this employee? This will enable payroll actions again.');
    if (!ok) return;
    try {
      const base = which === 'temporary' ? 'temporaryEmployees' : 'employees';
      // Normalize joinDate robustly
      const list = which === 'temporary' ? temporaryEmployees : employees;
      const emp = list.find(e => e.id === id);
      const joinDateNorm = normalizeJoinDate(emp?.joinDate);
      await updateDoc(doc(db, base, id), cleanData({ terminated: false, joinDate: joinDateNorm ?? undefined }));
      showToast('Employee reinstated', 'success');
      closeViewModal();
      renderPayrollTable();
      // Immediate local update for instant visual feedback
      try {
        if (which === 'temporary') {
          const idx = temporaryEmployees.findIndex(e => e.id === id);
          if (idx !== -1) temporaryEmployees[idx] = { ...temporaryEmployees[idx], terminated: false };
          renderTemporaryTable();
        } else {
          const idx = employees.findIndex(e => e.id === id);
          if (idx !== -1) employees[idx] = { ...employees[idx], terminated: false };
          renderEmployeeTable();
        }
      } catch {}
    } catch (e) {
      console.error('Reinstate failed', e);
      const msg = (e && e.code === 'permission-denied')
        ? 'Failed to reinstate: Start Date format is invalid. Please edit the employee, re-select a valid Start Date, and try again.'
        : 'Failed to reinstate employee';
      showToast(msg, 'error');
    }
  });

  // Mobile swipe-to-close for View modal
  setupSwipeToClose();

  // Payroll Monthly Report: month input default + change wiring
  const payrollMonthInput = document.getElementById('payrollMonth');
  if (payrollMonthInput) {
    // Set default value if empty (current YYYY-MM)
    if (!payrollMonthInput.value) {
      const now = new Date();
      payrollMonthInput.value = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    }
    if (!payrollMonthInput.__wired) {
      payrollMonthInput.addEventListener('change', () => {
        // Only render if we're on the report tab
        const reportTab = document.getElementById('payrollTabReport');
        if (reportTab && reportTab.style.display !== 'none') {
          renderPayrollFrame({
            getEmployees: () => employees,
            getTemporaryEmployees: () => temporaryEmployees,
            month: payrollMonthInput.value
          });
        }
        // Ensure monthly balances exist for the selected month
        try {
          const month = payrollMonthInput.value;
          if (month && /^\d{4}-\d{2}$/.test(month)) {
            const everyone = [...employees, ...temporaryEmployees];
            everyone.forEach(emp => { try { ensureMonthlyBalance(emp.id, month); } catch {} });
          }
        } catch {}
      });
      payrollMonthInput.__wired = true;
    }
  }
}

// Client Payment Modal controls
window.openClientPaymentModal = function() {
  const sel = document.getElementById('clientBillingClient');
  const monthEl = document.getElementById('clientBillingMonth');
  const modal = document.getElementById('clientPaymentModal');
  if (!sel || !monthEl || !modal) return;
  const clientId = sel.value || '';
  if (!clientId) { showToast('Choose a client first', 'warning'); return; }
  const ym = monthEl.value || '';
  if (!/^\d{4}-\d{2}$/.test(ym)) { showToast('Choose a valid month', 'warning'); return; }
  // Prefill labels
  try {
    const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
    const c = list.find(x => x.id === clientId);
    const name = c?.name || '—';
    const mm = new Date(Number(ym.slice(0,4)), Number(ym.slice(5,7)) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' });
    const dateStr = `${ym}-01`;
    const nameEl = document.getElementById('clientPaymentClientName');
    const monthLbl = document.getElementById('clientPaymentMonth');
    const dateInput = document.getElementById('cpDate');
    if (nameEl) nameEl.textContent = name;
    if (monthLbl) monthLbl.textContent = mm;
    if (dateInput) dateInput.value = dateStr;
  } catch {}
  // Populate accounts
  try {
    const selAcc = document.getElementById('cpAccount');
    const accs = __getLocalAccounts().filter(a => (String(a.type||'').toLowerCase() === 'asset'));
    selAcc.innerHTML = accs.map(a => `<option value="${a.id}">${escapeHtml(a.name||'')}</option>`).join('');
  } catch {}
  // Reset amount/notes
  try { const a = document.getElementById('cpAmount'); if (a) a.value = ''; } catch {}
  try { const n = document.getElementById('cpNotes'); if (n) n.value = ''; } catch {}
  modal.classList.add('show');
}
window.closeClientPaymentModal = function() {
  const modal = document.getElementById('clientPaymentModal');
  if (modal) modal.classList.remove('show');
}

document.addEventListener('submit', async (e) => {
  if (e.target && e.target.id === 'clientPaymentForm') {
    e.preventDefault();
    const date = document.getElementById('cpDate')?.value || '';
    const accountId = document.getElementById('cpAccount')?.value || '';
    const amount = Math.abs(Number(document.getElementById('cpAmount')?.value || 0)) || 0;
    const notes = document.getElementById('cpNotes')?.value || '';
    const sel = document.getElementById('clientBillingClient');
    const monthEl = document.getElementById('clientBillingMonth');
    const clientId = sel?.value || '';
    const ym = monthEl?.value || '';
    if (!date || !accountId || !(amount>0) || !clientId || !/^\d{4}-\d{2}$/.test(ym)) { showToast('Fill all fields correctly', 'warning'); return; }
    try {
      // Save as cashflow IN (payment received) and tag with clientId for reporting
      const inRef = await addDoc(collection(db, 'cashflows'), cleanData({
        date,
        type: 'in',
        accountId,
        amount,
        category: 'Client Payment',
        clientId,
        month: ym,
        notes,
        createdAt: new Date().toISOString(),
        createdBy: auth?.currentUser?.uid || undefined,
        createdByEmail: auth?.currentUser?.email || undefined
      }));
      // No AR offset needed per current requirement; payments impact Fund directly via selected Asset account
      // Update local grid cache credited for this (clientId|ym)
      const key = `${clientId}|${ym}`;
      const cur = (__clientBillingGridCache[key] ||= {});
      const prevCred = Number(cur.credited||0);
      cur.credited = prevCred + amount;
      showToast('Payment recorded', 'success');
      closeClientPaymentModal();
      // Re-render grid to reflect credited/outstanding
      try { renderClientTransactions(); } catch {}
    } catch (err) {
      console.error('Client payment save failed', err);
      showToast('Failed to save payment', 'error');
    }
  }
});

// Toggle visible section and active nav
function setActiveSection(key) {
  const sections = document.querySelectorAll('[data-section]');
  sections.forEach(sec => {
    if (sec.getAttribute('data-section') === key) {
      sec.style.display = '';
    } else {
      sec.style.display = 'none';
    }
  });

  document.querySelectorAll('[data-nav]').forEach(btn => {
    const isActive = btn.getAttribute('data-nav') === key;
    if (isActive) {
      btn.classList.add('bg-gray-100');
    } else {
      btn.classList.remove('bg-gray-100');
    }
  });

  // Toggle which add button shows based on section
  const addBtn = document.getElementById('openEmployeeModalBtn');
  const addTempBtn = document.getElementById('openTempEmployeeModalBtn');
  const addClientBtn = document.getElementById('openClientModalBtn');
  if (addBtn && addTempBtn) {
    if (key === 'temporary') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = '';
      if (addClientBtn) addClientBtn.style.display = 'none';
    } else if (key === 'employees') {
      addBtn.style.display = '';
      addTempBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = 'none';
    } else if (key === 'clients') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = '';
    } else {
      // On dashboard, hide both
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = 'none';
    }
  }

  // Clients: open modal
  // Clients/Assignments events are managed in their modules during init

  // When navigating to Payroll, ensure the table/frame are freshly rendered
  if (key === 'payroll') {
    // Always open Payroll on the Table sub-tab
    setPayrollSubTab('table');
    renderPayrollTable();
    try { ensureCurrentMonthBalances(); } catch {}
  }
}

// Clients/Assignments logic moved into modules

// Accounts sub-tabs (redesigned) — Overview, Ledger, Settings (Transactions removed)
document.addEventListener('click', (e) => {
  const btn = e.target && e.target.closest && e.target.closest('#accountsSubTabOverviewBtn, #accountsSubTabLedgerBtn, #accountsSubTabSettingsBtn');
  if (!btn) return;
  let which = 'overview';
  if (btn.id === 'accountsSubTabLedgerBtn') which = 'ledger';
  if (btn.id === 'accountsSubTabSettingsBtn') which = 'settings';
  setAccountsSubTab(which);
});

function setAccountsSubTab(which) {
  const tabs = {
    overview: document.getElementById('accountsTabOverview'),
    ledger: document.getElementById('accountsTabLedger'),
    settings: document.getElementById('accountsTabSettings'),
  };
  const btns = {
    overview: document.getElementById('accountsSubTabOverviewBtn'),
    ledger: document.getElementById('accountsSubTabLedgerBtn'),
    settings: document.getElementById('accountsSubTabSettingsBtn'),
  };
  const activate = (btn) => { if (!btn) return; btn.classList.add('font-semibold','text-indigo-600','border-b-2','border-indigo-600'); btn.classList.remove('text-gray-600'); };
  const deactivate = (btn) => { if (!btn) return; btn.classList.remove('font-semibold','text-indigo-600','border-b-2','border-indigo-600'); btn.classList.add('text-gray-600'); };
  // Toggle visibility
  Object.entries(tabs).forEach(([k, el]) => { if (el) el.style.display = (k === which ? '' : 'none'); });
  Object.entries(btns).forEach(([k, el]) => { if (k === which) activate(el); else deactivate(el); });
  // Render content per tab
  if (which === 'overview') {
    renderAccountsOverview();
  } else if (which === 'ledger') {
    try { refreshLedgerAccounts?.(); } catch {}
    try { renderLedgerTable?.(); } catch {}
    try { setupLedgerGrid(); } catch {}
  } else if (which === 'settings') {
    renderAccountsTable();
  }
}

// (no-op placeholder removed)

// Expose for inline handlers (fallback)
// Note: module-scoped functions are not global; attach explicitly
try { window.setAccountsSubTab = setAccountsSubTab; } catch {}

// Robust wiring for Accounts sub-tab buttons (direct listeners)
function wireAccountsTabButtons() {
  const ids = ['accountsSubTabOverviewBtn','accountsSubTabLedgerBtn','accountsSubTabSettingsBtn'];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (el && !el.__wired) {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        const which = id.includes('Overview') ? 'overview' : id.includes('Ledger') ? 'ledger' : 'settings';
        setAccountsSubTab(which);
      });
      el.__wired = true;
    }
  });
}

// Overview renderer: totals for today/this month and recent 10 transactions
function renderAccountsOverview() {
  try { updateAccountsFundCard(); } catch {}
  const flows = (Array.isArray(window.__cashflowAll) ? window.__cashflowAll.slice() : (__fundCashflowsCache||[])).sort((a,b)=>String(b.date||'').localeCompare(String(a.date||'')));
  const todayYmd = (()=>{const d=new Date();return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;})()
  const monthYm = todayYmd.slice(0,7);
  let tIn=0,tOut=0,mIn=0,mOut=0;
  const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
  const isAsset = (id) => !id || assetIds.includes(id);
  for (const t of flows) {
    const typ = String(t.type||'').toLowerCase();
    const amt = Math.abs(Number(t.amount||0))||0;
    const d = String(t.date||'');
    if (!isAsset(t.accountId)) continue;
    if (d === todayYmd) { if (typ==='in'||typ==='income'||typ==='credit') tIn+=amt; else if (typ==='out'||typ==='expense'||typ==='debit') tOut+=amt; }
    if (d.startsWith(monthYm+'-')) { if (typ==='in'||typ==='income'||typ==='credit') mIn+=amt; else if (typ==='out'||typ==='expense'||typ==='debit') mOut+=amt; }
  }
  const fmt = (n)=>`$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const byId = (x)=>document.getElementById(x);
  const setText=(id,val)=>{const el=byId(id); if (el) el.textContent = fmt(val)};
  setText('overviewTodayIn', tIn);
  setText('overviewTodayOut', tOut);
  setText('overviewMonthIn', mIn);
  setText('overviewMonthOut', mOut);
  // Recent transactions table
  const recentEmpty = byId('accountsRecentEmpty');
  const tbl = byId('accountsRecentTable');
  const tbody = byId('accountsRecentTbody');
  if (!tbody) return;
  const rows = flows.slice(0, 10);
  if (!rows.length) {
    if (recentEmpty) recentEmpty.style.display = '';
    if (tbl) tbl.style.display = 'none';
    tbody.innerHTML = '';
    return;
  }
  if (recentEmpty) recentEmpty.style.display = 'none';
  if (tbl) tbl.style.display = '';
  const accs = __getLocalAccounts();
  const accName = (id)=> (accs.find(a=>a.id===id)?.name) || '';
  tbody.innerHTML = rows.map(t=>{
    const typ = String(t.type||'').toLowerCase();
    const amt = Math.abs(Number(t.amount||0))||0;
    return `<tr class="hover:bg-gray-50">
      <td class="px-4 py-2">${escapeHtml(t.date||'')}</td>
      <td class="px-4 py-2">${escapeHtml(accName(t.accountId) || t.accountName || '')}</td>
      <td class="px-4 py-2">${(typ==='in'||typ==='income'||typ==='credit')?'In':'Out'}</td>
      <td class="px-4 py-2">${escapeHtml(t.category||'')}</td>
      <td class="px-4 py-2 text-right">${fmt(amt)}</td>
      <td class="px-4 py-2">${escapeHtml(t.notes||'')}</td>
    </tr>`;
  }).join('');
  // Make the Recent Transactions table grid-like (read-only)
  try {
    makeTableGrid(tbody, {
      tableId: 'accountsRecent',
      editableCols: [], // read-only
      numericCols: [4],
      fields: ['date','account','type','category','amount','notes'],
      getRowKey: (r) => {
        const tr = tbody.querySelectorAll('tr')[r]; if (!tr) return String(r);
        const date = tr.children[0]?.textContent||'';
        const acc = tr.children[1]?.textContent||'';
        const typ = tr.children[2]?.textContent||'';
        const cat = tr.children[3]?.textContent||'';
        const amt = tr.children[4]?.getAttribute('data-raw')||tr.children[4]?.textContent||'';
        const notes = tr.children[5]?.textContent||'';
        return `${date}|${acc}|${typ}|${cat}|${amt}|${notes}`;
      }
    });
  } catch {}
}

// Initialize grid on Ledger table after it renders
function setupLedgerGrid() {
  const tbody = document.getElementById('ledgerTableBody');
  if (!tbody) return;
  if (!tbody.querySelector('tr')) return; // nothing to grid
  makeTableGrid(tbody, {
    tableId: 'accountsLedger',
    editableCols: [1], // allow editing Description locally
    numericCols: [2,3,4], // Debit, Credit, Balance numeric formatting
    fields: ['date','description','debit','credit','balance'],
    getRowKey: (r) => {
      const tr = tbody.querySelectorAll('tr')[r]; if (!tr) return String(r);
      const date = tr.children[0]?.textContent||'';
      const desc = tr.children[1]?.textContent||'';
      const deb = tr.children[2]?.getAttribute('data-raw')||tr.children[2]?.textContent||'';
      const cred = tr.children[3]?.getAttribute('data-raw')||tr.children[3]?.textContent||'';
      const bal = tr.children[4]?.getAttribute('data-raw')||tr.children[4]?.textContent||'';
      return `${date}|${desc}|${deb}|${cred}|${bal}`;
    },
    recompute: () => { /* balance left read-only for now */ }
  });
}

// Transactions tab: date range and category filters + export CSV
// Transactions tab removed: filters and CSV export helpers deleted

// Transfer modal: open/close and submit
window.openTransferModal = function() {
  try { populateTransferAccounts(); } catch {}
  const d = document.getElementById('tfDate'); if (d) d.valueAsDate = new Date();
  const m = document.getElementById('transferModal'); if (m) m.classList.add('show');
}
window.closeTransferModal = function() { const m = document.getElementById('transferModal'); if (m) m.classList.remove('show'); }

function populateTransferAccounts() {
  const fromSel = document.getElementById('tfFromAccount');
  const toSel = document.getElementById('tfToAccount');
  const accs = __getLocalAccounts().filter(a => (String(a.type||'').toLowerCase() === 'asset'));
  const opts = accs.map(a => `<option value="${a.id}">${escapeHtml(a.name||'')}</option>`);
  if (fromSel) fromSel.innerHTML = opts.join('');
  if (toSel) toSel.innerHTML = opts.join('');
}

document.addEventListener('submit', async (e) => {
  if (e.target && e.target.id === 'transferForm') {
    e.preventDefault();
    const date = document.getElementById('tfDate')?.value || '';
    const from = document.getElementById('tfFromAccount')?.value || '';
    const to = document.getElementById('tfToAccount')?.value || '';
    const amount = Math.abs(Number(document.getElementById('tfAmount')?.value || 0)) || 0;
    const notes = document.getElementById('tfNotes')?.value || '';
    if (!date || !from || !to || !(amount > 0)) { showToast('Fill all fields with a positive amount', 'warning'); return; }
    if (from === to) { showToast('Choose two different accounts', 'warning'); return; }
    try {
      // Create two cashflow entries: OUT from source, IN to destination
      await addDoc(collection(db, 'cashflows'), cleanData({ date, type:'out', accountId: from, amount, category: 'Transfer', notes: `Transfer to ${to}. ${notes}`||undefined, createdAt: new Date().toISOString() }));
      await addDoc(collection(db, 'cashflows'), cleanData({ date, type:'in', accountId: to, amount, category: 'Transfer', notes: `Transfer from ${from}. ${notes}`||undefined, createdAt: new Date().toISOString() }));
      showToast('Transfer saved', 'success');
      try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
      closeTransferModal();
    } catch (err) {
      console.warn('Transfer failed', err);
      showToast('Failed to save transfer', 'error');
    }
  }
});

// Compute and render Current Fund Available
let __cashflowShadow = [];
function updateAccountsFundCard() {
  // Prefer dedicated caches; fallback to module-provided shadows
  // Opening fund comes from stats/fund (bank-style)
  const openingTotal = Number(__fundOpening || 0);
  // Net cashflow across all cashflows (asset account filtering removed)
  const flows = (__fundCashflowsCache && __fundCashflowsCache.length)
    ? __fundCashflowsCache
    : ((Array.isArray(__cashflowShadow) && __cashflowShadow.length)
      ? __cashflowShadow
      : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []));
  let inSum = 0, outSum = 0;
  const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
  const isAsset = (id) => !id || assetIds.includes(id);
  for (const t of flows) {
    if (!t) continue;
    if (!isAsset(t.accountId)) continue; // exclude non-asset accounts from Fund
    const typeStr = String(t.type || '').toLowerCase();
    const amt = Math.abs(Number(t.amount || 0)) || 0;
    if (typeStr === 'in' || typeStr === 'income' || typeStr === 'credit') inSum += amt;
    else if (typeStr === 'out' || typeStr === 'expense' || typeStr === 'debit') outSum += amt;
  }
  const net = inSum - outSum;
  const total = openingTotal + net;
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const el = document.getElementById('accountsFundValue');
  if (el) el.textContent = fmt(total);
  const asOf = document.getElementById('accountsFundAsOf');
  if (asOf) asOf.textContent = `as of ${new Date().toLocaleString()}`;
  // Update modal computed label if open
  const comp = document.getElementById('computedFundLabel');
  if (comp) comp.textContent = fmt(total);
}

// Dedicated snapshot-based fund updater
function subscribeFundCardSnapshots() {
  // Cashflows snapshot
  try {
    if (__unsubFundCashflows) { __unsubFundCashflows(); __unsubFundCashflows = null; }
  } catch {}
  __unsubFundCashflows = onSnapshot(collection(db, 'cashflows'), (snap) => {
    __fundCashflowsCache = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    // Also keep the global cache in sync for other listeners
    try { window.__cashflowAll = __fundCashflowsCache.slice(); } catch {}
    try { updateAccountsFundCard(); } catch {}
  }, (err) => { console.warn('Fund cashflows snapshot error', err); });

  // Stats/fund snapshot (opening)
  try {
    if (__unsubFundStats) { __unsubFundStats(); __unsubFundStats = null; }
  } catch {}
  __unsubFundStats = onSnapshot(doc(db, 'stats', 'fund'), (snap) => {
    if (snap && snap.exists()) {
      const d = snap.data();
      __fundOpening = Number(d.opening || d.value || 0) || 0; // prefer explicit opening; fallback to value for legacy
    } else {
      __fundOpening = 0;
    }
    try { updateAccountsFundCard(); } catch {}
  }, (err) => { console.warn('Fund stats snapshot error', err); });
}

// Deterministic recompute: read stats/fund opening + cashflows from Firestore, compute total, and persist to stats/fund
async function recomputeFundFromFirestoreAndPersist() {
  try {
    // Ensure stats/fund exists; read opening
    let openingTotal = 0;
    try {
      const fundSnap = await getDoc(doc(db, 'stats', 'fund'));
      if (fundSnap.exists()) {
        const d = fundSnap.data();
        openingTotal = Number(d.opening || d.value || 0) || 0; // backward compatible
      } else {
        // initialize with opening=0
        await setDoc(doc(db, 'stats', 'fund'), cleanData({ opening: 0, value: 0, inSum: 0, outSum: 0, asOf: new Date().toISOString() }));
        openingTotal = 0;
      }
    } catch {}
    const cfSnap = await getDocs(collection(db, 'cashflows'));
    const flows = cfSnap.docs.map(d => ({ id: d.id, ...d.data() }));
    const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
    const isAsset = (id) => !id || assetIds.includes(id);
    let inSum = 0, outSum = 0;
    for (const t of flows) {
      if (!isAsset(t.accountId)) continue;
      const amt = Math.abs(Number(t?.amount || 0)) || 0;
      const typeStr = String(t?.type || '').toLowerCase();
      if (typeStr === 'in' || typeStr === 'income' || typeStr === 'credit') inSum += amt;
      else if (typeStr === 'out' || typeStr === 'expense' || typeStr === 'debit') outSum += amt;
    }
    const total = openingTotal + (inSum - outSum);
    // Persist a small summary doc for robustness and quick reads if needed
    const payload = cleanData({
      opening: Number(openingTotal),
      value: Number(total),
      inSum: Number(inSum),
      outSum: Number(outSum),
      asOf: new Date().toISOString()
    });
    try { await setDoc(doc(db, 'stats', 'fund'), payload); } catch {}
    // Update UI now
    try { updateAccountsFundCard(); } catch {}
    return total;
  } catch (e) {
    console.warn('Fund recompute failed', e);
    // Still attempt to update UI from current caches
    try { updateAccountsFundCard(); } catch {}
    return null;
  }
}

// Expose for modules
try { window.__recomputeFund = recomputeFundFromFirestoreAndPersist; } catch {}

// Ensure a default Asset account exists (e.g., Cash/Bank) and return its id
async function ensureAssetAccount(keyword, fallbackName) {
  const list = __getLocalAccounts();
  const match = (list || []).find(a => (a.type || '').toLowerCase() === 'asset' && (a.name || '').toLowerCase().includes(keyword.toLowerCase()));
  if (match) return match.id;
  // Create new
  try {
    const payload = cleanData({ name: fallbackName, type: 'Asset', opening: 0, createdAt: new Date().toISOString() });
    const ref = await addDoc(collection(db, 'accounts'), payload);
    return ref.id;
  } catch (e) {
    console.warn('Failed to create default asset account', fallbackName, e);
    return '';
  }
}

// Ensure a Receivables account (non-Asset) exists; prefer Liability type so it doesn't affect Fund
async function ensureReceivableAccount(keyword, fallbackName) {
  const list = __getLocalAccounts();
  const lower = (s) => String(s||'').toLowerCase();
  const match = (list || []).find(a => lower(a.name).includes(keyword.toLowerCase()) && lower(a.type) !== 'asset');
  if (match) return match.id;
  try {
    const payload = cleanData({ name: fallbackName, type: 'Liability', opening: 0, createdAt: new Date().toISOString() });
    const ref = await addDoc(collection(db, 'accounts'), payload);
    return ref.id;
  } catch (e) {
    console.warn('Failed to create receivable account', fallbackName, e);
    return '';
  }
}

// Edit Fund Modal controls
// Compute current fund total from caches (opening + net(cashflows))
function __getCurrentFundTotal() {
  try {
    const openingTotal = Number(__fundOpening || 0);
    const flows = (__fundCashflowsCache && __fundCashflowsCache.length)
      ? __fundCashflowsCache
      : ((Array.isArray(window.__cashflowAll) && window.__cashflowAll.length) ? window.__cashflowAll : []);
    let inSum = 0, outSum = 0;
    const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
    const isAsset = (id) => !id || assetIds.includes(id);
    for (const t of flows) {
      if (!t) continue;
      if (!isAsset(t.accountId)) continue;
      const typeStr = String(t.type || '').toLowerCase();
      const amt = Math.abs(Number(t.amount || 0)) || 0;
      if (typeStr === 'in' || typeStr === 'income' || typeStr === 'credit') inSum += amt;
      else if (typeStr === 'out' || typeStr === 'expense' || typeStr === 'debit') outSum += amt;
    }
    return openingTotal + (inSum - outSum);
  } catch { return 0; }
}

async function __saveFundDesired(desired) {
  // Calculate required opening and persist to stats/fund
  let inSum = 0, outSum = 0;
  try {
    const flows = Array.isArray(__fundCashflowsCache) && __fundCashflowsCache.length ? __fundCashflowsCache : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []);
    const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
    const isAsset = (id) => !id || assetIds.includes(id);
    for (const t of flows) {
      if (!t) continue;
      if (!isAsset(t.accountId)) continue;
      const amt = Math.abs(Number(t.amount || 0)) || 0;
      const typeStr = String(t.type || '').toLowerCase();
      if (typeStr === 'in' || typeStr === 'income' || typeStr === 'credit') inSum += amt; else if (typeStr === 'out' || typeStr === 'expense' || typeStr === 'debit') outSum += amt;
    }
  } catch {}
  const requiredOpening = Number(desired) - (inSum - outSum);
  await setDoc(doc(db, 'stats', 'fund'), cleanData({
    opening: Number(requiredOpening),
    value: Number(desired),
    inSum: Number(inSum),
    outSum: Number(outSum),
    asOf: new Date().toISOString(),
    setBy: auth?.currentUser?.uid || undefined,
    setByEmail: auth?.currentUser?.email || undefined,
    setReason: 'manual-reconciliation'
  }));
  __fundOpening = Number(requiredOpening);
  try { updateAccountsFundCard(); } catch {}
}

// Portal-based Set Fund dialog (independent of existing DOM)
function openSetFundDialog() {
  const existing = document.getElementById('setFundPortal');
  if (existing) { try { existing.remove(); } catch {} }
  const portal = document.createElement('div');
  portal.id = 'setFundPortal';
  portal.setAttribute('role','dialog');
  portal.setAttribute('aria-modal','true');
  portal.style.position = 'fixed';
  portal.style.inset = '0';
  portal.style.zIndex = '100000';
  portal.innerHTML = `
    <div data-backdrop style="position:fixed;inset:0;background:rgba(17,24,39,0.78);"></div>
    <div data-modal style="position:fixed;inset:0;display:flex;align-items:center;justify-content:center;pointer-events:none;">
      <div style="pointer-events:auto;background:#fff;border-radius:12px;max-width:520px;width:95%;padding:20px;box-shadow:0 10px 30px rgba(0,0,0,0.2);">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
          <div style="display:flex;align-items:center;gap:8px;">
            <i class="fas fa-sack-dollar" style="color:#4f46e5"></i>
            <h3 style="margin:0;font-size:18px;font-weight:700;">Set Current Fund</h3>
          </div>
          <button type="button" data-close class="btn btn-ghost" aria-label="Close"><i class="fas fa-times"></i></button>
        </div>
        <div style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:10px;margin-bottom:12px;">
          <div style="font-size:14px;color:#334155;">Computed Fund: <span data-computed style="font-weight:600">$0</span></div>
        </div>
        <div style="display:flex;flex-direction:column;gap:8px;">
          <label style="font-weight:600;color:#334155;font-size:14px;"><i class="fas fa-dollar-sign" style="color:#4f46e5;margin-right:6px;"></i> Set Current Fund To</label>
          <input data-input type="number" step="any" class="input" style="width:100%" />
          <p style="font-size:12px;color:#64748b;margin:4px 0 0;">This updates the Fund opening so that opening + net cashflows = this amount. No transactions are modified.</p>
        </div>
        <div style="display:flex;justify-content:flex-end;gap:8px;margin-top:16px;">
          <button type="button" data-cancel class="btn btn-secondary"><i class="fas fa-times"></i> Cancel</button>
          <button type="button" data-save class="btn btn-primary"><i class="fas fa-save"></i> <span>Save</span></button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(portal);
  try { document.body.classList.add('modal-open'); } catch {}
  const fmt = (n)=>`$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const computedEl = portal.querySelector('[data-computed]');
  if (computedEl) computedEl.textContent = fmt(__getCurrentFundTotal());
  const input = portal.querySelector('[data-input]');
  if (input) {
    const currentText = document.getElementById('accountsFundValue')?.textContent || '';
    const numeric = currentText ? Number(currentText.replace(/[^0-9.\-]/g,'')) : __getCurrentFundTotal();
    input.value = String(Number(numeric||0));
    setTimeout(()=>{ try { input.focus({ preventScroll:true }); input.select(); } catch {} }, 0);
  }
  const dispose = () => { try { document.body.classList.remove('modal-open'); } catch {} try { portal.remove(); } catch {} };
  portal.addEventListener('click', (e)=>{
    if (e.target && e.target.hasAttribute && (e.target.hasAttribute('data-backdrop') || e.target.getAttribute('data-backdrop')!==null)) {
      dispose();
    }
  });
  portal.querySelector('[data-close]')?.addEventListener('click', dispose);
  portal.querySelector('[data-cancel]')?.addEventListener('click', dispose);
  portal.querySelector('[data-save]')?.addEventListener('click', async () => {
    try {
      const val = Number((portal.querySelector('[data-input]')?.value)||0);
      if (!isFinite(val)) { showToast('Enter a valid amount', 'warning'); return; }
      await __saveFundDesired(val);
      showToast('Fund updated', 'success');
      dispose();
    } catch (e) {
      console.error('Set fund failed', e);
      showToast('Failed to update fund', 'error');
    }
  });
}

// Public open function used by UI
window.openEditFundModal = function() {
  try { console.debug('[Fund] openEditFundModal (portal)'); } catch {}
  openSetFundDialog();
}

window.closeEditFundModal = function() {
  const modal = document.getElementById('editFundModal');
  if (modal) modal.classList.remove('show');
  try { document.body.classList.remove('modal-open'); } catch {}
}

async function handleEditFundSubmit(e) {
  e.preventDefault();
  try {
    // Busy indicator
    const btn = document.getElementById('editFundSaveBtn');
    const btnText = btn?.querySelector('.btn-text');
    const btnBusy = btn?.querySelector('.btn-busy');
    if (btn) { btn.disabled = true; }
    if (btnText) btnText.classList.add('hidden');
    if (btnBusy) btnBusy.classList.remove('hidden');

    const desired = Number(document.getElementById('desiredFund')?.value || 0);
    if (!isFinite(desired)) { showToast('Enter a valid amount', 'warning'); return; }
    // Bank-style: set opening in stats/fund so that opening + net(cashflows) = desired
    // Compute current net flows to calculate required opening
    let inSum = 0, outSum = 0;
    try {
      // Prefer live cache to avoid extra reads
      const flows = Array.isArray(__fundCashflowsCache) && __fundCashflowsCache.length ? __fundCashflowsCache : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []);
      for (const t of flows) {
        if (!t) continue;
        const amt = Math.abs(Number(t.amount || 0)) || 0;
        const typeStr = String(t.type || '').toLowerCase();
        if (typeStr === 'in' || typeStr === 'income') inSum += amt; else if (typeStr === 'out' || typeStr === 'expense') outSum += amt;
      }
    } catch {}
    const requiredOpening = Number(desired) - (inSum - outSum);
    try {
      await setDoc(doc(db, 'stats', 'fund'), cleanData({
        opening: Number(requiredOpening),
        value: Number(desired),
        inSum: Number(inSum),
        outSum: Number(outSum),
        asOf: new Date().toISOString(),
        // Audit: track who set the opening and when. This is not a transaction; it only affects opening.
        setBy: auth?.currentUser?.uid || undefined,
        setByEmail: auth?.currentUser?.email || undefined,
        setReason: 'manual-reconciliation'
      }));
      __fundOpening = Number(requiredOpening);
      try { updateAccountsFundCard(); } catch {}
    } catch (err2) {
      console.warn('Failed to set fund opening', err2);
      showToast('Failed to update fund', 'error');
      return;
    }
    showToast('Fund updated', 'success');
    closeEditFundModal();
  } catch (err) {
    console.error('Edit fund failed', err);
    showToast('Failed to update fund', 'error');
  }
  finally {
    try {
      const btn = document.getElementById('editFundSaveBtn');
      const btnText = btn?.querySelector('.btn-text');
      const btnBusy = btn?.querySelector('.btn-busy');
      if (btn) { btn.disabled = false; }
      if (btnText) btnText.classList.remove('hidden');
      if (btnBusy) btnBusy.classList.add('hidden');
    } catch {}
  }
}

// Provide accounts to cashflow (kept in shadow from realtime listener)
function __getLocalAccounts() { return __accountsShadow.slice(); }

async function handleSignOut() {
  try {
    await signOut(auth);
    showToast('Signed out successfully', 'success');
  } catch (error) {
    console.error('Sign out error:', error);
    showToast('Error signing out', 'error');
  }
}

function handleSearch(e) {
  if (e.target.id === 'searchInput') {
    currentSearch = e.target.value.toLowerCase();
  }
  // Department filter removed
  renderEmployeeTable();
  renderTemporaryTable();
  renderPayrollTable();
}

// Handle form submission
async function handleFormSubmit(e) {
  e.preventDefault();

  if (!authed) {
    showToast('Please sign in to add employees', 'warning');
    return;
  }

  if (!validateForm()) return;

  const employee = {
    id: document.getElementById('employeeId').value || null,
    name: document.getElementById('name').value.trim(),
    email: document.getElementById('email').value.trim(),
    position: document.getElementById('position').value.trim(),
    department: document.getElementById('department').value,
    salary: parseFloat(document.getElementById('salary').value),
    joinDate: document.getElementById('joinDate').value,
    qid: (document.getElementById('qid')?.value || '').trim(),
    qidExpiry: (document.getElementById('qidExpiry')?.value || '').trim() || undefined,
    passportExpiry: (document.getElementById('passportExpiry')?.value || '').trim() || undefined,
    phone: (document.getElementById('phone')?.value || '').trim(),
    bankName: (document.getElementById('bankName')?.value || '').trim(),
    bankAccountNumber: (document.getElementById('bankAccountNumber')?.value || '').trim(),
    bankIban: ((document.getElementById('bankIban')?.value || '').trim() || '').toUpperCase().replace(/\s+/g, '')
  };
  const isNew = !employee.id;

  // Upload PDFs if selected
  const which = document.getElementById('employeeForm')?.getAttribute('data-which') || 'employees';
  const files = {
    qidPdf: document.getElementById('qidPdf')?.files?.[0] || null,
    passportPdf: document.getElementById('passportPdf')?.files?.[0] || null,
    profileImage: document.getElementById('profileImage')?.files?.[0] || null,
  };

  const basePath = which === 'temporary' ? 'temporaryEmployees' : 'employees';

  // Close the modal immediately for both Add and Edit
  closeEmployeeModal();
  showToast(isNew ? 'Saving new employee…' : 'Updating employee…', 'info');

  // If creating new document without ID, first create the doc to get ID, then upload
  if (!employee.id) {
    try {
      const { id, ...newData } = employee;
      const docRef = await addDoc(collection(db, basePath), cleanData(newData));
      employee.id = docRef.id;
    } catch (error) {
      console.error('Failed to create employee record:', error);
      showToast('Failed to create employee record. Please try again.', 'error');
      return;
    }
  }

  // Helper to upload a file to a specific storage path and return its download URL
  const uploadIfNeeded = async (file, storagePath, type) => {
    if (!file) return null;
    if (type === 'pdf') {
      if (file.type !== 'application/pdf') {
        showToast('Please upload PDF files only', 'warning');
        return null;
      }
    } else if (type === 'image') {
      if (!/^image\/(png|jpeg|webp)$/.test(file.type)) {
        showToast('Please upload a PNG, JPEG, or WEBP image', 'warning');
        return null;
      }
    }
    try {
      const destRef = storageRef(storage, storagePath);
      const snapshot = await uploadBytes(destRef, file, { contentType: file.type });
      return await getDownloadURL(snapshot.ref);
    } catch (error) {
      console.error('Upload error:', error);
      if (error && error.code === 'storage/unauthorized') {
        showToast('Please sign in to upload files', 'error');
      } else {
        showToast('Failed to upload file: ' + (error?.message || 'Unknown error'), 'error');
      }
      return null;
    }
  };

  const qidUrl = await uploadIfNeeded(files.qidPdf, `${basePath}/${employee.id}/qidPdf.pdf`, 'pdf');
  const passportUrl = await uploadIfNeeded(files.passportPdf, `${basePath}/${employee.id}/passportPdf.pdf`, 'pdf');
  // Profile image: preserve extension based on mime type
  let profileUrl = null;
  if (files.profileImage) {
    const ext = files.profileImage.type === 'image/png' ? 'png' : files.profileImage.type === 'image/webp' ? 'webp' : 'jpg';
    profileUrl = await uploadIfNeeded(files.profileImage, `${basePath}/${employee.id}/profile.${ext}`, 'image');
  }
  if (qidUrl) employee.qidPdfUrl = qidUrl;
  if (passportUrl) employee.passportPdfUrl = passportUrl;
  if (profileUrl) employee.profileImageUrl = profileUrl;

  try {
    if (which === 'temporary') {
      await saveTemporaryEmployee(employee, isNew);
    } else {
      await saveEmployee(employee, isNew);
    }
  } catch (err) {
    console.error('Save failed:', err);
    showToast('Failed to save employee. Please try again.', 'error');
  }
}

// Delete employee
window.openDeleteModal = function(id, which) {
  deleteEmployeeId = { id, which: which || 'employees' };
  const modal = document.getElementById('deleteModal');
  if (modal) modal.classList.add('show');
}

// Confirm delete
async function handleConfirmDelete() {
  if (!deleteEmployeeId) return;
  const { id, which } = typeof deleteEmployeeId === 'object' ? deleteEmployeeId : { id: deleteEmployeeId, which: 'employees' };
  if (which === 'temporary') {
    await deleteTemporaryFromDB(id);
  } else {
    await deleteEmployeeFromDB(id);
  }
  closeModal();
  deleteEmployeeId = null;
}

// Close modal
window.closeModal = function() {
  const modal = document.getElementById('deleteModal');
  if (modal) modal.classList.remove('show');
  deleteEmployeeId = null;
}

// Employee form modal controls
window.openEmployeeModal = function(which = 'employees', mode = 'add') {
  const modal = document.getElementById('employeeModal');
  if (modal) modal.classList.add('show');
  // Reset form to add mode
  if (mode !== 'edit') {
    clearForm();
  }
  const form = document.getElementById('employeeForm');
  if (form) form.setAttribute('data-which', which);
  const titleEl = document.getElementById('formTitle');
  if (titleEl) {
    if (mode === 'edit') {
      titleEl.textContent = which === 'temporary' ? 'Edit Temporary Employee' : 'Edit Employee';
    } else {
      titleEl.textContent = which === 'temporary' ? 'Add Temporary Employee' : 'Add New Employee';
    }
  }
  const primaryBtn = document.querySelector('button[form="employeeForm"]');
  if (primaryBtn) {
    if (mode === 'edit') {
      primaryBtn.innerHTML = '<i class="fas fa-save"></i> Update Employee';
    } else {
      primaryBtn.innerHTML = '<i class="fas fa-save"></i> Save Employee';
    }
  }
}

// Payroll sorting (delegates to module)
window.sortPayroll = function(column) {
  payrollSort(column, {
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
    getSearchQuery: () => currentSearch,
  });
}

// Render payroll table (combines employees and temporary)
function renderPayrollTable() {
  payrollRenderTable({
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
    getSearchQuery: () => currentSearch,
  });
}

// Ensure monthly balances snapshot for current month
function ensureCurrentMonthBalances() {
  const now = new Date();
  const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  if (lastBalanceEnsureMonth === ym) return;
  // Only proceed if we have some employees loaded
  const all = [...employees, ...temporaryEmployees];
  if (!all.length) return;
  lastBalanceEnsureMonth = ym;
  // Fire-and-forget upserts; they compute basic/advances/payments for this month
  for (const emp of all) {
    upsertMonthlyBalanceFor(emp.id, now).catch(() => {});
  }
}
// Render a printable payroll frame
function renderPayrollFrame(opts) {
  // If options are provided, pass-through to module with explicit month
  if (opts && typeof opts === 'object') {
    return payrollRenderFrame(opts);
  }
  const monthEl = document.getElementById('payrollMonth');
  payrollRenderFrame({
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
    month: monthEl ? monthEl.value : null,
  });
}

// Open Payroll Detail modal
window.viewPayroll = function (id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const emp = list.find(e => e.id === id);
  if (!emp) return;

  const byId = (x) => document.getElementById(x);
  const fmtCurrency = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

  // Populate fields
  const typeLabel = which === 'temporary' ? 'Temporary' : 'Permanent';
  const maskAcc = (acc) => maskAccount(acc);
  const setText = (id, text) => { const el = byId(id); if (el) el.textContent = text; };
  setText('payrollViewName', emp.name || '—');
  setText('payrollViewType', typeLabel);
  setText('payrollViewDepartment', emp.department || '—');
  setText('payrollViewPosition', emp.position || '—');
  setText('payrollViewQid', emp.qid || '—');
  setText('payrollViewSalary', fmtCurrency(emp.salary));
  setText('payrollViewJoinDate', formatDate(emp.joinDate));
  setText('payrollViewBankName', emp.bankName || '—');
  setText('payrollViewAccount', maskAcc(emp.bankAccountNumber));
  setText('payrollViewIban', emp.bankIban || '—');

  // Save for payslip printing
  currentPayrollView = { ...emp, _which: which === 'temporary' ? 'temporary' : 'employees' };

  const modal = byId('payrollModal');
  if (modal) {
    modal.classList.add('show');
    try { modal.querySelector('.modal-content')?.focus(); } catch {}
  }

  // Default to Overview tab on open
  setPayrollViewActiveTab('overview');

  // Load payslips for this employee into the Payroll Details modal
  loadPayslipsForPayrollModal(emp.id).catch(() => {});
};

window.closePayrollModal = function () {
  const modal = document.getElementById('payrollModal');
  if (modal) modal.classList.remove('show');
  currentPayrollView = null;
  // Clear payslips list on close
  const l = document.getElementById('payrollPayslipsList');
  const e = document.getElementById('payrollPayslipsEmpty');
  const g = document.getElementById('payrollPayslipsLoading');
  if (l) l.innerHTML = '';
  if (e) e.style.display = '';
  if (g) g.style.display = 'none';
};

// Helper to switch tabs in Payroll Details modal
function setPayrollViewActiveTab(which) {
  const btnOv = document.getElementById('payrollViewTabOverviewBtn');
  const btnPs = document.getElementById('payrollViewTabPayslipsBtn');
  const tabOv = document.getElementById('payrollViewTabOverview');
  const tabPs = document.getElementById('payrollViewTabPayslips');
  if (!btnOv || !btnPs || !tabOv || !tabPs) return;

  const activate = (btn) => {
    btn.classList.add('font-semibold', 'text-indigo-600');
    btn.classList.add('border-b-2', 'border-indigo-600');
    btn.classList.remove('text-gray-600');
  };
  const deactivate = (btn) => {
    btn.classList.remove('font-semibold', 'text-indigo-600');
    btn.classList.remove('border-b-2', 'border-indigo-600');
    btn.classList.add('text-gray-600');
  };

  if (which === 'payslips') {
    tabOv.style.display = 'none';
    tabPs.style.display = '';
    deactivate(btnOv); activate(btnPs);
    // Ensure list is populated/refreshed when switching
    if (currentPayrollView?.id) {
      loadPayslipsForPayrollModal(currentPayrollView.id).catch(() => {});
    }
  } else {
    tabPs.style.display = 'none';
    tabOv.style.display = '';
    deactivate(btnPs); activate(btnOv);
  }
}

// Wire tab buttons for Payroll Details modal
document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'payrollViewTabOverviewBtn') {
    setPayrollViewActiveTab('overview');
  } else if (e.target && e.target.id === 'payrollViewTabPayslipsBtn') {
    setPayrollViewActiveTab('payslips');
  }
});
async function loadPayslipsForPayrollModal(employeeId) {
  const listEl = document.getElementById('payrollPayslipsList');
  // New split sections
  const advListEl = document.getElementById('payrollPayslipsAdvList');
  const advEmptyEl = document.getElementById('payrollPayslipsAdvEmpty');
  const advLoadingEl = document.getElementById('payrollPayslipsAdvLoading');
  const salListEl = document.getElementById('payrollPayslipsSalList');
  const salEmptyEl = document.getElementById('payrollPayslipsSalEmpty');
  const salLoadingEl = document.getElementById('payrollPayslipsSalLoading');
  // Backward compatibility: if new elements are missing, fall back to old
  const emptyEl = document.getElementById('payrollPayslipsEmpty');
  const loadingEl = document.getElementById('payrollPayslipsLoading');
  if (!employeeId) return;
  try {
    // Initialize new sections
    if (advLoadingEl) advLoadingEl.style.display = '';
    if (salLoadingEl) salLoadingEl.style.display = '';
    if (advEmptyEl) advEmptyEl.style.display = 'none';
    if (salEmptyEl) salEmptyEl.style.display = 'none';
    if (advListEl) advListEl.innerHTML = '';
    if (salListEl) salListEl.innerHTML = '';
    // Clear legacy area if present
    if (loadingEl) loadingEl.style.display = '';
    if (emptyEl) emptyEl.style.display = 'none';
    if (listEl) listEl.innerHTML = '';
    // Avoid requiring a composite index by removing orderBy and sorting client-side
    const q2 = query(collection(db, 'payslips'), where('employeeId', '==', employeeId));
    const snap2 = await getDocs(q2);
    if (snap2.empty) {
      if (advLoadingEl) advLoadingEl.style.display = 'none';
      if (salLoadingEl) salLoadingEl.style.display = 'none';
      if (loadingEl) loadingEl.style.display = 'none';
      if (advEmptyEl) advEmptyEl.style.display = '';
      if (salEmptyEl) salEmptyEl.style.display = '';
      if (emptyEl) emptyEl.style.display = '';
      return;
    }
    const docs = snap2.docs.map(docu => {
      const d = docu.data();
      const dt = d.createdAt?.toDate ? d.createdAt.toDate() : (d.createdAt && d.createdAt.seconds ? new Date(d.createdAt.seconds * 1000) : null);
      return { id: docu.id, data: d, createdAt: dt };
    }).sort((a, b) => {
      const ta = a.createdAt ? a.createdAt.getTime() : 0;
      const tb = b.createdAt ? b.createdAt.getTime() : 0;
      return tb - ta; // desc
    });
    const renderItem = ({ id, data: d, createdAt: dt }) => {
      const created = dt ? dt.toLocaleString() : '';
      const ym = d.period || '';
      const monthTitle = ym ? new Date(Number(ym.split('-')[0]), Number(ym.split('-')[1]) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : '';
      const amount = Number(d.net || d.advance || 0).toLocaleString(undefined, { maximumFractionDigits: 2 });
      return `
        <div class="border border-gray-200 rounded-md p-3 flex items-center justify-between">
          <div>
            <div class="font-semibold text-gray-900">${monthTitle || 'Payslip'}</div>
            <div class="text-xs text-gray-500">${created}</div>
            <div class="text-sm text-gray-700">${d.isAdvance ? 'Advance' : 'Salary'}: $${amount}</div>
          </div>
          <div class="flex items-center gap-2">
            <button class="btn btn-ghost btn-sm" data-preview data-id="${id}"><i class="fas fa-eye"></i> View</button>
            <button class="btn btn-secondary btn-sm" data-reprint data-id="${id}"><i class="fas fa-print"></i> Reprint</button>
            <button class="btn btn-danger btn-sm" data-delete data-id="${id}"><i class="fas fa-trash"></i> Delete</button>
          </div>
        </div>
      `;
    };
    // Split docs
    const advDocs = docs.filter(x => Boolean(x.data.isAdvance));
    const salDocs = docs.filter(x => !Boolean(x.data.isAdvance));
    if (advListEl) advListEl.innerHTML = advDocs.map(renderItem).join('');
    if (salListEl) salListEl.innerHTML = salDocs.map(renderItem).join('');
    // Legacy fill (if new elements not found)
    if ((!advListEl || !salListEl) && listEl) listEl.innerHTML = docs.map(renderItem).join('');
    // Empty states
    if (advEmptyEl) advEmptyEl.style.display = advDocs.length ? 'none' : '';
    if (salEmptyEl) salEmptyEl.style.display = salDocs.length ? 'none' : '';
  } catch (e) {
    console.warn('Failed to load payslips (payroll modal)', e);
    if (advEmptyEl) { advEmptyEl.textContent = 'Failed to load payslips'; advEmptyEl.style.display = ''; }
    if (salEmptyEl) { salEmptyEl.textContent = 'Failed to load payslips'; salEmptyEl.style.display = ''; }
    if (emptyEl) { emptyEl.textContent = 'Failed to load payslips'; emptyEl.style.display = ''; }
  } finally {
    if (advLoadingEl) advLoadingEl.style.display = 'none';
    if (salLoadingEl) salLoadingEl.style.display = 'none';
    if (loadingEl) loadingEl.style.display = 'none';
  }
}

// Payslip Form modal controls
window.openPayslipForm = function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const emp = list.find(e => e.id === id);
  if (!emp) { showToast('Employee not found for payslip', 'error'); return; }
  if (emp.terminated) { showToast('This employee is terminated. Payslips are disabled.', 'warning'); return; }

  // Prefill form fields
  const nameEl = document.getElementById('psEmployeeName');
  const typeEl = document.getElementById('psEmployeeType');
  const periodEl = document.getElementById('psPeriod');
  const basicEl = document.getElementById('psBasic');
  const advAmtEl = document.getElementById('psAdvanceAmount');
  // removed: psIsAdvance control
  const notesEl = document.getElementById('psNotes');
  if (nameEl) nameEl.value = emp.name || '';
  if (typeEl) typeEl.value = which === 'temporary' ? 'Temporary' : 'Permanent';
  if (basicEl) basicEl.value = Number(emp.salary || 0);
  if (advAmtEl) advAmtEl.value = Number(advAmtEl?.value || 0);
  // removed default for psIsAdvance
  if (notesEl) notesEl.value = notesEl.value || '';
  if (periodEl && !periodEl.value) {
    const now = new Date();
    periodEl.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  }

  // Store context for printing
  currentPayrollView = { ...emp, _which: which === 'temporary' ? 'temporary' : 'employees' };

  // Show modal
  const modal = document.getElementById('payslipModal');
  if (modal) {
    modal.classList.add('show');
    try { modal.querySelector('.modal-content')?.focus(); } catch {}
  }

  // Update net pay preview
  updatePayslipNet();
};

window.closePayslipModal = function() {
  const modal = document.getElementById('payslipModal');
  if (modal) modal.classList.remove('show');
};

// Payment Slip modal controls
window.openPaymentForm = function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const emp = list.find(e => e.id === id);
  if (!emp) { showToast('Employee not found for salary payment', 'error'); return; }
  if (emp.terminated) { showToast('This employee is terminated. Payments are disabled.', 'warning'); return; }

  const nameEl = document.getElementById('payEmployeeName');
  const typeEl = document.getElementById('payEmployeeType');
  const dateEl = document.getElementById('payDate');
  const amountEl = document.getElementById('payAmount');
  const methodEl = document.getElementById('payMethod');
  const refEl = document.getElementById('payRef');
  const advEl = document.getElementById('payIsAdvance');
  const deductEl = document.getElementById('payDeductFrom');
  const notesEl = document.getElementById('payNotes');
  const overtimeEl = document.getElementById('payOvertime');
  const overtimeHoursEl = document.getElementById('payOvertimeHours');
  if (nameEl) nameEl.value = emp.name || '';
  if (typeEl) typeEl.value = which === 'temporary' ? 'Temporary' : 'Permanent';
  if (dateEl) dateEl.valueAsDate = new Date();
  if (amountEl) amountEl.value = '';
  if (methodEl) methodEl.value = 'cash';
  if (refEl) refEl.value = '';
  if (advEl) advEl.value = 'no';
  if (deductEl) deductEl.value = 'none';
  if (notesEl) notesEl.value = '';
  if (overtimeEl) overtimeEl.value = '';
  if (overtimeHoursEl) overtimeHoursEl.value = '';

  currentPayrollView = { ...emp, _which: which === 'temporary' ? 'temporary' : 'employees' };
  const modal = document.getElementById('paymentModal');
  if (modal) {
    modal.classList.add('show');
    try { modal.querySelector('.modal-content')?.focus(); } catch {}
  }
};

window.closePaymentModal = function() {
  const modal = document.getElementById('paymentModal');
  if (modal) modal.classList.remove('show');
};

// Payslip net calculator
function getPayslipNumbers() {
  const basic = Number(document.getElementById('psBasic')?.value || 0);
  const advance = Number(document.getElementById('psAdvanceAmount')?.value || 0);
  // For advance slip, the paid amount equals the advance value; net here is simply the advance amount shown
  const net = advance;
  return { basic, advance, net };
}

function updatePayslipNet() {
  const { net } = getPayslipNumbers();
  const el = document.getElementById('psNet');
  if (el) el.textContent = `$${Number(net).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

// Wire payslip input change handlers and print button
document.addEventListener('input', (e) => {
  const ids = ['psBasic','psAdvanceAmount'];
  if (e.target && ids.includes(e.target.id)) {
    updatePayslipNet();
  }
});

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'payslipPrintBtn') {
    e.preventDefault();
    const ok = confirm('Confirm generating this payslip and updating the current salary balance?');
    if (!ok) return;
    savePayslipThenPrint().then(() => {
      try { window.__payrollBalancesInvalidate?.(); } catch {}
    }).catch(err => {
      console.error(err);
      showToast('Failed to save payslip; printing anyway', 'warning');
      try { renderAndPrintPayslip(); } catch (err2) { console.error(err2); showToast('Failed to print payslip', 'error'); }
    });
  } else if (e.target && e.target.id === 'paymentPrintBtn') {
    e.preventDefault();
    savePaymentThenPrint().catch(err => {
      console.error(err);
      showToast('Failed to save salary payment; printing anyway', 'warning');
      try { renderAndPrintPaymentSlip(); } catch (err2) { console.error(err2); showToast('Failed to print salary slip', 'error'); }
    });
  }
});

async function savePaymentThenPrint() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const payDate = document.getElementById('payDate')?.value || '';
  const amount = Number(document.getElementById('payAmount')?.value || 0);
  const method = document.getElementById('payMethod')?.value || 'cash';
  const ref = document.getElementById('payRef')?.value || '';
  // Payment modal no longer supports advances here; treat as non-advance salary payment
  const isAdvance = false;
  const deductFrom = document.getElementById('payDeductFrom')?.value || 'none';
  const notes = document.getElementById('payNotes')?.value || '';
  const overtime = Number(document.getElementById('payOvertime')?.value || 0);
  const overtimeHours = Number(document.getElementById('payOvertimeHours')?.value || 0);
  const totalEntered = Number(amount) + Number(overtime || 0);
  if (totalEntered <= 0) { showToast('Enter a valid payment amount or overtime', 'warning'); return; }
  if (overtime < 0) { showToast('Overtime amount cannot be negative', 'warning'); return; }
  if (overtimeHours < 0) { showToast('Overtime hours cannot be negative', 'warning'); return; }
  if (!payDate) { showToast('Select a payment date', 'warning'); return; }

  const record = cleanData({
    employeeId: emp.id,
    employeeType: emp._which === 'temporary' ? 'Temporary' : 'Permanent',
    employeeName: emp.name || '',
    department: emp.department || '',
    position: emp.position || '',
    qid: emp.qid || '',
    amount,
  overtime,
  overtimeHours,
    method,
    reference: ref,
  isAdvance, // always false for salary payments
    deductFrom,
    notes,
    date: payDate,
    createdAt: serverTimestamp(),
    createdBy: auth?.currentUser?.uid || undefined,
    createdByEmail: auth?.currentUser?.email || undefined,
  });

  try {
    await addDoc(collection(db, 'payments'), record);
    showToast('Salary payment saved', 'success');
    try { window.__payrollBalancesInvalidate?.(); } catch {}
    // Update monthly balance snapshot for payment month via carryover function
    try {
      const paymentMonth = String((record.date||'')).slice(0,7);
      if (paymentMonth && /^\d{4}-\d{2}$/.test(paymentMonth)) {
        await updateEmployeeBalance(emp.id, paymentMonth, { ...emp, _type: emp._which === 'temporary' ? 'Temporary' : 'Permanent' });
      }
      // Notify any listeners to recompute UI balances
      try { document.dispatchEvent(new Event('payroll:recompute-balances')); } catch {}
    } catch (e) { console.warn('Balance update failed (payment)', e); }
    // Also update the payroll table immediately if it's on screen
    try {
      const payrollSection = document.getElementById('payrollSection');
      if (payrollSection && payrollSection.style.display !== 'none') {
        renderPayrollTable();
      }
    } catch {}
    // Persist the monthly balance snapshot for this employee and month
  try { await upsertMonthlyBalanceFor(emp.id, new Date(payDate)); } catch (e) { console.warn('Balance upsert failed (payment)', e); }
    // Also create a non-advance payslip entry for this payment so it appears under Salary Payslips
    try {
      const dObj = new Date(payDate);
      const ym = `${dObj.getFullYear()}-${String(dObj.getMonth() + 1).padStart(2, '0')}`;
      // Determine basic for the month (prefer latest payslip basic if exists)
      let basicForMonth = Number(emp.salary || 0);
      try {
        const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
        const all = psSnap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
        const thisMonth = all.filter(p => (p.period || '') === ym);
        if (thisMonth.length) {
          const latest = thisMonth.sort((a,b) => (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0))[0];
          if (latest && Number(latest.basic)) basicForMonth = Number(latest.basic);
        }
      } catch {}
      const totalPaid = Number(amount) + Number(overtime || 0);
      const notesParts = [
        `Payment: ${method}`,
        ref ? `Ref: ${ref}` : null,
        overtime > 0 ? `OT: $${Number(overtime).toLocaleString(undefined,{maximumFractionDigits:2})}${overtimeHours>0?` (${overtimeHours}h)`:''}` : null,
      ].filter(Boolean);
      const slip = cleanData({
        employeeId: emp.id,
        employeeType: emp._which === 'temporary' ? 'Temporary' : 'Permanent',
        employeeName: emp.name || '',
        department: emp.department || '',
        position: emp.position || '',
        qid: emp.qid || '',
        period: ym,
        basic: Number(basicForMonth),
        advance: 0,
        net: Number(totalPaid),
        notes: notesParts.join(' • '),
        isAdvance: false,
        createdAt: serverTimestamp(),
        createdBy: auth?.currentUser?.uid || undefined,
        createdByEmail: auth?.currentUser?.email || undefined,
      });
      await addDoc(collection(db, 'payslips'), slip);
      // If Payroll Details is open for this employee, refresh the payslips tab
      try {
        const modal = document.getElementById('payrollModal');
        if (modal && modal.classList.contains('show') && currentPayrollView?.id === emp.id) {
          await loadPayslipsForPayrollModal(emp.id);
        }
      } catch {}
    } catch (e) {
      console.warn('Failed to create payslip from salary payment (non-fatal)', e);
    }
  } catch (e) {
    console.error('Save payment failed', e);
    showToast('Failed to save salary payment', 'error');
    throw e;
  }
  // Post a cashflow OUT entry to reduce fund (amount + overtime)
  try {
    const dObj = new Date(document.getElementById('payDate')?.value || new Date());
    const totalPaid = Number(document.getElementById('payAmount')?.value || 0) + Number(document.getElementById('payOvertime')?.value || 0);
    if (totalPaid > 0) {
      const accId = await ensureAssetAccount('cash', 'Cash');
      if (accId) {
        await addDoc(collection(db, 'cashflows'), cleanData({
          date: `${dObj.getFullYear()}-${String(dObj.getMonth()+1).padStart(2,'0')}-${String(dObj.getDate()).padStart(2,'0')}`,
          type: 'out',
          accountId: accId,
          amount: Math.abs(Number(totalPaid) || 0),
          category: 'Salary',
          notes: `Salary payment for ${currentPayrollView?.name || ''}`,
          createdAt: new Date().toISOString(),
        }));
        try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
      }
    }
  } catch (cfErr) {
    console.warn('Failed to post cashflow for salary payment (non-fatal)', cfErr);
  }
  await renderAndPrintPaymentSlip();
}

async function savePayslipThenPrint() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const period = document.getElementById('psPeriod')?.value || '';
  const notes = document.getElementById('psNotes')?.value || '';
  const { basic, advance, net } = getPayslipNumbers();
  const isAdvance = Number(advance) > 0;
  if (!period) { showToast('Select a pay period', 'warning'); return; }

  const record = cleanData({
    employeeId: emp.id,
    employeeType: emp._which === 'temporary' ? 'Temporary' : 'Permanent',
    employeeName: emp.name || '',
    department: emp.department || '',
    position: emp.position || '',
    qid: emp.qid || '',
    period,
    basic,
    advance,
    net,
    notes,
    isAdvance,
    createdAt: serverTimestamp(),
    createdBy: auth?.currentUser?.uid || null,
    createdByEmail: auth?.currentUser?.email || null,
  });
  try {
    await addDoc(collection(db, 'payslips'), record);
    showToast('Payslip saved', 'success');
    // CRITICAL: Update balance for this period using carryover-aware function
    try {
      await updateEmployeeBalance(emp.id, period, { ...emp, _type: emp._which === 'temporary' ? 'Temporary' : 'Permanent' });
      try { document.dispatchEvent(new Event('payroll:recompute-balances')); } catch {}
    } catch (e) { console.warn('Balance update failed (payslip)', e); }
    // If Payroll Details modal is open for this employee, refresh the payslips tab
    try {
      const modal = document.getElementById('payrollModal');
      if (modal && modal.classList.contains('show') && currentPayrollView?.id === emp.id) {
        await loadPayslipsForPayrollModal(emp.id);
      }
    } catch {}
    // Persist the monthly balance snapshot for this employee and selected period
    try {
      const [y,m] = (record.period || '').split('-');
      const when = (y && m) ? new Date(Number(y), Number(m)-1, 1) : new Date();
      await upsertMonthlyBalanceFor(emp.id, when);
      try { window.__payrollBalancesInvalidate?.(); } catch {}
    } catch (e) { console.warn('Balance upsert failed (payslip)', e); }
  } catch (e) {
    console.error('Save payslip failed', e);
    showToast('Failed to save payslip', 'error');
    throw e;
  }
  // If this was an advance, post a cashflow OUT entry
  try {
    if (isAdvance && Number(advance) > 0) {
      const [yy, mm] = (period || '').split('-');
      const dt = (yy && mm) ? new Date(Number(yy), Number(mm)-1, 1) : new Date();
      const accId = await ensureAssetAccount('cash', 'Cash');
      if (accId) {
        await addDoc(collection(db, 'cashflows'), cleanData({
          date: `${dt.getFullYear()}-${String(dt.getMonth()+1).padStart(2,'0')}-${String(dt.getDate()).padStart(2,'0')}`,
          type: 'out',
          accountId: accId,
          amount: Math.abs(Number(advance) || 0),
          category: 'Advance',
          notes: `Salary advance for ${emp.name || ''}`,
          createdAt: new Date().toISOString(),
        }));
        try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
      }
    }
  } catch (cfErr) {
    console.warn('Failed to post cashflow for advance (non-fatal)', cfErr);
  }
  renderAndPrintPayslip();
}

function renderAndPrintPayslip() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const period = document.getElementById('psPeriod')?.value || '';
  const notes = document.getElementById('psNotes')?.value || '';
  const { basic, advance, net } = getPayslipNumbers();
  const area = document.getElementById('payslipPrintArea');
  if (!area) { showToast('Print area missing in DOM', 'error'); return; }

  // Use shared HTML builder so preview and print match
  const html = renderPayslipHtml({ emp, period, notes, basic, advance, net });
  const prevDisplay = area.style.display;
  area.innerHTML = html;
  // Ensure the area is visible for printing
  area.style.display = 'block';
  // Trigger print
  const afterPrint = () => {
    window.removeEventListener('afterprint', afterPrint);
    // Restore previous display (hide in screen context per CSS)
    area.style.display = prevDisplay || '';
  };
  window.addEventListener('afterprint', afterPrint);
  setTimeout(() => window.print(), 50);
}

// Build payslip HTML so it can be reused by print and preview
function renderPayslipHtml({ emp, period, notes, basic, advance, net }) {
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const monthTitle = period ? new Date(Number(period.split('-')[0]), Number(period.split('-')[1]) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Period';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  const balanceAfterAdvance = Math.max(0, Number(basic || 0) - Number(advance || 0));
  return `
    <section class="payslip">
      <header class="payslip-header">
        <div>
          <div class="title">Payslip</div>
          <div class="subtitle">${monthTitle}</div>
        </div>
        <div class="brand">CRM</div>
      </header>
      <div class="payslip-grid">
        <div>
          <div class="label">Employee</div>
          <div class="value">${emp.name || ''}</div>
        </div>
        <div>
          <div class="label">Type</div>
          <div class="value">${typeLabel}</div>
        </div>
        <div>
          <div class="label">Company</div>
          <div class="value">${emp.department || '-'}</div>
        </div>
        <div>
          <div class="label">Position</div>
          <div class="value">${emp.position || '-'}</div>
        </div>
        <div>
          <div class="label">Join Date</div>
          <div class="value">${formatDate(emp.joinDate)}</div>
        </div>
        <div>
          <div class="label">QID</div>
          <div class="value">${emp.qid || '-'}</div>
        </div>
      </div>
      <table class="payslip-table">
        <thead>
          <tr><th>Detail</th><th class="text-right">Amount</th></tr>
        </thead>
        <tbody>
          <tr><td>Basic Salary (Monthly)</td><td class="text-right">${fmt(basic)}</td></tr>
          <tr><td>Advance Amount</td><td class="text-right">${fmt(advance)}</td></tr>
          <tr class="total"><td>Balance Salary (after advance)</td><td class="text-right">${fmt(balanceAfterAdvance)}</td></tr>
          <tr><td>Total Paid</td><td class="text-right">${fmt(net)}</td></tr>
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <footer class="payslip-footer">Generated by CRM • ${new Date().toLocaleString()}</footer>
    </section>
  `;
}

async function renderAndPrintPaymentSlip() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const payDate = document.getElementById('payDate')?.value || '';
  const amount = Number(document.getElementById('payAmount')?.value || 0);
  const method = document.getElementById('payMethod')?.value || 'cash';
  const ref = document.getElementById('payRef')?.value || '';
  // Advances are not handled in this modal; treat as salary payment
  const isAdvance = false;
  const deductFrom = document.getElementById('payDeductFrom')?.value || 'none';
  const notes = document.getElementById('payNotes')?.value || '';
  const overtime = Number(document.getElementById('payOvertime')?.value || 0);
  const overtimeHours = Number(document.getElementById('payOvertimeHours')?.value || 0);
  const totalEntered = Number(amount) + Number(overtime || 0);
  if (totalEntered <= 0) { showToast('Enter a valid payment amount or overtime', 'warning'); return; }
  if (overtime < 0) { showToast('Overtime amount cannot be negative', 'warning'); return; }
  if (overtimeHours < 0) { showToast('Overtime hours cannot be negative', 'warning'); return; }
  if (!payDate) { showToast('Select a payment date', 'warning'); return; }

  const area = document.getElementById('payslipPrintArea');
  if (!area) { showToast('Print area missing in DOM', 'error'); return; }

  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const dateStr = payDate ? new Date(payDate).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  // Deduction plan display removed per request

  // Compute monthly advances and balance after advances for the payment month
  const d = new Date(payDate);
  const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  let advancesThisPeriod = 0;
  let basicForMonth = Number(emp.salary || 0);
  try {
    const snap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', emp.id)));
    const all = snap.docs.map(doc => ({ id: doc.id, ...doc.data() }));
    const thisMonth = all.filter(p => (p.period || '') === ym);
    if (thisMonth.length) {
      const latest = thisMonth.sort((a,b) => {
        const ta = a.createdAt?.seconds || 0; const tb = b.createdAt?.seconds || 0; return tb - ta;
      })[0];
      if (latest && Number(latest.basic)) basicForMonth = Number(latest.basic);
      advancesThisPeriod = thisMonth.reduce((sum, p) => sum + Number(p.advance || 0), 0);
    }
  } catch (e) {
    console.warn('Failed to compute monthly advances', e);
  }
  const balanceAfterAdvances = Math.max(0, Number(basicForMonth) - Number(advancesThisPeriod));
  const totalPaid = Number(amount) + Number(overtime || 0);

  area.innerHTML = `
    <section class="payslip">
      <header class="payslip-header">
        <div>
          <div class="title">Salary Slip</div>
          <div class="subtitle">${dateStr}</div>
        </div>
        <div class="brand">CRM</div>
      </header>
      <div class="payslip-grid">
        <div>
          <div class="label">Employee</div>
          <div class="value">${emp.name || ''}</div>
        </div>
        <div>
          <div class="label">Type</div>
          <div class="value">${typeLabel}</div>
        </div>
        <div>
          <div class="label">Company</div>
          <div class="value">${emp.department || '-'}</div>
        </div>
        <div>
          <div class="label">Position</div>
          <div class="value">${emp.position || '-'}</div>
        </div>
        <div>
          <div class="label">Payment Method</div>
          <div class="value">${method}</div>
        </div>
        <div>
          <div class="label">Reference</div>
          <div class="value">${ref || '-'}</div>
        </div>
      </div>
      <table class="payslip-table">
        <thead>
          <tr><th>Detail</th><th class="text-right">Amount</th></tr>
        </thead>
        <tbody>
          <tr><td>Basic Salary (Monthly)</td><td class="text-right">${fmt(basicForMonth)}</td></tr>
          <tr><td>Advances This Period (${ym})</td><td class="text-right">${fmt(advancesThisPeriod)}</td></tr>
          <tr class="total"><td>Balance Salary (after advances)</td><td class="text-right">${fmt(balanceAfterAdvances)}</td></tr>
          <tr><td>${isAdvance ? 'Advance Paid' : 'Salary Paid'}</td><td class="text-right">${fmt(amount)}</td></tr>
          ${overtime > 0 ? `<tr><td>Overtime</td><td class="text-right">${fmt(overtime)}</td></tr>` : ''}
          ${overtimeHours > 0 ? `<tr><td>Overtime Hours</td><td class="text-right">${Number(overtimeHours).toLocaleString()}</td></tr>` : ''}
          ${overtime > 0 ? `<tr class="total"><td>Total Paid</td><td class="text-right">${fmt(totalPaid)}</td></tr>` : ''}
          
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <footer class="payslip-footer">Generated by CRM • ${new Date().toLocaleString()}</footer>
    </section>
  `;
  const prevDisplay = area.style.display;
  area.style.display = 'block';
  const afterPrint = () => {
    window.removeEventListener('afterprint', afterPrint);
    area.style.display = prevDisplay || '';
  };
  window.addEventListener('afterprint', afterPrint);
  setTimeout(() => window.print(), 50);
}

// Optional: swipe-to-close on small screens for Payroll modal
(function setupPayrollSwipeToClose(){
  const el = document.getElementById('payrollModal');
  if (!el) return;
  let startY = null, startX = null, tracking = false;
  el.addEventListener('touchstart', (e) => {
    if (!el.classList.contains('show')) return;
    const t = e.touches[0]; startY = t.clientY; startX = t.clientX; tracking = true;
  }, { passive: true });
  el.addEventListener('touchend', (e) => {
    if (!tracking) return; tracking = false;
    const t = e.changedTouches[0];
    const dy = t.clientY - (startY ?? 0);
    const dx = Math.abs(t.clientX - (startX ?? 0));
    if (dy > 80 && dx < 60) closePayrollModal();
  }, { passive: true });
})();

// Toggle Payroll sub-tabs (delegates to module)
function setPayrollSubTab(which) {
  payrollSetPayrollSubTab(which, {
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
    getSearchQuery: () => currentSearch,
  });
}

// Allow payroll module to request balance recompute after transactions
window.__payrollBalancesInvalidate = function() {
  try {
    const evt = new CustomEvent('payroll:recompute-balances');
    document.dispatchEvent(evt);
  } catch {}
}

// Note: maskAccount is also implemented in modules/utils for module use

function exportPayrollCsv() {
  payrollExportPayrollCsv(employees, temporaryEmployees);
}

function quoteCsv(val) {
  const s = String(val ?? '');
  if (/[",\n]/.test(s)) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function escapeHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

window.closeEmployeeModal = function() {
  const modal = document.getElementById('employeeModal');
  if (modal) modal.classList.remove('show');
}

// Prevent clicks inside dialog from bubbling to overlay/global handlers
document.addEventListener('click', (e) => {
  const insideContent = e.target.closest('.modal-content');
  if (insideContent) {
    e.stopPropagation();
  }
});

// Clear form
window.clearForm = function() {
  const form = document.getElementById('employeeForm');
  if (form) form.reset();
  const joinDateEl = document.getElementById('joinDate');
  if (joinDateEl) joinDateEl.valueAsDate = new Date();
  document.querySelectorAll('.error').forEach(el => el.classList.remove('error'));
  const idEl = document.getElementById('employeeId');
  if (idEl) idEl.value = '';
  const titleEl = document.getElementById('formTitle');
  if (titleEl) titleEl.textContent = 'Add New Employee';
  const primaryBtn = document.querySelector('button[form="employeeForm"]');
  if (primaryBtn) primaryBtn.innerHTML = '<i class="fas fa-save"></i> Save Employee';
}

// Sort handlers
window.sortTable = function(column, which) {
  if (which === 'temporary') {
    sortTemporary(column, {
      getTemporaryEmployees: () => temporaryEmployees,
      getSearchQuery: () => currentSearch,
      getDepartmentFilter: () => currentDepartmentFilter,
    });
  } else {
    sortEmployees(column, {
      getEmployees: () => employees,
      getSearchQuery: () => currentSearch,
      getDepartmentFilter: () => currentDepartmentFilter,
    });
  }
}

// Edit employee
window.editEmployee = function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  let employee = list.find(emp => emp.id === id);
  if (employee) {
    // Open modal in edit mode (prevents clearing fields)
    openEmployeeModal(which === 'temporary' ? 'temporary' : 'employees', 'edit');

    const setVal = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.value = val ?? '';
    };
    setVal('employeeId', employee.id);
    setVal('name', employee.name);
    setVal('email', employee.email);
    setVal('position', employee.position);
    setVal('department', employee.department);
    setVal('salary', employee.salary);
    setVal('joinDate', employee.joinDate);
  setVal('qid', employee.qid || '');
  // Resolve possible legacy/alternate keys for expiry dates
  const qidExVal = employee.qidExpiry || employee.qidExpiryDate || employee.qatarIdExpiry || employee.qid_expiry || employee.qidExpire || employee.qidExpiryDt || employee.qidExpiryDateStr || '';
  const passExVal = employee.passportExpiry || employee.passportExpiryDate || employee.passport_expiry || employee.passportExpire || employee.passportExpiryDt || employee.passportExpiryDateStr || '';
  setVal('qidExpiry', qidExVal);
  setVal('passportExpiry', passExVal);
    setVal('phone', employee.phone || '');
  setVal('bankName', employee.bankName || '');
  setVal('bankAccountNumber', employee.bankAccountNumber || '');
  setVal('bankIban', employee.bankIban || '');

    // PDF statuses (buttons removed from form)
    const qidStatus = document.getElementById('qidPdfStatus');
    const passStatus = document.getElementById('passportPdfStatus');
    if (qidStatus) qidStatus.textContent = employee.qidPdfUrl ? 'Uploaded' : '';
    if (passStatus) passStatus.textContent = employee.passportPdfUrl ? 'Uploaded' : '';
    // If expiry fields are missing locally, try a quick fetch to populate the inputs
    try {
      if (!employee.qidExpiry || !employee.passportExpiry) {
        const base = which === 'temporary' ? 'temporaryEmployees' : 'employees';
        getDoc(doc(db, base, id)).then((snap) => {
          if (snap && snap.exists()) {
            const fresh = snap.data();
            const qidEx = document.getElementById('qidExpiry');
            const passEx = document.getElementById('passportExpiry');
            const qidVal = fresh.qidExpiry || fresh.qidExpiryDate || fresh.qatarIdExpiry || fresh.qid_expiry || fresh.qidExpire || fresh.qidExpiryDt || fresh.qidExpiryDateStr || '';
            const passVal = fresh.passportExpiry || fresh.passportExpiryDate || fresh.passport_expiry || fresh.passportExpire || fresh.passportExpiryDt || fresh.passportExpiryDateStr || '';
            if (qidEx) qidEx.value = qidVal;
            if (passEx) passEx.value = passVal;
          }
        }).catch(()=>{});
      }
    } catch {}
  }
}

// Render employee table
function renderEmployeeTable() {
  employeesRenderTable({
    getEmployees: () => employees,
    getSearchQuery: () => currentSearch,
    getDepartmentFilter: () => currentDepartmentFilter,
    getShowTerminated: () => true,
  });
}

// View employee (read-only modal)
window.viewEmployee = async function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  let emp = list.find(e => e.id === id);
  if (!emp) return;
  // If expiry fields are missing locally, fetch a fresh copy to ensure we display up-to-date values
  try {
    if (!emp.qidExpiry && !emp.passportExpiry) {
      const base = which === 'temporary' ? 'temporaryEmployees' : 'employees';
      const snap = await getDoc(doc(db, base, id));
      if (snap && snap.exists()) {
        const fresh = snap.data();
        // Merge and normalize possible legacy field names
        const qidEx = fresh.qidExpiry || fresh.qidExpiryDate || fresh.qatarIdExpiry || fresh.qid_expiry || fresh.qidExpire || fresh.qidExpiryDt || fresh.qidExpiryDateStr;
        const passEx = fresh.passportExpiry || fresh.passportExpiryDate || fresh.passport_expiry || fresh.passportExpire || fresh.passportExpiryDt || fresh.passportExpiryDateStr;
        emp = { ...emp, ...fresh, qidExpiry: emp.qidExpiry || qidEx, passportExpiry: emp.passportExpiry || passEx };
      }
    }
  } catch {}
  const byId = (x) => document.getElementById(x);

  // Open modal immediately to improve perceived performance
  const vm = document.getElementById('viewModal');
  if (vm) vm.classList.add('show');
  // Focus the dialog for accessibility
  try { document.querySelector('#viewModal .modal-content')?.focus(); } catch {}

  // Defer heavy DOM updates to the next animation frame
  requestAnimationFrame(() => {
    const perfLabel = `viewInfoPopulate:${emp.id}`;
    try { console.time(perfLabel); } catch {}

    const fmtCurrency = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
    const deptText = emp.department || '';
    const setText = (id, text) => { const el = byId(id); if (el) el.textContent = text; };
  setText('viewName', emp.name || '');
  setText('viewNameSub', emp.position ? `${emp.position} • ${deptText}` : deptText);
  // Large header duplicates for enhanced layout
  setText('viewNameDisplay', emp.name || '');
  setText('viewNameSubDisplay', emp.position ? `${emp.position} • ${deptText}` : deptText);
  const deptChip = byId('viewDepartmentChip');
  if (deptChip) deptChip.textContent = deptText || '—';
  const typeChip = byId('viewTypeChip');
  if (typeChip) typeChip.textContent = (list === temporaryEmployees) ? 'Temporary' : 'Permanent';
  const termBadge = byId('viewTerminatedBadge');
  if (termBadge) {
    if (emp.terminated) termBadge.classList.remove('hidden'); else termBadge.classList.add('hidden');
  }
    // Show Reinstate when terminated; show Terminate otherwise
    try {
      const termBtn = byId('viewActionTerminate');
      const reinBtn = byId('viewActionReinstate');
      if (termBtn) termBtn.style.display = emp.terminated ? 'none' : '';
      if (reinBtn) reinBtn.style.display = emp.terminated ? '' : 'none';
    } catch {}
    setText('viewEmail', emp.email || '');
    setText('viewQid', emp.qid || '-');
  setText('viewQidExpiry', emp.qidExpiry ? formatDate(emp.qidExpiry) : '-');
  setText('viewPassportExpiry', emp.passportExpiry ? formatDate(emp.passportExpiry) : '-');
  // Also fill short badges in Overview
    setText('viewQidExpiryShort', emp.qidExpiry ? formatDate(emp.qidExpiry) : '-');
    setText('viewPassportExpiryShort', emp.passportExpiry ? formatDate(emp.passportExpiry) : '-');
    // Visual status for expiries (expired/red, soon/orange, ok/neutral)
    try {
      const qidEl = document.getElementById('viewQidExpiryShort');
      const passEl = document.getElementById('viewPassportExpiryShort');
      const updateStatus = (el, ymd) => {
        if (!el) return;
        el.classList.remove('text-rose-600','text-amber-600');
        if (!ymd) return;
        const d = new Date(ymd);
        if (!isNaN(d.getTime())) {
          const today = new Date();
          today.setHours(0,0,0,0);
          const diffDays = Math.floor((d - today) / 86400000);
          if (diffDays < 0) el.classList.add('text-rose-600');
          else if (diffDays <= 30) el.classList.add('text-amber-600');
        }
      };
      updateStatus(qidEl, emp.qidExpiry);
      updateStatus(passEl, emp.passportExpiry);
    } catch {}
    setText('viewPhone', emp.phone || '-');
    setText('viewPosition', emp.position || '');
    setText('viewDepartment', deptText);
    setText('viewSalary', fmtCurrency(emp.salary));
    setText('viewJoinDate', formatDate(emp.joinDate));
  setText('viewBankName', emp.bankName || '-');
  setText('viewAccountNumber', emp.bankAccountNumber || '-');
  setText('viewIban', emp.bankIban || '-');

    // Profile image preview
    const img = byId('viewProfileImage');
    const ph = byId('viewProfilePlaceholder');
    const imgL = byId('viewProfileImageLarge');
    const phL = byId('viewProfilePlaceholderLarge');
    const applyImg = (imgEl, phEl) => {
      if (!imgEl || !phEl) return;
      if (emp.profileImageUrl) {
        imgEl.src = emp.profileImageUrl;
        imgEl.classList.remove('hidden');
        phEl.classList.add('hidden');
      } else {
        imgEl.removeAttribute('src');
        imgEl.classList.add('hidden');
        phEl.classList.remove('hidden');
      }
    };
    applyImg(img, ph);
    applyImg(imgL, phL);

    // Reset Documents tab previews (lazy-load when tab is opened)
    const qidPreview = byId('qidPdfPreview');
    const qidLink = byId('qidPdfLink');
    const qidEmpty = byId('qidDocEmpty');
    if (qidPreview && qidLink && qidEmpty) {
      if (qidPreview.src && qidPreview.src.startsWith('blob:')) {
        try { URL.revokeObjectURL(qidPreview.src); } catch {}
      }
      qidPreview.removeAttribute('src');
      qidPreview.style.display = 'none';
      qidLink.style.display = 'none';
      qidEmpty.style.display = '';
    }
    const passPreview = byId('passportPdfPreview');
    const passLink = byId('passportPdfLink');
    const passEmpty = byId('passportDocEmpty');
    if (passPreview && passLink && passEmpty) {
      if (passPreview.src && passPreview.src.startsWith('blob:')) {
        try { URL.revokeObjectURL(passPreview.src); } catch {}
      }
      passPreview.removeAttribute('src');
      passPreview.style.display = 'none';
      passLink.style.display = 'none';
      passEmpty.style.display = '';
    }

    // Save context for lazy document loading
    currentViewCtx = { id: emp.id, which: (which === 'temporary' ? 'temporary' : 'employees'), docsLoaded: false, revoke: [] };

    // Default to Info tab active
    // Expand only Overview by default; collapse others
    try {
      const sections = ['#acc-overview', '#acc-contact', '#acc-employment', '#acc-bank', '#acc-docs'];
      sections.forEach((sel, idx) => {
        const pane = document.querySelector(sel);
        if (!pane) return;
        if (idx === 0) {
          pane.classList.remove('hidden');
          const chev = document.querySelector(`[data-acc-target="${sel}"] .fa-chevron-down`);
          if (chev) chev.style.transform = 'rotate(180deg)';
        } else {
          pane.classList.add('hidden');
          const chev = document.querySelector(`[data-acc-target="${sel}"] .fa-chevron-down`);
          if (chev) chev.style.transform = '';
        }
      });
    } catch {}

    try { console.timeEnd(perfLabel); } catch {}
  });
}
// (Removed) Employee View payslips loader

// Reprint handler
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-reprint]');
  if (!btn) return;
  const id = btn.getAttribute('data-id');
  if (!id) return;
  try {
    const snap = await getDocs(query(collection(db, 'payslips'), where('__name__', '==', id)));
    if (snap.empty) return;
    const d = snap.docs[0].data();
    // Populate transient state and fields used by print template
    currentPayrollView = {
      id: d.employeeId,
      name: d.employeeName,
      department: d.department,
      position: d.position,
      qid: d.qid,
      joinDate: '',
      _which: d.employeeType === 'Temporary' ? 'temporary' : 'employees'
    };
    // Inject into form inputs so render routine can read values
    const periodEl = document.getElementById('psPeriod');
    const basicEl = document.getElementById('psBasic');
    const notesEl = document.getElementById('psNotes');
  // removed: psIsAdvance element
    const advAmtEl = document.getElementById('psAdvanceAmount');
    if (periodEl) periodEl.value = d.period || '';
    if (basicEl) basicEl.value = d.basic || 0;
    if (notesEl) notesEl.value = d.notes || '';
  // no psIsAdvance; isAdvance derived from amount when printing/saving
    if (advAmtEl) advAmtEl.value = d.advance || d.net || 0;
    renderAndPrintPayslip();
  } catch (err) {
    console.warn('Reprint failed', err);
    showToast('Failed to reprint payslip', 'error');
  }
});

// Preview handler: open payslip in a view modal (same template as print)
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-preview]');
  if (!btn) return;
  const id = btn.getAttribute('data-id');
  if (!id) return;
  try {
    const snap = await getDocs(query(collection(db, 'payslips'), where('__name__', '==', id)));
    if (snap.empty) return;
    const d = snap.docs[0].data();
    // Build a minimal emp context for template rendering
    const emp = {
      id: d.employeeId,
      name: d.employeeName,
      department: d.department,
      position: d.position,
      qid: d.qid,
      joinDate: '',
      _which: d.employeeType === 'Temporary' ? 'temporary' : 'employees'
    };
    // Render the payslip HTML fragment
    const html = renderPayslipHtml({
      emp,
      period: d.period || '',
      notes: d.notes || '',
      basic: Number(d.basic || 0),
      advance: Number(d.advance || d.net || 0),
      net: Number(d.net || d.advance || 0)
    });
    const container = document.getElementById('payslipPreviewContent');
    if (container) container.innerHTML = html;
    const modal = document.getElementById('payslipPreviewModal');
    if (modal) modal.classList.add('show');
  } catch (err) {
    console.warn('Preview failed', err);
    showToast('Failed to load payslip preview', 'error');
  }
});

// Delete payslip handler
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('[data-delete]');
  if (!btn) return;
  const id = btn.getAttribute('data-id');
  if (!id) return;
  const ok = confirm('Delete this payslip? This action cannot be undone.');
  if (!ok) return;
  try {
    // Load the document first to capture month and employee id
    let delEmpId = currentPayrollView?.id || '';
    let delYmDate = new Date();
    try {
      const snap = await getDocs(query(collection(db, 'payslips'), where('__name__','==', id)));
      if (!snap.empty) {
        const d = snap.docs[0].data();
        delEmpId = d.employeeId || delEmpId;
        const ym = d.period || '';
        if (ym && /^\d{4}-\d{2}$/.test(ym)) {
          const [yy,mm] = ym.split('-');
          delYmDate = new Date(Number(yy), Number(mm)-1, 1);
        }
      }
    } catch {}

    await deleteDoc(doc(db, 'payslips', id));
    showToast('Payslip deleted', 'success');
    // Refresh the list if payroll modal is still open
    if (currentPayrollView?.id) {
      await loadPayslipsForPayrollModal(currentPayrollView.id);
    }
    try { window.__payrollBalancesInvalidate?.(); } catch {}
    // Recompute and upsert balance for the month of the deleted slip
    try { await upsertMonthlyBalanceFor(delEmpId, delYmDate); } catch (e) { console.warn('Balance upsert failed (payslip delete)', e); }
  } catch (err) {
    console.error('Delete payslip failed', err);
    showToast('Failed to delete payslip', 'error');
  }
});

// Upsert the monthly balance snapshot into 'balances' collection
async function upsertMonthlyBalanceFor(employeeId, dateObj) {
  try {
    if (!employeeId) return;
    const d = dateObj instanceof Date ? dateObj : new Date(dateObj);
    const ym = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    // Previous month for carryover
    const prevDate = new Date(d.getFullYear(), d.getMonth() - 1, 1);
    const prevYm = `${prevDate.getFullYear()}-${String(prevDate.getMonth() + 1).padStart(2, '0')}`;

    // Gather employee basics
    const list = [...employees, ...temporaryEmployees];
    const emp = list.find(e => e.id === employeeId);
    if (!emp) return;
    const isTemp = temporaryEmployees.some(e => e.id === employeeId);

    // Compute month metrics similarly to table logic
    const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', employeeId)));
    const slips = psSnap.docs.map(docu => ({ id: docu.id, ...docu.data() }));
    const slipsThisMonth = slips.filter(s => (s.period || '') === ym);
    const basic = slipsThisMonth.length
      ? Number(slipsThisMonth.sort((a, b) => (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0))[0].basic || emp.salary || 0)
      : Number(emp.salary || 0);
    const advances = slipsThisMonth.reduce((sum, s) => sum + Number(s.advance || 0), 0);

    const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId', '==', employeeId)));
    const payments = paySnap.docs
      .map(docu => ({ id: docu.id, ...docu.data() }))
      .filter(p => (p.date || '').startsWith(ym + '-') && !Boolean(p.isAdvance))
      .reduce((sum, p) => sum + Number(p.amount || 0) + Number(p.overtime || 0), 0);

    // Carryover: previous month's remaining balance (if any)
    let carryover = 0;
    try {
      const prevDocRef = doc(db, 'balances', `${employeeId}_${prevYm}`);
      const prevSnap = await getDoc(prevDocRef);
      if (prevSnap.exists()) {
        const prev = prevSnap.data();
        carryover = Number(prev.balance || 0) || 0;
      }
    } catch {}

    const balance = Math.max(0, Number(carryover) + Number(basic) - Number(advances) - Number(payments));

    const payload = cleanData({
      employeeId,
      employeeType: isTemp ? 'Temporary' : 'Permanent',
      employeeName: emp.name || '',
      department: emp.department || '',
      position: emp.position || '',
      qid: emp.qid || '',
      month: ym,
      carryover: Number(carryover),
      basic: Number(basic),
      advances: Number(advances),
      payments: Number(payments),
      balance: Number(balance),
      updatedAt: serverTimestamp(),
      updatedBy: auth?.currentUser?.uid || null,
      updatedByEmail: auth?.currentUser?.email || null,
    });

    // Use stable document id: `${employeeId}_${ym}`
    const docId = `${employeeId}_${ym}`;
    const ref = doc(db, 'balances', docId);
    await setDoc(ref, payload);
  } catch (e) {
    console.warn('upsertMonthlyBalanceFor failed', e);
  }
}

// Carryover-aware balance recompute and persistence for a given employee and YYYY-MM
async function updateEmployeeBalance(employeeId, month, employeeData) {
  const ym = String(month || '').slice(0,7);
  if (!employeeId || !/^\d{4}-\d{2}$/.test(ym)) return 0;
  try {
    const [yearStr, monthStr] = ym.split('-');
    const year = Number(yearStr), m = Number(monthStr);
    const prev = new Date(year, m - 2, 1);
    const prevKey = `${prev.getFullYear()}-${String(prev.getMonth()+1).padStart(2,'0')}`;

    // Previous carryover
    let carryover = 0;
    try {
      const prevDoc = await getDoc(doc(db, 'balances', `${employeeId}_${prevKey}`));
      if (prevDoc.exists()) carryover = Number(prevDoc.data().balance || 0) || 0;
    } catch {}

    // Month payslips: basic override + advances sum
    const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId','==', employeeId)));
    const slips = psSnap.docs.map(d => ({ id:d.id, ...d.data() }));
    const curSlips = slips.filter(s => (s.period||'') === ym);
    const basic = curSlips.length
      ? Number(curSlips.sort((a,b)=> (b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0].basic || employeeData?.salary || 0)
      : Number(employeeData?.salary || 0);
    const advances = curSlips.reduce((sum, s) => sum + Number(s.advance || 0), 0);

    // Month payments (exclude advances)
    const paySnap = await getDocs(query(collection(db,'payments'), where('employeeId','==', employeeId)));
    const payments = paySnap.docs
      .map(d => ({ id:d.id, ...d.data() }))
      .filter(p => String(p.date||'').startsWith(ym+'-') && !Boolean(p.isAdvance))
      .reduce((sum, p) => sum + Number(p.amount||0) + Number(p.overtime||0), 0);

    const balance = Math.max(0, Number(carryover) + Number(basic) - Number(advances) - Number(payments));
    const isTemp = String(employeeData?._type || employeeData?._which || '').toLowerCase().includes('temp');
    const payload = cleanData({
      employeeId,
      employeeType: isTemp ? 'Temporary' : 'Permanent',
      employeeName: employeeData?.name || '',
      department: employeeData?.department || '',
      position: employeeData?.position || '',
      qid: employeeData?.qid || '',
      month: ym,
      carryover: Number(carryover),
      basic: Number(basic),
      advances: Number(advances),
      payments: Number(payments),
      balance: Number(balance),
      updatedAt: serverTimestamp(),
      updatedBy: auth?.currentUser?.uid || null,
      updatedByEmail: auth?.currentUser?.email || null,
    });
    await setDoc(doc(db, 'balances', `${employeeId}_${ym}`), payload);
    return balance;
  } catch (e) {
    console.warn('updateEmployeeBalance failed', e);
    return 0;
  }
}

// Ensure a balance doc exists for an employee and month; compute if missing
async function ensureMonthlyBalance(employeeId, month) {
  const ym = String(month||'').slice(0,7);
  if (!employeeId || !/^\d{4}-\d{2}$/.test(ym)) return;
  try {
    const ref = doc(db, 'balances', `${employeeId}_${ym}`);
    const snap = await getDoc(ref);
    if (snap.exists()) return;
    const all = [...employees, ...temporaryEmployees];
    const emp = all.find(e => e.id === employeeId);
    if (!emp) return;
    const isTemp = temporaryEmployees.some(e => e.id === employeeId);
    await updateEmployeeBalance(employeeId, ym, { ...emp, _type: isTemp ? 'Temporary' : 'Permanent' });
  } catch (e) { console.warn('ensureMonthlyBalance failed', e); }
}

// Reconcile all employees for the current month
async function reconcileAllBalances() {
  const ym = new Date().toISOString().slice(0,7);
  const everyone = [...employees, ...temporaryEmployees];
  for (const emp of everyone) {
    const isTemp = temporaryEmployees.some(e => e.id === emp.id);
    try { await updateEmployeeBalance(emp.id, ym, { ...emp, _type: isTemp ? 'Temporary' : 'Permanent' }); } catch (e) { console.warn('reconcile balance failed', emp?.name, e); }
  }
  try { document.dispatchEvent(new Event('payroll:recompute-balances')); } catch {}
}

// Print from preview: copy preview HTML into print area and trigger print
document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'payslipPreviewPrintBtn') {
    const container = document.getElementById('payslipPreviewContent');
    const area = document.getElementById('payslipPrintArea');
    if (!container || !area) return;
    const prevDisplay = area.style.display;
    area.innerHTML = container.innerHTML;
    area.style.display = 'block';
    const afterPrint = () => {
      window.removeEventListener('afterprint', afterPrint);
      area.style.display = prevDisplay || '';
    };
    window.addEventListener('afterprint', afterPrint);
    setTimeout(() => window.print(), 50);
  }
});

// Close preview modal
window.closePayslipPreviewModal = function () {
  const modal = document.getElementById('payslipPreviewModal');
  if (modal) modal.classList.remove('show');
};

// Use overlay/escape to close preview as well (reuse existing global handlers)

window.closeViewModal = function() {
  const vm = document.getElementById('viewModal');
  if (vm) vm.classList.remove('show');
  // Revoke any blob URLs and reset context
  try {
    const qidPreview = document.getElementById('qidPdfPreview');
    const passPreview = document.getElementById('passportPdfPreview');
    [qidPreview, passPreview].forEach((ifr) => {
      if (ifr && ifr.src && ifr.src.startsWith('blob:')) {
        try { URL.revokeObjectURL(ifr.src); } catch {}
      }
      if (ifr) {
        ifr.removeAttribute('src');
        ifr.style.display = 'none';
      }
    });
  } catch {}
  currentViewCtx = { id: null, which: 'employees', docsLoaded: false, revoke: [] };
}

// Enable swipe-to-close on small screens for the View modal
function setupSwipeToClose() {
  const el = document.getElementById('viewModal');
  if (!el) return;
  let startY = null;
  let startX = null;
  let tracking = false;
  el.addEventListener('touchstart', (e) => {
    if (!el.classList.contains('show')) return;
    const t = e.touches[0];
    startY = t.clientY; startX = t.clientX; tracking = true;
  }, { passive: true });
  el.addEventListener('touchmove', (e) => {
    if (!tracking) return;
    // no-op, we only need end delta
  }, { passive: true });
  el.addEventListener('touchend', (e) => {
    if (!tracking) return; tracking = false;
    const t = e.changedTouches[0];
    const dy = t.clientY - (startY ?? 0);
    const dx = Math.abs(t.clientX - (startX ?? 0));
    if (dy > 80 && dx < 60) { // downward swipe
      closeViewModal();
    }
  });
}

// View modal tab handling
function activateViewTab(which) {
  const infoBtn = document.getElementById('viewTabInfoBtn');
  const docsBtn = document.getElementById('viewTabDocsBtn');
  const info = document.getElementById('viewTabInfo');
  const docs = document.getElementById('viewTabDocs');
  if (!infoBtn || !docsBtn || !info || !docs) return;

  const setActive = (btn, active) => {
    if (active) {
      btn.classList.add('font-semibold', 'text-indigo-600');
      btn.classList.remove('text-gray-600');
      btn.classList.add('border-b-2', 'border-indigo-600');
    } else {
      btn.classList.remove('font-semibold', 'text-indigo-600');
      btn.classList.add('text-gray-600');
      btn.classList.remove('border-b-2', 'border-indigo-600');
    }
  };

  if (which === 'docs') {
    info.classList.add('hidden');
    docs.classList.remove('hidden');
    setActive(infoBtn, false);
    setActive(docsBtn, true);
    // Lazy load documents on first open
    if (!currentViewCtx.docsLoaded && currentViewCtx.id) {
      loadCurrentViewDocuments().catch(err => console.warn('Doc load failed', err));
    }
  } else {
    docs.classList.add('hidden');
    info.classList.remove('hidden');
    setActive(infoBtn, true);
    setActive(docsBtn, false);
  }
}

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'viewTabInfoBtn') {
    activateViewTab('info');
  } else if (e.target && e.target.id === 'viewTabDocsBtn') {
    activateViewTab('docs');
  } else if (e.target && e.target.id === 'openDocsFromHeaderBtn') {
    activateViewTab('docs');
  }
});

// Load documents for the current View modal context
async function loadCurrentViewDocuments() {
  const byId = (x) => document.getElementById(x);
  const basePathFor = (w) => w === 'temporary' ? 'temporaryEmployees' : 'employees';
  const w = currentViewCtx.which;
  const id = currentViewCtx.id;
  if (!id) return;

  const overallLabel = `viewDocsLoad:${id}`;
  try { console.time(overallLabel); } catch {}

  async function setDoc(kind, previewId, linkId, emptyId, loadingId) {
    const preview = byId(previewId);
    const link = byId(linkId);
    const empty = byId(emptyId);
    const loading = byId(loadingId);
    if (!preview || !link || !empty) return;

    const storagePath = `${basePathFor(w)}/${id}/${kind}Pdf.pdf`;
    const ref = storageRef(storage, storagePath);
    if (loading) loading.style.display = '';
    empty.style.display = 'none';
    // Try a fresh streaming URL first
    try {
      const url = await getDownloadURL(ref);
      preview.src = url;
      preview.style.display = '';
      link.href = url;
      link.style.display = '';
      if (loading) loading.style.display = 'none';
      return;
    } catch (e) {
      console.warn('getDownloadURL failed, trying blob', kind, e?.code || e?.message || e);
    }
    // Fallback to blob (may be heavy for large files)
    try {
      const blob = await getBlob(ref);
      const blobUrl = URL.createObjectURL(blob);
      preview.src = blobUrl;
      preview.style.display = '';
      link.href = blobUrl;
      link.style.display = '';
      if (loading) loading.style.display = 'none';
      currentViewCtx.revoke.push(blobUrl);
    } catch (e) {
      console.warn('Blob download failed', kind, e?.code || e?.message || e);
      preview.removeAttribute('src');
      preview.style.display = 'none';
      link.style.display = 'none';
      if (loading) loading.style.display = 'none';
      empty.style.display = '';
    }
  }

  await Promise.all([
    setDoc('qid', 'qidPdfPreview', 'qidPdfLink', 'qidDocEmpty', 'qidLoading'),
    setDoc('passport', 'passportPdfPreview', 'passportPdfLink', 'passportDocEmpty', 'passportLoading'),
  ]);
  currentViewCtx.docsLoaded = true;
  try { console.timeEnd(overallLabel); } catch {}
}

// Render temporary employee table
function renderTemporaryTable() {
  temporaryRenderTable({
    getTemporaryEmployees: () => temporaryEmployees,
    getSearchQuery: () => currentSearch,
    getDepartmentFilter: () => currentDepartmentFilter,
    getShowTerminated: () => true,
  });
}

// Update statistics
function updateStats() {
  const totalEl = document.getElementById('totalEmployees');
  const deptEl = document.getElementById('totalDepartments');
  const avgEl = document.getElementById('avgSalary');
  if (!totalEl && !deptEl && !avgEl) return; // nothing to update

  if (totalEl) totalEl.textContent = employees.length;

  if (deptEl) {
    const departments = [...new Set(employees.map(emp => emp.department))];
    deptEl.textContent = departments.length;
  }

  if (avgEl) {
    const avgSalary = employees.length > 0
      ? employees.reduce((sum, emp) => sum + Number(emp.salary || 0), 0) / employees.length
      : 0;
    avgEl.textContent = `$${avgSalary.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  }
}

// Update department filter
function updateDepartmentFilter() {
  // Department filter removed from UI; nothing to update
}

// Form validation
function validateForm() {
  let isValid = true;
  const fields = ['name', 'email', 'position', 'department', 'salary', 'joinDate'];

  fields.forEach(field => {
    const input = document.getElementById(field);
    const value = input.value.trim();

    if (!value) {
      input.classList.add('error');
      isValid = false;
    } else {
      input.classList.remove('error');
    }

    if (field === 'email' && value) {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(value)) {
        input.classList.add('error');
        isValid = false;
      }
    }
  });

  // Optional: Additional validation for qid if present (digits only)
  const qidInput = document.getElementById('qid');
  if (qidInput && qidInput.value.trim()) {
    const digits = qidInput.value.replace(/\D/g, '');
    if (digits.length < 6 || digits.length > 15) {
      qidInput.classList.add('error');
      isValid = false;
    } else {
      qidInput.classList.remove('error');
      qidInput.value = digits;
    }
  }

  return isValid;
}

// Mark file input pending helper
function markPending(which) {
  const map = {
    qid: 'qidPdfStatus',
    passport: 'passportPdfStatus',
    profile: 'profileImageStatus'
  };
  const id = map[which];
  const el = document.getElementById(id);
  if (el) el.textContent = 'Selected (will upload on Save)';
}

// Toast notifications
function showToast(message, type = 'success') {
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;

  const icon = type === 'success' ? 'fa-check-circle' :
    type === 'error' ? 'fa-exclamation-circle' : 
    type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle';

  toast.innerHTML = `
        <i class="fas ${icon}"></i>
        <span>${message}</span>
    `;

  document.getElementById('toastContainer').appendChild(toast);

  setTimeout(() => {
    toast.style.animation = 'fadeOut 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}

// Theme toggle removed: no theme initialization or toggle handlers

// Format date
function formatDate(dateString) {
  const options = { year: 'numeric', month: 'short', day: 'numeric' };
  return new Date(dateString).toLocaleDateString(undefined, options);
}

// Add pulse and fadeOut animations (and badge style) once
(function injectStyles() {
  if (document.getElementById('crm-style-inject')) return;
  const style = document.createElement('style');
  style.id = 'crm-style-inject';
  style.textContent = `
    @keyframes pulse {
        0% { box-shadow: 0 0 0 0 rgba(99, 102, 241, 0.4); }
        70% { box-shadow: 0 0 0 10px rgba(99, 102, 241, 0); }
        100% { box-shadow: 0 0 0 0 rgba(99, 102, 241, 0); }
    }
    @keyframes fadeOut {
        to { opacity: 0; transform: translateX(100%); }
    }
    .badge {
    background: var(--primary);
        color: white;
        padding: 0.25rem 0.5rem;
        border-radius: 4px;
        font-size: 0.85rem;
    }
  /* Modal polish */
  #viewModal .modal-content { border-radius: 16px; }
  #viewModal .modal-header { border-bottom: 1px solid #f1f5f9; }
  #viewModal .modal-body { background: linear-gradient(180deg, rgba(248,250,252,0.6), rgba(255,255,255,1)); }
  #viewModal .card { box-shadow: 0 0 0 1px rgba(226,232,240,0.7) inset; }
    #openDocsFromHeaderBtn:hover { background: rgba(99,102,241,0.06); }
    /* Accordion chevron animation */
    [data-acc-toggle] .fa-chevron-down { transition: transform 0.2s ease; }
    /* Focus outline for modal content */
    #viewModal .modal-content:focus { outline: 3px solid rgba(99,102,241,0.3); outline-offset: 2px; }
    /* Tighter spacing for grid rows and labels */
    #viewModal dl > div { align-items: start; }
    #viewModal dt { line-height: 1rem; margin-top: 0.125rem; }
    #viewModal dd { line-height: 1.25rem; }
    #viewModal .modal-header { padding-left: 1.25rem; padding-right: 1.25rem; }
    #viewModal .modal-footer { padding-left: 1.25rem; padding-right: 1.25rem; }
  `;
  document.head.appendChild(style);
})();

// Utilities and helpers
function setDefaultJoinDate() {
  const el = document.getElementById('joinDate');
  if (el && !el.value) el.valueAsDate = new Date();
}

// Auth helpers
function updateAuthUI(user) {
  const userInfo = document.getElementById('userInfo');
  const userName = document.getElementById('userName');
  const userPhoto = document.getElementById('userPhoto');
  const userAvatar = document.getElementById('userAvatar');
  const userBadge = document.querySelector('.user-badge');

  if (user) {
    if (userInfo) userInfo.style.display = 'inline-block';
    if (userName) userName.textContent = user.displayName || user.email || 'Signed in';
    if (userAvatar) {
      const seed = (user.displayName || user.email || 'U').trim();
      userAvatar.textContent = seed.charAt(0).toUpperCase();
    }
    if (userBadge) userBadge.style.visibility = 'visible';
    if (userPhoto && user.photoURL) {
      userPhoto.src = user.photoURL;
      userPhoto.style.display = 'inline-block';
    } else if (userPhoto) {
      userPhoto.style.display = 'none';
    }
  } else {
    if (userInfo) userInfo.style.display = 'none';
    if (userBadge) userBadge.style.visibility = 'hidden';
  }
}

// Email/password auth flows (login page)
async function emailPasswordSignIn() {
  const emailEl = document.getElementById('loginEmail');
  const passEl = document.getElementById('loginPassword');
  const email = emailEl ? emailEl.value.trim() : '';
  const password = passEl ? passEl.value : '';
  if (!email || !password) {
    showToast('Email and password are required', 'warning');
    return;
  }
  try {
    await signInWithEmailAndPassword(auth, email, password);
    showToast('Signed in successfully', 'success');
  } catch (e) {
    console.error('Email sign-in failed', e);
    showToast(prettyAuthError(e), 'error');
  }
}

async function emailPasswordSignUp() {
  const emailEl = document.getElementById('loginEmail');
  const passEl = document.getElementById('loginPassword');
  const email = emailEl ? emailEl.value.trim() : '';
  const password = passEl ? passEl.value : '';
  if (!email || !password) {
    showToast('Email and password are required', 'warning');
    return;
  }
  try {
    await createUserWithEmailAndPassword(auth, email, password);
    showToast('Account created and signed in', 'success');
  } catch (e) {
    console.error('Sign-up failed', e);
    showToast(prettyAuthError(e), 'error');
  }
}

async function emailPasswordReset() {
  const emailEl = document.getElementById('loginEmail');
  const email = emailEl ? emailEl.value.trim() : '';
  if (!email) {
    showToast('Enter your email to reset password', 'warning');
    return;
  }
  try {
    await sendPasswordResetEmail(auth, email);
    showToast('Password reset email sent', 'success');
  } catch (e) {
    console.error('Reset failed', e);
    showToast(prettyAuthError(e), 'error');
  }
}

function prettyAuthError(e) {
  const code = e && e.code ? e.code : '';
  switch (code) {
    case 'auth/invalid-email':
      return 'Invalid email address';
    case 'auth/user-disabled':
      return 'This user account is disabled';
    case 'auth/user-not-found':
      return 'No user found with that email';
    case 'auth/wrong-password':
    case 'auth/invalid-credential':
      return 'Incorrect email or password';
    case 'auth/email-already-in-use':
      return 'Email already in use';
    case 'auth/weak-password':
      return 'Password should be at least 6 characters';
    case 'auth/operation-not-allowed':
      return 'This sign-in method is not enabled';
    default:
      return 'Authentication error. Please try again.';
  }
}

// =====================
// Client Transactions (Monthly)
// =====================
// Lightweight in-memory cache for grid edits per client+month
const __clientBillingGridCache = (window.__clientBillingGridCache ||= {});
let __clientBillingGridCtx = null; // { tbody, ym, rows: [{key, clientId}], cols }
let __clientBillingSel = null;     // { sr, sc, er, ec }
let __clientBillingEditing = null; // { r, c, cell, field, original }
let __clientBillingActive = false;

function injectClientBillingGridStyles() {
  if (document.getElementById('client-billing-grid-styles')) return;
  const style = document.createElement('style');
  style.id = 'client-billing-grid-styles';
  style.textContent = `
    /* Excel-like grid visuals for Client Transactions */
    #clientsBillingSection td.grid-cell, #clientsBillingSection th.grid-cell { border: 1px solid #e5e7eb; }
    #clientsBillingSection table { border-collapse: separate; border-spacing: 0; }
    #clientsBillingSection td.grid-cell { position: relative; cursor: cell; }
    #clientsBillingSection td.grid-cell.readonly { background: #fafafa; color: #475569; }
    #clientsBillingSection td.grid-cell.grid-active { outline: 2px solid #4f46e5; outline-offset: -2px; background: #eef2ff; }
    #clientsBillingSection td.grid-cell.grid-selected { background: #eef2ff; }
    #clientsBillingSection td.grid-cell.editing { outline: 2px solid #22c55e; background: #ecfdf5; }
    #clientsBillingSection td.grid-cell[contenteditable="true"] { caret-color: #111827; }
  `;
  document.head.appendChild(style);
}

function __cb_isEditableCol(c) {
  // 0 Date (ro), 1 Assigned (ro), 2 Monthly (num), 3 Debited (num), 4 Credited (num), 5 Outstanding (ro computed), 6 Notes (text)
  return c === 2 || c === 3 || c === 4 || c === 6;
}
function __cb_isNumericCol(c) { return c === 2 || c === 3 || c === 4; }
function __cb_fmtCurrency(n) { return `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`; }
function __cb_parseNumber(s) {
  if (typeof s !== 'string') return Number(s||0);
  const t = s.replace(/[^0-9.\-]/g, '');
  const v = Number(t);
  return isFinite(v) ? v : 0;
}

function __cb_getCell(r, c) {
  if (!__clientBillingGridCtx?.tbody) return null;
  return __clientBillingGridCtx.tbody.querySelector(`td.grid-cell[data-row="${r}"][data-col="${c}"]`);
}
function __cb_clearSel() {
  if (!__clientBillingGridCtx?.tbody) return;
  __clientBillingGridCtx.tbody.querySelectorAll('td.grid-cell.grid-active, td.grid-cell.grid-selected').forEach(el=>{
    el.classList.remove('grid-active','grid-selected');
  });
}
function __cb_applySel() {
  if (!__clientBillingGridCtx?.tbody || !__clientBillingSel) return;
  const { sr, sc, er, ec } = __clientBillingSel;
  const r0 = Math.min(sr, er), r1 = Math.max(sr, er);
  const c0 = Math.min(sc, ec), c1 = Math.max(sc, ec);
  for (let r=r0; r<=r1; r++) {
    for (let c=c0; c<=c1; c++) {
      const cell = __cb_getCell(r,c);
      if (!cell) continue;
      cell.classList.add('grid-selected');
    }
  }
  const active = __cb_getCell(sr, sc);
  if (active) active.classList.add('grid-active');
}
function __cb_setActive(r,c, extend=false) {
  if (!__clientBillingGridCtx) return;
  const maxR = (__clientBillingGridCtx.rows?.length||1)-1;
  const maxC = 6;
  r = Math.max(0, Math.min(maxR, r));
  c = Math.max(0, Math.min(maxC, c));
  if (!extend || !__clientBillingSel) {
    __clientBillingSel = { sr:r, sc:c, er:r, ec:c };
  } else {
    __clientBillingSel.er = r; __clientBillingSel.ec = c;
  }
  __cb_clearSel();
  __cb_applySel();
}
function __cb_startEdit(r,c) {
  if (!__clientBillingGridCtx) return;
  if (!__cb_isEditableCol(c)) return;
  const cell = __cb_getCell(r,c);
  if (!cell) return;
  if (__clientBillingEditing) __cb_stopEdit(false);
  const field = cell.getAttribute('data-field');
  const raw = cell.getAttribute('data-raw') || cell.textContent;
  __clientBillingEditing = { r, c, cell, field, original: raw };
  cell.classList.add('editing');
  cell.setAttribute('contenteditable','true');
  // Set plain value for numeric fields when entering
  if (__cb_isNumericCol(c)) {
    const v = __cb_parseNumber(raw);
    cell.textContent = String(v || 0);
  }
  // Focus and select
  const range = document.createRange();
  range.selectNodeContents(cell);
  const sel = window.getSelection();
  sel.removeAllRanges(); sel.addRange(range);
}
function __cb_commitEdit(save) {
  if (!__clientBillingEditing) return;
  const { r, c, cell, field, original } = __clientBillingEditing;
  const text = cell.textContent || '';
  let display = text;
  let toStore = text;
  if (__cb_isNumericCol(c)) {
    const num = __cb_parseNumber(text);
    display = __cb_fmtCurrency(num);
    toStore = String(num);
  }
  if (save) {
    cell.setAttribute('data-raw', toStore);
    const rowMeta = __clientBillingGridCtx.rows?.[r];
    if (rowMeta) {
      const key = rowMeta.key;
      const bucket = (__clientBillingGridCache[key] ||= {});
      if (field) bucket[field] = __cb_isNumericCol(c) ? __cb_parseNumber(toStore) : text;
      // If Debited changed, reconcile AR
      if (field === 'debited') { try { __cb_reconcileDebitedAR(r); } catch {} }
      // Recompute Outstanding (col 5)
      try {
        // Effective Monthly = user-entered monthly (>0) else computed default for the row
        const mcell = __cb_getCell(r,2);
        const oc = __cb_getCell(r,5);
        const prevBase = Number(oc?.getAttribute('data-prev-outstanding') || 0);
        const deb = Number(bucket.debited||0);
        const mRaw = Number((bucket.monthly ?? __cb_parseNumber(mcell?.getAttribute('data-raw') || '0')) || 0);
        const mDef = Number(mcell?.getAttribute('data-default-monthly') || 0);
        const effMonthly = mRaw > 0 ? mRaw : mDef;
        const out = Math.max(0, Number(prevBase) + Number(effMonthly) - Number(deb));
        if (oc) { oc.textContent = __cb_fmtCurrency(out); oc.setAttribute('data-raw', String(out)); }
      } catch {}
    }
  } else {
    // Restore previous display
    if (__cb_isNumericCol(c)) display = __cb_fmtCurrency(__cb_parseNumber(original)); else display = original;
  }
  cell.textContent = display;
}
function __cb_stopEdit(save) {
  if (!__clientBillingEditing) return;
  try { __cb_commitEdit(save); } catch {}
  const { cell } = __clientBillingEditing;
  cell.removeAttribute('contenteditable');
  cell.classList.remove('editing');
  __clientBillingEditing = null;
}

function setupClientBillingGrid(tbody, gridRowsMeta, ym) {
  injectClientBillingGridStyles();
  __clientBillingGridCtx = { tbody, rows: gridRowsMeta, ym };
  __clientBillingActive = true;
  __cb_setActive(0, 0);

  // Click to select or Shift+Click to extend
  if (!tbody.__gridWired) {
    tbody.addEventListener('mousedown', (e) => {
      const td = e.target.closest('td.grid-cell');
      if (!td) return;
      __clientBillingActive = true;
      const r = Number(td.getAttribute('data-row')||0);
      const c = Number(td.getAttribute('data-col')||0);
      const extend = e.shiftKey;
      __cb_setActive(r, c, extend);
    });
    // Double click or F2 to edit
    tbody.addEventListener('dblclick', (e) => {
      const td = e.target.closest('td.grid-cell');
      if (!td) return;
      const r = Number(td.getAttribute('data-row')||0);
      const c = Number(td.getAttribute('data-col')||0);
      __cb_startEdit(r,c);
    });
    tbody.__gridWired = true;
  }
  // Global key handlers (once)
  if (!document.__cbKeyWired) {
    document.addEventListener('keydown', (e) => {
      if (!__clientBillingActive) return;
      // If editing, handle Enter/Escape/Tab
      if (__clientBillingEditing) {
        if (e.key === 'Enter') { e.preventDefault(); __cb_stopEdit(true); return; }
        if (e.key === 'Escape') { e.preventDefault(); __cb_stopEdit(false); return; }
        return;
      }
      if (!__clientBillingSel) return;
      const { sr, sc } = __clientBillingSel;
      if (e.key === 'F2') { e.preventDefault(); __cb_startEdit(sr, sc); return; }
      if (e.key === 'Enter') { e.preventDefault(); __cb_setActive(sr+1, sc); return; }
      if (e.key === 'Tab') { e.preventDefault(); __cb_setActive(sr, sc + (e.shiftKey ? -1 : 1)); return; }
      if (e.key === 'ArrowLeft') { e.preventDefault(); __cb_setActive(sr, sc-1); return; }
      if (e.key === 'ArrowRight') { e.preventDefault(); __cb_setActive(sr, sc+1); return; }
      if (e.key === 'ArrowUp') { e.preventDefault(); __cb_setActive(sr-1, sc); return; }
      if (e.key === 'ArrowDown') { e.preventDefault(); __cb_setActive(sr+1, sc); return; }
      // Typing starts edit if editable
      if (typeof e.key === 'string' && e.key.length === 1 && !e.ctrlKey && !e.metaKey && !e.altKey) {
        if (__cb_isEditableCol(sc)) {
          __cb_startEdit(sr, sc);
          // Replace with typed char
          setTimeout(()=>{ try { if (__clientBillingEditing?.cell) { __clientBillingEditing.cell.textContent = e.key; } } catch {} }, 0);
          e.preventDefault();
        }
      }
    });
    // Copy
    document.addEventListener('copy', (e) => {
      if (!__clientBillingActive || !__clientBillingSel) return;
      const { sr, sc, er, ec } = __clientBillingSel;
      const r0 = Math.min(sr, er), r1 = Math.max(sr, er);
      const c0 = Math.min(sc, ec), c1 = Math.max(sc, ec);
      let out = [];
      for (let r=r0; r<=r1; r++) {
        const row = [];
        for (let c=c0; c<=c1; c++) {
          const cell = __cb_getCell(r,c);
          row.push(cell ? (cell.getAttribute('data-raw') || cell.textContent || '') : '');
        }
        out.push(row.join('\t'));
      }
      try { e.clipboardData.setData('text/plain', out.join('\n')); e.preventDefault(); } catch {}
    });
    // Paste
    document.addEventListener('paste', (e) => {
      if (!__clientBillingActive || !__clientBillingSel) return;
      const text = e.clipboardData?.getData('text/plain');
      if (!text) return;
      const { sr, sc } = __clientBillingSel;
      const rows = text.split(/\r?\n/);
      for (let i=0; i<rows.length; i++) {
        if (rows[i] === '') continue;
        const cols = rows[i].split('\t');
        for (let j=0; j<cols.length; j++) {
          const r = sr + i, c = sc + j;
          if (!__cb_isEditableCol(c)) continue;
          const cell = __cb_getCell(r,c);
          if (!cell) continue;
          const val = cols[j];
          // Commit directly without entering edit mode
          const field = cell.getAttribute('data-field');
          const rowMeta = __clientBillingGridCtx.rows?.[r];
          if (!rowMeta) continue;
          const key = rowMeta.key;
          const bucket = (__clientBillingGridCache[key] ||= {});
          if (__cb_isNumericCol(c)) {
            const num = __cb_parseNumber(val);
            bucket[field] = num;
            cell.setAttribute('data-raw', String(num));
            cell.textContent = __cb_fmtCurrency(num);
            if (field === 'debited') { try { __cb_reconcileDebitedAR(r); } catch {} }
          } else {
            bucket[field] = val;
            cell.setAttribute('data-raw', val);
            cell.textContent = val;
          }
          // Update Outstanding if needed
          try {
            const mcell = __cb_getCell(r,2);
            const oc = __cb_getCell(r,5);
            const prevBase = Number(oc?.getAttribute('data-prev-outstanding') || 0);
            const deb = Number(bucket.debited||0);
            const mRaw = Number((bucket.monthly ?? __cb_parseNumber(mcell?.getAttribute('data-raw') || '0')) || 0);
            const mDef = Number(mcell?.getAttribute('data-default-monthly') || 0);
            const effMonthly = mRaw > 0 ? mRaw : mDef;
            const out = Math.max(0, Number(prevBase) + Number(effMonthly) - Number(deb));
            if (oc) { oc.textContent = __cb_fmtCurrency(out); oc.setAttribute('data-raw', String(out)); }
          } catch {}
        }
      }
      e.preventDefault();
    });
    // Click outside to deactivate
    document.addEventListener('mousedown', (e) => {
      const sec = document.getElementById('clientsBillingSection');
      if (!sec) return;
      if (!sec.contains(e.target)) {
        __clientBillingActive = false;
        if (__clientBillingEditing) __cb_stopEdit(true);
      }
    });
    document.__cbKeyWired = true;
  }
}

// Reconcile Fund Adjustment (Asset) for a client+month when Debited changes
async function __cb_reconcileDebitedAR(rowIndex) {
  try {
    const rowMeta = __clientBillingGridCtx?.rows?.[rowIndex];
    const ym = __clientBillingGridCtx?.ym || '';
    if (!rowMeta || !rowMeta.clientId || !/^\d{4}-\d{2}$/.test(ym)) return;
    const key = rowMeta.key;
    const bucket = (__clientBillingGridCache[key] ||= {});
    const debited = Math.abs(Number(bucket.debited || 0)) || 0;
    // Resolve client name for human-friendly notes
    let clientName = '';
    try {
      const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
      clientName = (list.find(x => x.id === rowMeta.clientId)?.name) || rowMeta.clientName || rowMeta.clientEmail || rowMeta.clientId || '';
    } catch {}
    // Ensure a Fund Adjustment Asset account exists (affects Fund by design per request)
    const fundAdjId = await ensureAssetAccount('fund adjustment', 'Fund Adjustment');
    if (!fundAdjId) return;
    // Compute current posted debited total for this client+month in Fund Adjustment account
    const qInv = query(collection(db, 'cashflows'), where('clientId', '==', rowMeta.clientId));
    const snap = await getDocs(qInv);
    let postedDebits = 0, postedDebitAdjustments = 0;
    snap.forEach(docu => {
      const d = docu.data();
      const cat = String(d.category || '');
      const typ = String(d.type || '').toLowerCase();
      const amt = Math.abs(Number(d.amount || 0)) || 0;
      if ((d.month || '') !== ym) return;
      if (d.accountId !== fundAdjId) return;
      if (cat === 'Fund Adjustment (Debited)' && typ === 'in') postedDebits += amt;
      if (cat === 'Fund Adjustment (Debited Adjustment)' && typ === 'out') postedDebitAdjustments += amt;
    });
    const currentPosted = Math.max(0, postedDebits - postedDebitAdjustments);
    const delta = Number(debited) - Number(currentPosted);
    if (Math.abs(delta) < 0.005) return; // no material change
    const today = new Date();
    // Use today's date so the entry shows up in Recent Transactions immediately; keep `month` for reporting
    const dateStr = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
    const accName = 'Fund Adjustment';
    if (delta > 0) {
      await addDoc(collection(db, 'cashflows'), cleanData({
        date: dateStr,
        type: 'in',
        accountId: fundAdjId,
        accountName: accName,
        amount: Number(delta),
        category: 'Fund Adjustment (Debited)',
        clientId: rowMeta.clientId,
        month: ym,
        notes: `Debited update (posted to Fund Adjustment) — Client: ${clientName}`,
        createdAt: today.toISOString(),
        createdBy: auth?.currentUser?.uid || undefined,
        createdByEmail: auth?.currentUser?.email || undefined,
      }));
    } else {
      await addDoc(collection(db, 'cashflows'), cleanData({
        date: dateStr,
        type: 'out',
        accountId: fundAdjId,
        accountName: accName,
        amount: Math.abs(Number(delta)),
        category: 'Fund Adjustment (Debited Adjustment)',
        clientId: rowMeta.clientId,
        month: ym,
        notes: `Debited decrease (posted to Fund Adjustment) — Client: ${clientName}`,
        createdAt: today.toISOString(),
        createdBy: auth?.currentUser?.uid || undefined,
        createdByEmail: auth?.currentUser?.email || undefined,
      }));
    }
    // Ledger/Overview will auto-refresh via snapshot listeners
    try {
      // Update cache so the grid reflects the saved value without reload
      const cache = (__clientBillingGridCache[key] ||= {});
      cache.debited = debited;
    } catch {}
  } catch (e) {
    console.warn('Fund Adjustment reconcile failed (debited)', e);
    try { showToast && showToast('Failed to post Debited to Fund Adjustment', 'error'); } catch {}
  }
}

// Fetch sums for a client+month from cashflows: debited (Fund Adjustment) and credited (Client Payment)
async function __cb_fetchClientMonthSums(clientId, ym) {
  try {
    if (!clientId || !/^\d{4}-\d{2}$/.test(ym)) return { debited: 0, credited: 0 };
    const qInv = query(collection(db, 'cashflows'), where('clientId', '==', clientId));
    const snap = await getDocs(qInv);
    let debitedIn = 0, debitedAdjOut = 0, creditedIn = 0;
    snap.forEach(docu => {
      const d = docu.data();
      if ((d.month || '') !== ym) return;
      const cat = String(d.category || '');
      const typ = String(d.type || '').toLowerCase();
      const amt = Math.abs(Number(d.amount || 0)) || 0;
      if (typ === 'in' && cat === 'Client Payment') creditedIn += amt;
      if (typ === 'in' && cat === 'Fund Adjustment (Debited)') debitedIn += amt;
      if (typ === 'out' && cat === 'Fund Adjustment (Debited Adjustment)') debitedAdjOut += amt;
    });
    return { debited: Math.max(0, debitedIn - debitedAdjOut), credited: creditedIn };
  } catch (e) {
    console.warn('Failed to fetch client-month sums', e);
    return { debited: 0, credited: 0 };
  }
}

async function renderClientTransactions() {
  const monthEl = document.getElementById('clientBillingMonth');
  const clientSel = document.getElementById('clientBillingClient');
  const tbody = document.getElementById('clientBillingTableBody');
  const empty = document.getElementById('clientBillingEmpty');
  if (!monthEl || !tbody || !empty) return;
  const ym = monthEl.value || '';
  const clientFilter = clientSel ? (clientSel.value || '') : '';
  const clients = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
  const assignments = (typeof getAssignments === 'function') ? getAssignments() : (window.getAssignments ? window.getAssignments() : []);
  // Filter assignments that are active within selected Ym
  const parseYmd = (s) => {
    if (!s) return null;
    try { const d = new Date(s); return isNaN(d.getTime()) ? null : d; } catch { return null; }
  };
  const monthStart = ym && /^\d{4}-\d{2}$/.test(ym) ? new Date(Number(ym.slice(0,4)), Number(ym.slice(5,7)) - 1, 1) : null;
  const monthEnd = monthStart ? new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 0) : null; // last day of month

  const activeForMonth = (a) => {
    if (!monthStart || !monthEnd) return true; // if not set, count all
    const s = parseYmd(a.startDate);
    const e = a.endDate ? parseYmd(a.endDate) : null;
    // active if assignment overlaps any day in [monthStart, monthEnd]
    const startsBeforeOrOnEnd = s ? (s <= monthEnd) : true;
    const endsAfterOrOnStart = e ? (e >= monthStart) : true; // null end -> ongoing
    return startsBeforeOrOnEnd && endsAfterOrOnStart;
  };

  const byClient = new Map();
  for (const c of clients) {
    byClient.set(c.id, { client: c, count: 0 });
  }
  for (const a of assignments) {
    if (!a || !a.clientId) continue;
    if (!activeForMonth(a)) continue;
    if (!byClient.has(a.clientId)) {
      byClient.set(a.clientId, { client: { id: a.clientId, name: a.clientName, email: a.clientEmail, phone: '' }, count: 0 });
    }
    byClient.get(a.clientId).count += 1;
  }

  let rows = Array.from(byClient.values()).sort((A,B)=> (A.client.name||'').localeCompare(B.client.name||''));
  // Filter by selected client; if none selected, show nothing so page loads clean
  if (clientFilter) {
    rows = rows.filter(r => r.client && r.client.id === clientFilter);
  } else {
    rows = [];
  }
  if (!rows.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmt = (n) => `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const monthDateStr = (() => {
    if (!ym || !/^\d{4}-\d{2}$/.test(ym)) return '';
    // Avoid UTC conversion shifting the date (e.g., to previous day); format directly
    return `${ym}-01`;
  })();
  // Preload Firestore-backed sums (current month) and compute previous outstanding by folding prior months
  const sumsByClient = new Map();
  const prevOutstandingByClient = new Map();
  // Helpers for months
  const ymToDate = (s) => new Date(Number(s.slice(0,4)), Number(s.slice(5,7)) - 1, 1);
  const dateToYm = (d) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
  const nextYm = (s) => { const d = ymToDate(s); d.setMonth(d.getMonth()+1); return dateToYm(d); };
  const monthRange = (startYm, endYmExcl) => { const out=[]; let cur=startYm; while(cur < endYmExcl){ out.push(cur); cur = nextYm(cur);} return out; };
  const isActiveForYm2 = (a, ymStr) => {
    const y = Number(ymStr.slice(0,4)); const m = Number(ymStr.slice(5,7));
    const s = a.startDate ? new Date(a.startDate) : null;
    const e = a.endDate ? new Date(a.endDate) : null;
    const mStart = new Date(y, m-1, 1);
    const mEnd = new Date(y, m, 0);
    const startsBeforeOrOnEnd = s ? (s <= mEnd) : true;
    const endsAfterOrOnStart = e ? (e >= mStart) : true;
    return startsBeforeOrOnEnd && endsAfterOrOnStart;
  };
  for (const { client } of rows) {
    const id = client.id || client.clientId; if (!id) continue;
    // Current-month sums (debited and credited for display)
    try { sumsByClient.set(id, await __cb_fetchClientMonthSums(id, ym)); } catch { sumsByClient.set(id, { debited: 0, credited: 0 }); }
    // Build debited-by-month from cashflows once
    let snap = null; try { snap = await getDocs(query(collection(db,'cashflows'), where('clientId','==', id))); } catch {}
    const debitedByMonth = new Map(); // ym => net debited
    if (snap) {
      snap.forEach(docu => {
        const d = docu.data(); const mon = String(d.month || ''); if (!/^\d{4}-\d{2}$/.test(mon)) return;
        const cat = String(d.category || ''); const typ = String(d.type || '').toLowerCase(); const amt = Math.abs(Number(d.amount || 0)) || 0;
        if (typ==='in' && cat==='Fund Adjustment (Debited)') debitedByMonth.set(mon, (debitedByMonth.get(mon)||0) + amt);
        if (typ==='out' && cat==='Fund Adjustment (Debited Adjustment)') debitedByMonth.set(mon, (debitedByMonth.get(mon)||0) - amt);
      });
    }
    // Determine earliest month to start folding from:
    // 1) earliest assignment start month (if any)
    // 2) earliest month that has a posted Debited entry (from cashflows)
    const clientAssignments = (assignments||[]).filter(a => a && a.clientId === id);
    let earliestYm = ym;
    // From assignments
    if (clientAssignments.length) {
      try {
        const minStart = clientAssignments.reduce((min,a)=>{ const d=a.startDate?new Date(a.startDate):null; if(!d||isNaN(d))return min; return (!min||d<min)?d:min; }, null);
        if (minStart) earliestYm = dateToYm(new Date(minStart.getFullYear(), minStart.getMonth(), 1));
      } catch {}
    }
    // From posted debited months
    try {
      const debitedMonths = Array.from(debitedByMonth.keys()).filter(m => /^\d{4}-\d{2}$/.test(m)).sort();
      if (debitedMonths.length) {
        const firstDebitedYm = debitedMonths[0];
        if (firstDebitedYm < earliestYm) earliestYm = firstDebitedYm;
      }
    } catch {}
    let prevOutstanding = 0;
    if (earliestYm < ym) {
      const months = monthRange(earliestYm, ym);
      for (const mYm of months) {
        // Monthly for mYm: override if >0 else sum monthly assignment rates active in mYm
        const override = Number(client.monthly||0) || 0;
        let monthlyForM = 0;
        if (override > 0) monthlyForM = override; else {
          let sum=0; for (const a of clientAssignments) { if (String(a.rateType||'').toLowerCase()!=='monthly') continue; if (isActiveForYm2(a, mYm)) sum += Number(a.rate||0)||0; }
          monthlyForM = sum;
        }
        const debForM = Math.max(0, Number(debitedByMonth.get(mYm)||0));
        prevOutstanding = Math.max(0, prevOutstanding + monthlyForM - debForM);
      }
    }
    prevOutstandingByClient.set(id, prevOutstanding);
  }

  // Build rows with Firestore sums as defaults and cache-backed overrides
  const gridRowsMeta = [];
  const html = rows.map(({ client, count }, rIndex) => {
    const key = `${client.id || client.clientId || ''}|${ym}`;
    gridRowsMeta.push({ key, clientId: client.id || client.clientId });
    const cache = __clientBillingGridCache[key] || {};
    // Compute default monthly due: client.monthly override if > 0 else sum of active monthly assignment rates
    let assignedMonthlySum = 0;
    try {
      for (const a of assignments) {
        if (!a || a.clientId !== (client.id || client.clientId)) continue;
        const rt = String(a.rateType || '').toLowerCase();
        if (rt === 'monthly' && activeForMonth(a)) assignedMonthlySum += Number(a.rate || 0) || 0;
      }
    } catch {}
    const clientMonthlyOverride = Number((client.monthly ?? 0)) || 0;
    const defaultMonthly = clientMonthlyOverride > 0 ? clientMonthlyOverride : assignedMonthlySum;

  const monthlyRaw = Number(cache.monthly || 0);
  const effMonthly = monthlyRaw > 0 ? monthlyRaw : defaultMonthly;
  const sums = sumsByClient.get(client.id || client.clientId) || { debited: 0, credited: 0 };
  const debited = Number((cache.debited !== undefined ? cache.debited : sums.debited) || 0);
  const credited = Number((cache.credited !== undefined ? cache.credited : sums.credited) || 0);
  // Outstanding carry-forward: previous outstanding + current monthly − current debited
  const prevOutstanding = Math.max(0, Number(prevOutstandingByClient.get(client.id || client.clientId) || 0));
  const outstanding = Math.max(0, prevOutstanding + Number(effMonthly) - Number(debited));
    const notes = String(cache.notes || '');
    // td builder
    const td = (cIndex, text, raw, editable, field, extraCls='', extraAttrs='') => `
      <td class="px-3 py-2 grid-cell ${editable?'' : 'readonly'} ${extraCls}" data-row="${rIndex}" data-col="${cIndex}" ${field?`data-field="${field}"`:''} ${raw!==undefined?`data-raw="${raw}"`:''} ${extraAttrs}>${text}</td>`;
    return `
      <tr>
        ${td(0, escapeHtml(monthDateStr), monthDateStr, false, '', 'text-left')}
        ${td(1, Number(count).toLocaleString(), String(Number(count)||0), false, '', 'text-left')}
        ${td(2, effMonthly ? fmt(effMonthly) : '-', String(monthlyRaw||0), true, 'monthly', 'text-right', `data-default-monthly="${defaultMonthly}"`)}
        ${td(3, fmt(debited), String(debited||0), true, 'debited', 'text-right')}
        ${td(4, fmt(credited), String(credited||0), true, 'credited', 'text-right')}
  ${td(5, fmt(outstanding), String(outstanding||0), false, '', 'text-right', `data-prev-outstanding="${String(prevOutstanding||0)}"`)}
        ${td(6, escapeHtml(notes), notes, true, 'notes', 'text-left')}
      </tr>`;
  }).join('');
  tbody.innerHTML = html;
  // Elevate cells to grid mode
  tbody.querySelectorAll('td').forEach(el => el.classList.add('grid-cell'));
  // Initialize grid interactions
  setupClientBillingGrid(tbody, gridRowsMeta, ym);
}
