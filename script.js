import { db, auth, storage } from './firebase-config.js?v=20251007-06';
// Global currency configuration
const CURRENCY_PREFIX = 'QAR ';
// Ensure escapeHtml is available in module scope (ES modules don't auto-bind window props to identifiers)
const escapeHtml = (typeof window !== 'undefined' && typeof window.escapeHtml === 'function')
  ? window.escapeHtml
  : function escapeHtml(str) {
      const s = String(str ?? '');
      return s
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    };
try { if (typeof window !== 'undefined') window.escapeHtml = escapeHtml; } catch {}
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
  ref as storageRef,
  uploadBytesResumable,
  uploadBytes,
  getDownloadURL,
  getBlob,
  deleteObject
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";
import {
  onAuthStateChanged,
  signOut,
  signInWithEmailAndPassword,
  createUserWithEmailAndPassword,
  sendPasswordResetEmail
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";

// Modules
import {
  initPayroll,
  renderPayrollTable as payrollRenderTable,
  renderPayrollFrame as payrollRenderFrame,
  setPayrollSubTab as payrollSetPayrollSubTab,
  sortPayroll as payrollSort,
  exportPayrollCsv as payrollExportPayrollCsv,
} from './modules/payroll.js?v=20251019-04';
// v-bump to pick up compact UI changes
//
//
// Utilities used in this file (masking account numbers in Payroll modal)
import { renderEmployeeTable as employeesRenderTable, sortEmployees } from './modules/employees.js?v=20251018-07';
import { renderTemporaryTable as temporaryRenderTable, sortTemporary } from './modules/temporary.js?v=20251018-07';
import { renderContractorTable as vehiclesRenderTable, sortContractors as sortVehicles } from './modules/Vehicles.js?v=20251024-01';
import { initClients, subscribeClients, renderClientsTable, getClients, forceRebuildClientsFilter } from './modules/clients.js?v=20251014-01';
import { initAssignments, subscribeAssignments, renderAssignmentsTable, getAssignments } from './modules/assignments.js?v=20251002-03';
import { initAccounts, subscribeAccounts, renderAccountsTable } from './modules/accounts.js?v=20250929-01';
import { initCashflow, subscribeCashflow, renderCashflowTable } from './modules/cashflow.js?v=20251015-07';
import { initLedger, subscribeLedger, renderLedgerTable, refreshLedgerAccounts } from './modules/ledger.js?v=20251014-11';
// Shared utilities (needed for payroll modal account masking & date formatting)
import { maskAccount, formatDate } from './modules/utils.js';

// Global flag to force brand‑new simple client billing implementation
// IMPORTANT: Set BEFORE the conditional so the rewrite block executes.
window.__SIMPLE_BILLING_MODE = true;

// =============================
// SIMPLE BILLING REWRITE (from scratch)
// =============================
// Goals:
// 1. Historical rows immutable.
// 2. One active editable row only.
// 3. Outstanding = Base (last snapshot outstanding or 0 + first monthly) - sum(payments after snapshot) - creditedOverride
// 4. Payment docs do NOT carry running totals.
// 5. New Row creates snapshot doc freezing current active row.

if (window.__SIMPLE_BILLING_MODE) {
  // Override original function definitions with minimal versions.
  window.renderClientTransactions = async function renderClientTransactions() {
    const monthEl = document.getElementById('clientBillingMonth');
    const clientSel = document.getElementById('clientBillingClient');
    const tbody = document.getElementById('clientBillingTableBody');
    const empty = document.getElementById('clientBillingEmpty');
    if (!monthEl || !clientSel || !tbody || !empty) return;
    const ym = monthEl.value || '';
    const clientId = clientSel.value || '';
    if (!/^\d{4}-\d{2}$/.test(ym) || !clientId) {
      tbody.innerHTML=''; empty.classList.remove('hidden'); return;
    }
    empty.classList.add('hidden');
    // Fetch docs for client+month
    let snap=null; try { snap = await getDocs(query(collection(db,'clientTransactions'), where('clientId','==',clientId), where('month','==',ym))); } catch {}
    const docs=[]; if (snap) snap.forEach(d=> docs.push({ id:d.id, ...d.data()}));
    docs.sort((a,b)=> (a.createdAt||'').localeCompare(b.createdAt||''));
  const snapshotDocs = docs.filter(d=>d.field==='newRow');
  const paymentDocs = docs.filter(d=>d.field==='payment');
  const creditedEditDocs = docs.filter(d=>d.field==='credited');
  const debitedEditDocs = docs.filter(d=>d.field==='debited'); // NEW: persist debited for active row (does not affect outstanding)
  const monthlyEditDocs = docs.filter(d=>d.field==='monthly');
  const outstandingOverrideDocs = docs.filter(d=>d.field==='outstanding');
  const noteDocs = docs.filter(d=>d.field==='notes');
    // Determine default monthly (client override else assignment sum = 0 fallback)
    let defaultMonthly = 0;
    try {
      const clients = (typeof getClients==='function')?getClients():[];
      const c = clients.find(c=>c.id===clientId);
      defaultMonthly = Number(c?.monthly||0)||0;
    } catch {}
    // Build immutable historical rows
    const hist = snapshotDocs.map(s => {
      const snap = s.snapshot || {}; // snapshot MUST have canonical fields
      return {
        date: s.date || (s.createdAt||'').slice(0,10) || `${ym}-01`,
        monthly: Number(snap.monthly||s.monthly||defaultMonthly)||0,
        debited: Number(snap.debited||0)||0,
        credited: Number(snap.credited||0)||0,
        outstanding: Number(snap.outstanding||0)||0,
        notes: String(snap.notes||s.notes||''),
        locked: true
      };
    });
    const lastHist = hist.length? hist[hist.length-1]: null;
    const lastSnapshotCreatedAt = snapshotDocs.length? (snapshotDocs[snapshotDocs.length-1].createdAt||'') : '';

    // =============================
    // Carry Forward: If this month has no historical snapshot (i.e., first active segment for the month),
    // fetch previous month's final snapshot outstanding and add it to this month's base outstanding.
    // =============================
    let prevOutstandingCarry = 0;
    if (!lastHist) {
      try {
        if (/^\d{4}-\d{2}$/.test(ym)) {
          const [yStr,mStr] = ym.split('-');
          const y = Number(yStr), m = Number(mStr);
          const prevDate = new Date(y, m - 2, 1); // JS month index: month-1 , so m-2 for previous
          const prevYm = `${prevDate.getFullYear()}-${String(prevDate.getMonth()+1).padStart(2,'0')}`;
          // Only query if prevYm differs (avoid infinite loop if something odd)
          if (prevYm !== ym) {
            let prevSnap = null;
            try {
              prevSnap = await getDocs(query(collection(db,'clientTransactions'), where('clientId','==',clientId), where('month','==',prevYm)));
            } catch {}
            if (prevSnap) {
              const prevDocs = [];
              prevSnap.forEach(d=> prevDocs.push({ id:d.id, ...d.data() }));
              // Filter previous month snapshot docs
              const prevSnapshots = prevDocs.filter(d=> d.field==='newRow').sort((a,b)=> (a.createdAt||'').localeCompare(b.createdAt||''));
              if (prevSnapshots.length) {
                const finalPrev = prevSnapshots[prevSnapshots.length-1];
                const snapObj = finalPrev.snapshot || finalPrev;
                const rawPrevOutstanding = Number(snapObj.outstanding || 0) || 0;
                if (rawPrevOutstanding > 0) prevOutstandingCarry = rawPrevOutstanding;
              }
            }
          }
        }
      } catch (cfErr) {
        console.warn('[ClientBilling] Carry forward lookup failed (non-fatal)', cfErr);
      }
    }
    // Active segment docs (after last snapshot)
    const activePayments = paymentDocs.filter(p => (p.createdAt||'') > lastSnapshotCreatedAt);
    const paymentSum = activePayments.reduce((sum,p)=> sum + Math.abs(Number(p.value||0)||0), 0);
  const latestCreditedEdit = [...creditedEditDocs.filter(p=> (p.createdAt||'') > lastSnapshotCreatedAt)].pop();
  const latestDebitedEdit = [...debitedEditDocs.filter(p=> (p.createdAt||'') > lastSnapshotCreatedAt)].pop();
    const latestMonthlyEdit = [...monthlyEditDocs.filter(p=> (p.createdAt||'') > lastSnapshotCreatedAt)].pop();
    const latestOutstandingOverride = [...outstandingOverrideDocs.filter(p=> (p.createdAt||'') > lastSnapshotCreatedAt)].pop();
    const latestNotes = [...noteDocs.filter(p=> (p.createdAt||'') > lastSnapshotCreatedAt)].pop();
    const activeMonthlyOverride = latestMonthlyEdit? Number(latestMonthlyEdit.value||0)||0 : 0;
    const monthlyApplied = lastHist? 0 : (activeMonthlyOverride>0? activeMonthlyOverride: defaultMonthly);
  // Base outstanding now includes prior month carry if no historical row yet
  const baseOutstanding = lastHist? lastHist.outstanding : (prevOutstandingCarry + monthlyApplied);
    const creditedValue = latestCreditedEdit? Number(latestCreditedEdit.value||0)||0 : paymentSum;
    let activeOutstanding = Math.max(0, baseOutstanding - creditedValue);
    if (latestOutstandingOverride) activeOutstanding = Math.max(0, Number(latestOutstandingOverride.value||latestOutstandingOverride.outstanding||0)||0);
    const activeRow = {
      date: new Date().toISOString().slice(0,10),
      monthly: lastHist? (activeMonthlyOverride>0? activeMonthlyOverride: lastHist.monthly) : (activeMonthlyOverride>0? activeMonthlyOverride: defaultMonthly),
      debited: latestDebitedEdit? Number(latestDebitedEdit.value||0)||0 : 0, // debited is informational only
      credited: creditedValue,
      outstanding: activeOutstanding,
      notes: latestNotes? String(latestNotes.notes||latestNotes.value||'') : '',
      locked:false
    };
    const rows = hist.concat([activeRow]);
    const fmt = (n)=> `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`; 
    const html = rows.map((r,i)=> {
      const editable = i===rows.length-1; // only active row
      const cell = (cIdx,text,raw,field,allow)=>(`<td class="px-3 py-2 grid-cell ${allow?'':'readonly'}" data-row="${i}" data-col="${cIdx}" ${field?`data-field='${field}'`:''} data-raw="${raw}">${text}</td>`);
      return `<tr class="${r.locked?'opacity-95':''}">
        ${cell(0,r.date,r.date,'',false)}
        ${cell(1,'0','0','',false)}
        ${cell(2,r.monthly?fmt(r.monthly):'-',String(r.monthly||0),'monthly',editable)}
        ${cell(3,fmt(r.debited),String(r.debited||0),'debited',editable)}
        ${cell(4,fmt(r.credited),String(r.credited||0),'credited',editable)}
        ${cell(5,fmt(r.outstanding),String(r.outstanding||0),'',false)}
        ${cell(6,escapeHtml(r.notes||''),r.notes||'','notes',editable)}
      </tr>`;
    }).join('');
    tbody.innerHTML = html;
    tbody.querySelectorAll('td').forEach(td=> td.classList.add('grid-cell'));
    // Minimal grid activation (only editing active row cells)
    setupClientBillingGrid(tbody, rows.map((_,idx)=>({key:`${clientId}|${ym}|${idx}`, clientId})), ym);
  };

  // Override new row handler to write snapshot using active row values only.
  document.addEventListener('click', async (e)=> {
    const btn = e.target.closest && e.target.closest('#clientBillingNewRowBtn');
    if (!btn || btn.disabled) return;
    if (window.__CB_CREATING_SNAPSHOT) return; // prevent double fire
    const monthEl=document.getElementById('clientBillingMonth');
    const clientSel=document.getElementById('clientBillingClient');
    const ym=monthEl?.value||''; const clientId=clientSel?.value||'';
    if (!/^\d{4}-\d{2}$/.test(ym)||!clientId) return;
    // Read last row displayed (active row) for snapshot
    const tbody=document.getElementById('clientBillingTableBody');
    const last=tbody?.querySelector('tr:last-child');
    let monthly=0, debited=0, credited=0, outstanding=0, notes='';
    if (last) {
      const tds=last.querySelectorAll('td');
      monthly = Number((tds[2]?.getAttribute('data-raw'))||0);
      debited = Number((tds[3]?.getAttribute('data-raw'))||0);
      credited = Number((tds[4]?.getAttribute('data-raw'))||0);
      outstanding = Number((tds[5]?.getAttribute('data-raw'))||0);
      notes = tds[6]?.getAttribute('data-raw')||'';
    }
    // If outstanding was zeroed by an inline debited edit bug, recompute expected outstanding (monthly - credited) in simple mode
    if (window.__SIMPLE_BILLING_MODE && monthly>0 && outstanding===0 && credited < monthly) {
      outstanding = Math.max(0, monthly - credited);
    }
    const now=new Date();
    const dateStr=`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
    const docData=cleanData({
      clientId, month:ym, date:dateStr, field:'newRow', value:'', prevValue:'',
      monthly, debited, credited, outstanding, notes: notes||undefined,
      snapshot:{monthly,debited,credited,outstanding,notes:notes||''},
      createdAt: now.toISOString(), createdBy: auth?.currentUser?.uid||undefined, createdByEmail: auth?.currentUser?.email||undefined
    });
    window.__CB_CREATING_SNAPSHOT = true;
    try { await addDoc(collection(db,'clientTransactions'), docData); showToast && showToast('Snapshot created','success'); } catch(err){ console.warn('snapshot create failed',err); showToast && showToast('Failed to create snapshot','error'); }
    window.__CB_CREATING_SNAPSHOT = false;
    try { renderClientTransactions(); } catch {}
  });

  // Override edit commit to write minimal field doc (no recompute chain)
  const __origCommit = typeof __cb_commitEdit === 'function'? __cb_commitEdit : null;
  window.__cb_commitEdit = async function(save){
    if (!save) { return __origCommit? __origCommit(false):undefined; }
    // Only allow for active row (last row)
    const tbody=document.getElementById('clientBillingTableBody');
    const lastRowIndex = (tbody?.querySelectorAll('tr')||[]).length - 1;
    if (__clientBillingEditing && __clientBillingEditing.r !== lastRowIndex) { return __origCommit? __origCommit(save):undefined; }
    if (!__clientBillingEditing) return;
    const { c, cell, field } = __clientBillingEditing;
    const clientId=document.getElementById('clientBillingClient')?.value||'';
    const ym=document.getElementById('clientBillingMonth')?.value||'';
    if (!clientId || !/^\d{4}-\d{2}$/.test(ym) || !field) return;
    const rawVal = cell.textContent||'';
    let numVal = 0; if (['monthly','debited','credited','outstanding'].includes(field)) numVal = Number(rawVal.replace(/[^0-9.\-]/g,''))||0;
    const now = new Date();
    const payload = cleanData({
      clientId, month: ym, date: now.toISOString().slice(0,10), field, value: ['monthly','debited','credited','outstanding'].includes(field)? numVal : rawVal,
      createdAt: now.toISOString(), createdBy: auth?.currentUser?.uid||undefined, createdByEmail: auth?.currentUser?.email||undefined
    });
    try { await addDoc(collection(db,'clientTransactions'), payload); } catch(err){ console.warn('simple edit doc failed', err); }
    // Re-render using new simple pipeline
    try { renderClientTransactions(); } catch {}
  };
}

let employees = [];
let temporaryEmployees = [];
let vehicles = [];
// Clients and Assignments state moved into modules; keep thin getters
// Sorting is handled by modules (employees/temporary/vehicles)
let deleteEmployeeId = null;
let vehicleIstimaraURL = null; // Store uploaded Istimara PDF URL
let currentSearch = '';
let currentDepartmentFilter = '';
// Removed per UX decision: always show terminated with visual cue
let unsubscribeEmployees = null;
let unsubscribeTemporary = null;
let unsubscribeVehicles = null;
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
let __fundOpening = 0; // Legacy compatibility (no longer drives display in fixed mode)
let __fundBalance = 0; // Authoritative fund balance (fixed mode)
const FUND_MODE_FIXED = true; // If true, ignore opening+net model and use incremental balance
let __seenFlows = new Set(); // Track processed cashflow IDs in fixed mode
let __fundPrevTotal = null; // For anomaly detection (sign flips without new flows)
let __fundPrevFlowHash = null; // Track flow ids signature to detect real changes
let __fundSignCorrectionApplied = false; // Avoid repeated corrections in one session
const __FUND_LS_KEY = 'fund_prev_snapshot_v1';
let __unsubFundStats = null;

// =====================
// Notifications (expiring documents)
// =====================
let __notifications = []; // { id, type:'expiry', message, employeeId, which }
let __notifPanelOpen = false;
let __notifPanelPortalled = false;
let __notifRepositionHandlerAttached = false;
let __notifUserInteractionEnabled = false; // becomes true after first trusted user action

function __injectNotificationStylesOnce() {
  if (document.getElementById('notificationsStyles')) return;
  const style = document.createElement('style');
  style.id = 'notificationsStyles';
  style.textContent = `
    .notifications-wrapper { position: relative; }
    /* Base panel styles (will be portalled to body to avoid clipping) */
  #notificationsPanel { position: fixed; width: 340px; max-height: 480px; background:#ffffff; border:1px solid #e5e7eb; border-radius:12px; box-shadow:0 12px 42px rgba(15,23,42,0.22); padding:10px 10px 12px; z-index:120500; flex-direction:column; gap:8px; overflow-y:auto; overscroll-behavior:contain; -webkit-overflow-scrolling:touch; }
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
    div.className = `notif-item ${n.type || 'info'}`;
    
    // Determine icon based on type or custom icon
    let iconHtml;
    if (n.icon) {
      iconHtml = `<i class="fas ${n.icon}"></i>`;
    } else if (n.type === 'expiry' || n.type === 'warning') {
      iconHtml = '<i class="fas fa-exclamation-triangle"></i>';
    } else {
      iconHtml = '<i class="fas fa-info-circle"></i>';
    }
    
    // Build message and action link
    let messageHtml = n.message.replace(/(Qatar ID|Passport|Registration)/g, '<strong>$1</strong>');
    let actionHtml = '';
    
    if (n.onClick) {
      // For notifications with custom onClick (like vehicles)
      actionHtml = `<div class="notif-meta"><a href="#" class="notif-emp-link notif-custom-link" data-notif-id="${n.id}">View</a></div>`;
    } else if (n.which && n.employeeId) {
      // For employee notifications
      actionHtml = `<div class="notif-meta"><a data-notif-view href="#" class="notif-emp-link" data-which="${n.which}" data-eid="${n.employeeId}">View</a></div>`;
    }
    
    div.innerHTML = `
      <div class="notif-icon" aria-hidden="true">${iconHtml}</div>
      <div class="notif-msg">
        ${messageHtml}${actionHtml}
      </div>
      <button class="notif-dismiss" data-notif-dismiss="${n.id}" title="Dismiss" aria-label="Dismiss">✕</button>`;
    listEl.appendChild(div);
  });
  
  // Attach click handlers for custom onClick notifications
  listEl.querySelectorAll('.notif-custom-link').forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const notifId = link.getAttribute('data-notif-id');
      const notif = __notifications.find(n => n.id === notifId);
      if (notif && notif.onClick) {
        notif.onClick();
        // Close notification panel
        const panel = document.getElementById('notificationsPanel');
        if (panel) panel.classList.add('hidden');
      }
    });
  });
}

function __positionNotificationsPanel() {
  const btn = document.getElementById('notificationsBtn');
  const panel = document.getElementById('notificationsPanel');
  if (!btn || !panel || panel.classList.contains('hidden')) return;
  const rect = btn.getBoundingClientRect();
  const panelWidth = panel.offsetWidth || 340;
  const margin = 8;
  let top = rect.bottom + margin;
  let left = rect.right - panelWidth;
  if (left < 8) left = 8;
  const panelHeight = panel.offsetHeight || 0;
  const maxTop = window.innerHeight - (panelHeight + 8);
  if (top > maxTop) top = Math.max(8, rect.top - panelHeight - margin);
  panel.style.top = `${Math.max(8, top)}px`;
  panel.style.left = `${Math.round(left)}px`;
}

function __attachNotifRepositionHandlers() {
  if (__notifRepositionHandlerAttached) return;
  __notifRepositionHandlerAttached = true;
  window.addEventListener('resize', __positionNotificationsPanel, { passive: true });
  window.addEventListener('scroll', __positionNotificationsPanel, { passive: true });
}

function __toggleNotificationsPanel(force) {
  const btn = document.getElementById('notificationsBtn');
  let panel = document.getElementById('notificationsPanel');
  if (!btn || !panel) return;
  if (!__notifUserInteractionEnabled) return; // prevent auto-open before any user action
  const wantOpen = force !== undefined ? force : !__notifPanelOpen;
  __notifPanelOpen = wantOpen;
  if (wantOpen) {
    // Portal to body if not already
    if (!__notifPanelPortalled) {
      document.body.appendChild(panel); // move out of potentially clipped container
      __notifPanelPortalled = true;
    }
    panel.classList.remove('hidden');
    panel.classList.add('notif-anim');
    btn.setAttribute('aria-expanded','true');
    __attachNotifRepositionHandlers();
    // Allow layout to settle then position
    requestAnimationFrame(() => { __positionNotificationsPanel(); setTimeout(()=> panel.focus(), 0); });
  } else {
    panel.classList.add('hidden');
    btn.setAttribute('aria-expanded','false');
  }
}

document.addEventListener('click', (e) => {
  if (e.isTrusted) __notifUserInteractionEnabled = true; // enable after real user click
  const btn = document.getElementById('notificationsBtn');
  const panel = document.getElementById('notificationsPanel');
  if (!btn || !panel) return;
  if (e.target === btn || btn.contains(e.target)) {
    e.preventDefault();
    __toggleNotificationsPanel();
    return;
  }
  // Header clear/dismiss button
  const clearBtn = document.getElementById('notificationsClearBtn');
  if (clearBtn && (e.target === clearBtn || clearBtn.contains(e.target))) {
    e.preventDefault();
    // Option A: just close the panel; Option B: also clear current list. Prefer closing only to avoid losing alerts accidentally.
    __toggleNotificationsPanel(false);
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

// Global delegated handler for cashflow delete buttons to ensure Summary/Overview update immediately
document.addEventListener('click', async (e) => {
  const btn = e.target && e.target.closest && e.target.closest('button[data-delete-cashflow]');
  if (!btn) return;
  e.preventDefault();
  const id = btn.getAttribute('data-delete-cashflow');
  if (!id) return;
  try {
    await deleteCashflowWithLinks(id);
    showToast && showToast('Transaction deleted', 'success');
  } catch (err) {
    console.warn('Delete cashflow failed', err);
    showToast && showToast('Failed to delete transaction', 'error');
  }
});

document.addEventListener('keydown', (e) => {
  if (e.isTrusted) __notifUserInteractionEnabled = true;
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

// Vehicle expiry notification system
function __rebuildVehicleExpiryNotifications() {
  if (!vehicles || vehicles.length === 0) {
    console.log('[VehicleExpiry] No vehicles to check');
    return;
  }
  
  console.log(`[VehicleExpiry] Checking ${vehicles.length} vehicles for expiry`);
  
  const today = new Date();
  const thirtyDaysFromNow = new Date(today);
  thirtyDaysFromNow.setDate(today.getDate() + 30);
  const validIds = new Set();
  
  vehicles.forEach(vehicle => {
    let tooltipParts = [];
    let status = 'valid';
    
    const checkExpiry = (val, label) => {
      if (!val) {
        console.log(`[VehicleExpiry] ${vehicle.brand} ${vehicle.model}: No ${label} date found`);
        return;
      }
      const d = new Date(val);
      if (isNaN(d.getTime())) {
        console.log(`[VehicleExpiry] ${vehicle.brand} ${vehicle.model}: Invalid ${label} date: ${val}`);
        return;
      }
      const daysUntil = Math.ceil((d - today) / (1000 * 60 * 60 * 24));
      
      console.log(`[VehicleExpiry] ${vehicle.brand} ${vehicle.model}: ${label} expires on ${val}, ${daysUntil} days from now (threshold: 30 days)`);
      
      if (d <= thirtyDaysFromNow) {
        status = 'expiring';
        const msg = daysUntil < 0 
          ? `${label} expired ${Math.abs(daysUntil)} day${Math.abs(daysUntil) === 1 ? '' : 's'} ago`
          : `${label} expires in ${daysUntil} day${daysUntil === 1 ? '' : 's'}`;
        tooltipParts.push(msg);
        console.log(`[VehicleExpiry] ALERT: ${msg}`);
      }
    };
    
    // Check expiry date (registration expiry)
    checkExpiry(vehicle.expiryDate, 'Registration');
    
    if (status === 'expiring') {
      const id = `expiry-vehicle-${vehicle.id}`;
      validIds.add(id);
      console.log(`[VehicleExpiry] Adding notification for ${vehicle.brand} ${vehicle.model}: ${tooltipParts.join('; ')}`);
      __addOrUpdateVehicleExpiryNotification(vehicle, tooltipParts.join('; '));
    }
  });
  
  console.log(`[VehicleExpiry] Found ${validIds.size} vehicles with expiring registrations`);
  __pruneVehicleExpiryNotifications(validIds);
  __renderNotifications();
}

function __addOrUpdateVehicleExpiryNotification(vehicle, tooltip) {
  if (!__notifications) {
    console.error('[VehicleExpiry] __notifications not initialized!');
    return;
  }
  
  const id = `expiry-vehicle-${vehicle.id}`;
  const existing = __notifications.find(n => n.id === id);
  const title = `Vehicle Expiry Alert: ${vehicle.brand} ${vehicle.model}`;
  const message = `${vehicle.vehicleNumber} - ${tooltip}`;
  
  console.log(`[VehicleExpiry] ${existing ? 'Updating' : 'Creating'} notification:`, { id, title, message });
  
  if (existing) {
    existing.title = title;
    existing.message = message;
    existing.tooltip = tooltip;
  } else {
    __notifications.push({
      id,
      title,
      message,
      tooltip,
      type: 'warning',
      icon: 'fa-car',
      timestamp: Date.now(),
      read: false,
      onClick: () => {
        // Open vehicles tab and view vehicle
        setActiveSection('contractors');
        setTimeout(() => {
          if (typeof window.viewVehicle === 'function') {
            window.viewVehicle(vehicle.id);
          }
        }, 300);
      }
    });
    console.log(`[VehicleExpiry] Notification added. Total notifications: ${__notifications.length}`);
  }
}

function __pruneVehicleExpiryNotifications(validIds) {
  __notifications = __notifications.filter(n => {
    if (n.id.startsWith('expiry-vehicle-')) {
      return validIds.has(n.id);
    }
    return true;
  });
}

__injectNotificationStylesOnce();

// Force closed state on DOM ready and delay enabling toggle to block any premature scripted interaction
document.addEventListener('DOMContentLoaded', () => {
  const panel = document.getElementById('notificationsPanel');
  const btn = document.getElementById('notificationsBtn');
  if (panel) panel.classList.add('hidden');
  if (btn) btn.setAttribute('aria-expanded','false');
  setTimeout(()=> { __notifUserInteractionEnabled = true; }, 150);
});

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
function __grid_fmtCurrency(n) { return `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`; }
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

  // Global escapeHtml fallback (used across multiple dynamic option builders)
  if (typeof window.escapeHtml !== 'function') {
    window.escapeHtml = function escapeHtml(str) {
      const s = String(str ?? '');
      return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    };
  }

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
    // Ensure a primary Fund (Asset) account exists alongside Cash; create it if missing.
    try { ensureAssetAccount('fund', 'Fund'); } catch {}
    try { ensureAssetAccount('cash', 'Cash'); } catch {}
    // If Overview tab is active, refresh it to reflect account name changes
    try {
      const ovBtn = document.getElementById('accountsSubTabOverviewBtn');
      const ovTab = document.getElementById('accountsTabOverview');
      if ((ovBtn && ovBtn.classList.contains('border-b-2')) || (ovTab && ovTab.style.display !== 'none')) {
        renderAccountsOverview();
      }
      const sumTab = document.getElementById('accountsTabSummary');
      if (sumTab && sumTab.style.display !== 'none') { try { renderAccountsSummary(); } catch {} }
    } catch {}
  });
  // Keep a cashflow shadow array to compute fund total
  window.addEventListener('cashflow:updated', (e) => {
    try {
      const arr = Array.isArray(e?.detail) ? e.detail.slice() : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll.slice() : []);
      __cashflowShadow = arr;
    } catch { __cashflowShadow = Array.isArray(window.__cashflowAll)? window.__cashflowAll.slice() : []; }
    try { updateAccountsFundCard(); } catch {}
    // Update Overview metrics and Transactions filters/live table if those tabs are visible
    try {
      const ovTab = document.getElementById('accountsTabOverview');
      if (ovTab && ovTab.style.display !== 'none') renderAccountsOverview();
      const sumTab = document.getElementById('accountsTabSummary');
      if (sumTab && sumTab.style.display !== 'none') renderAccountsSummary();
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
  loadVehiclesRealtime();
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
    // Provide accounts to cashflow for dropdowns and driver population
    initCashflow({
      db, collection, query, onSnapshot, addDoc, orderBy, where, showToast, cleanData,
      getAccounts: () => __getLocalAccounts(),
      ensureAssetAccount,
      getEmployees: () => employees,
      getTemporaryEmployees: () => temporaryEmployees,
      serverTimestamp
    });
    subscribeCashflow();
  } catch (e) { console.warn('Cashflow init failed', e); }
  // Start dedicated fund watchers (accounts + cashflows snapshots) to keep fund card in sync
  try { subscribeFundCardSnapshots(); } catch (e) { console.warn('Fund snapshot subscribe failed', e); }
  try {
    // Provide accounts to ledger as well
  initLedger({ db, collection, query, onSnapshot, orderBy, where, showToast, cleanData, getAccounts: () => __getLocalAccounts(), deleteDoc, doc });
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
      if (unsubscribeVehicles) {
        unsubscribeVehicles();
        unsubscribeVehicles = null;
      }
      // Cleanup fund snapshot listeners
      try { if (__unsubFundAccounts) { __unsubFundAccounts(); __unsubFundAccounts = null; } } catch {}
      try { if (__unsubFundCashflows) { __unsubFundCashflows(); __unsubFundCashflows = null; } } catch {}
      try { if (__unsubFundStats) { __unsubFundStats(); __unsubFundStats = null; } } catch {}
    employees = [];
    temporaryEmployees = [];
    vehicles = [];
      renderEmployeeTable();
      renderTemporaryTable();
      renderVehicleTable();
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

// Listen for newly recorded payslip cashflow to optionally notify user and defer highlight clearing until viewed
try {
  window.addEventListener('payslip:cashflow-recorded', (e) => {
    try {
      // Keep highlight id already set in appendPayslipCashflow; just notify.
      showToast && showToast('Payslip recorded. View it in Accounts → Overview.', 'success');
      // If user is already on Accounts Overview, force immediate refresh for visibility
      const accSection = document.getElementById('accountsSection');
      const ovTab = document.getElementById('accountsTabOverview');
      if (accSection && accSection.style.display !== 'none' && ovTab && ovTab.style.display !== 'none') {
        try { renderAccountsOverview && renderAccountsOverview(); } catch {}
      }
    } catch {}
  });
} catch {}

// Real-time listener for employees
function loadEmployeesRealtime() {
  const q = query(collection(db, "employees"), orderBy("name"));
  if (unsubscribeEmployees) { unsubscribeEmployees(); }
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
  if (unsubscribeTemporary) { unsubscribeTemporary(); }
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

// Real-time listener for vehicles
function loadVehiclesRealtime() {
  const q = query(collection(db, "vehicles"), orderBy("model"));
  if (unsubscribeVehicles) { unsubscribeVehicles(); }
  unsubscribeVehicles = onSnapshot(
    q,
    (snapshot) => {
  vehicles = snapshot.docs.map((d) => ({ id: d.id, ...d.data() }));
  console.log('[Vehicles] Loaded vehicles:', vehicles.length);
  if (vehicles.length > 0) {
    console.log('[Vehicles] First vehicle data:', vehicles[0]);
  }
  renderVehicleTable();
  __rebuildExpiryNotifications();
  __rebuildVehicleExpiryNotifications();
      // Keep payroll in sync as data arrives
      renderPayrollTable();
      try { ensureCurrentMonthBalances(); } catch {}
    },
    (error) => {
      console.error("Error loading vehicles: ", error);
      showToast('Error loading vehicles', 'error');
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

// Reset employee form fields and related status indicators (used after save or when opening add modal)
function clearForm() {
  const form = document.getElementById('employeeForm');
  if (!form) return;
  try { form.reset(); } catch {}
  // Explicitly clear hidden id (reset may not always clear hidden inputs in some browsers)
  const idEl = document.getElementById('employeeId');
  if (idEl) idEl.value = '';
  // Clear status text elements for uploads
  ['profileImageStatus','qidPdfStatus','passportPdfStatus'].forEach(id => {
    const el = document.getElementById(id); if (el) el.textContent = '';
  });
  // Clear file inputs to remove previously selected files
  ['profileImage','qidPdf','passportPdf'].forEach(id => { const inp = document.getElementById(id); if (inp) inp.value = ''; });
  // Remove any validation/error indicators
  try {
    form.querySelectorAll('.error, .input-error').forEach(n => n.classList.remove('error','input-error'));
  } catch {}
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

// Add or update contractor in Firestore
async function saveContractor(contractor, isNew = false) {
  try {
    if (contractor.id) {
      const contractorRef = doc(db, "contractors", contractor.id);
      const { id, ...updateData } = contractor;
      await updateDoc(contractorRef, cleanData(updateData));
      showToast(isNew ? 'Contractor added successfully' : 'Contractor updated successfully', 'success');
    } else {
      const { id, ...newContractor } = contractor;
  await addDoc(collection(db, "contractors"), cleanData(newContractor));
  showToast('Contractor added successfully', 'success');
    }
    clearForm();
    if (document.getElementById('employeeModal')?.classList.contains('show')) {
      closeEmployeeModal();
    }
  } catch (error) {
    console.error("Error saving contractor: ", error);
    showToast('Error saving contractor', 'error');
  }
}

// Delete contractor from Firestore
async function deleteContractorFromDB(contractorId) {
  try {
    await deleteDoc(doc(db, "contractors", contractorId));
    showToast('Contractor deleted successfully', 'success');
  } catch (error) {
    console.error("Error deleting contractor: ", error);
    showToast('Error deleting contractor', 'error');
  }
}

// Setup event listeners
function setupEventListeners() {
  const formEl = document.getElementById('employeeForm');
  if (formEl) formEl.addEventListener('submit', handleFormSubmit);
  const vehicleFormEl = document.getElementById('vehicleForm');
  if (vehicleFormEl) vehicleFormEl.addEventListener('submit', handleVehicleFormSubmit);
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
  const openContractorModalBtn = document.getElementById('openContractorModalBtn');
  if (openContractorModalBtn) openContractorModalBtn.addEventListener('click', () => openEmployeeModal('contractors', 'add'));
  const openVehicleModalBtn = document.getElementById('openVehicleModalBtn');
  if (openVehicleModalBtn) openVehicleModalBtn.addEventListener('click', openVehicleModal);

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
    // Silenced: fund button intentionally not present in current UI
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
    // Ensure the floating open button only shows when sidebar is collapsed
    try {
      const openBtn = document.getElementById('openSidebarBtn');
      if (openBtn) {
        if (hidden) {
          openBtn.classList.remove('hidden');
        } else {
          if (!openBtn.classList.contains('hidden')) openBtn.classList.add('hidden');
        }
      }
    } catch {}
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
window.openClientPaymentModal = async function() {
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
    const selYear = Number(ym.slice(0,4));
    const selMonth = Number(ym.slice(5,7));
    const now = new Date();
    const isCurrentMonth = (now.getFullYear() === selYear && (now.getMonth()+1) === selMonth);
    // If selected month is current month, use today's date; else use first day of selected month
    const dateStr = isCurrentMonth ? `${selYear}-${String(selMonth).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}` : `${ym}-01`;
    const mm = new Date(selYear, selMonth - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' });
    const nameEl = document.getElementById('clientPaymentClientName');
    const monthLbl = document.getElementById('clientPaymentMonth');
    const dateInput = document.getElementById('cpDate');
    if (nameEl) nameEl.textContent = name;
    if (monthLbl) monthLbl.textContent = mm;
    if (dateInput) {
      dateInput.removeAttribute('readonly');
      // Optional: constrain selection within the chosen month
      const first = `${ym}-01`;
      const lastDay = new Date(selYear, selMonth, 0).getDate();
      const last = `${ym}-${String(lastDay).padStart(2,'0')}`;
      dateInput.setAttribute('min', first);
      dateInput.setAttribute('max', last);
      dateInput.value = dateStr;
    }
  } catch {}
  // Populate account: force Fund (Asset) only
  try {
    const selAcc = document.getElementById('cpAccount');
    const fundId = await ensureAssetAccount('fund','Fund');
    if (selAcc) {
      selAcc.innerHTML = `<option value="${fundId}">Fund</option>`;
      selAcc.value = fundId;
      selAcc.disabled = true; // lock to Fund per requirement
    }
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
    // Always target Fund (Asset) for client payments
    const fundId = await ensureAssetAccount('fund','Fund');
    const accountId = fundId || (document.getElementById('cpAccount')?.value || '');
    const amount = Math.abs(Number(document.getElementById('cpAmount')?.value || 0)) || 0;
    const notes = document.getElementById('cpNotes')?.value || '';
    const sel = document.getElementById('clientBillingClient');
    const monthEl = document.getElementById('clientBillingMonth');
    const clientId = sel?.value || '';
    const ym = monthEl?.value || '';
    if (!date || !accountId || !(amount>0) || !clientId || !/^\d{4}-\d{2}$/.test(ym)) { showToast('Fill all fields correctly', 'warning'); return; }
    try {
      // Save as cashflow IN (payment received) and tag with clientId for reporting
      // Resolve client name for description tagging
      let clientNameTag = '';
      try {
        const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
        const cli = list.find(c => c.id === clientId);
        clientNameTag = cli?.name || cli?.company || '';
      } catch {}
      const inRef = await addDoc(collection(db, 'cashflows'), cleanData({
        date,
        type: 'in',
        accountId,
        accountName: 'Fund',
        amount,
        category: 'Client Payment',
        clientId,
        month: ym,
        notes: notes ? `${notes}${clientNameTag?` | ${clientNameTag}`:''}` : (clientNameTag?`Client: ${clientNameTag}`:undefined),
        createdAt: new Date().toISOString(),
        createdBy: auth?.currentUser?.uid || undefined,
        createdByEmail: auth?.currentUser?.email || undefined
      }));
      // Removed previous automatic OUT offset (Fund Adjustment (Payment Applied)). Payment now records only the IN entry.
      // Update local grid cache credited for this (clientId|ym)
      // Remove local credited additive update (caused double counting with payment doc). Rendering will rebuild from docs.
      try {
        const key = `${clientId}|${ym}`;
        const cur = (__clientBillingGridCache[key] ||= {});
        cur.__pendingPaymentApplied = true; // marker only (debug)
      } catch {}
      showToast('Payment recorded', 'success');
      closeClientPaymentModal();
      // Append clientTransactions log for payment
      try {
        const now = new Date();
        const dateStr = date; // use chosen date for payment
        const key = `${clientId}|${ym}`;
        const bucket = (__clientBillingGridCache[key] ||= {});
        const outstandingCellPrev = 0; // will recompute after render
        // Payment log should ONLY record the payment value; do not persist snapshot fields that can corrupt historical rows.
        const docData = cleanData({
          clientId,
          month: ym,
          date: dateStr,
          field: 'payment',
          value: amount,
          notes: notes || undefined,
          createdAt: now.toISOString(),
          createdBy: auth?.currentUser?.uid || undefined,
          createdByEmail: auth?.currentUser?.email || undefined
        });
        try { await addDoc(collection(db,'clientTransactions'), docData); } catch(e){ console.warn('clientTransactions log (payment) failed', e);}      } catch {}
      // Re-render grid to reflect credited/outstanding
      try { renderClientTransactions(); } catch {}
    } catch (err) {
      console.error('Client payment save failed', err);
      showToast('Failed to save payment', 'error');
    }
  }
});

// New Row button enable & handler
document.addEventListener('click', async (e) => {
  const btn = e.target.closest && e.target.closest('#clientBillingNewRowBtn');
  if (!btn) return;
  if (btn.disabled) return;
  if (window.__SIMPLE_BILLING_MODE || window.__CB_CREATING_SNAPSHOT) return; // skip legacy duplication
  try {
    const monthEl = document.getElementById('clientBillingMonth');
    const clientSel = document.getElementById('clientBillingClient');
    const ym = monthEl?.value || '';
    const clientId = clientSel?.value || '';
    if (!/^\d{4}-\d{2}$/.test(ym) || !clientId) return;
    // Derive current state purely from the last rendered grid row to avoid stale zero from older docs
    let latestMonthly = 0, latestOutstanding = 0, latestDebited=0, latestCredited=0, latestNotes='';
    try {
      const tbody = document.getElementById('clientBillingTableBody');
      const row = tbody?.querySelector('tr:last-child');
      if (row) {
        const tds = row.querySelectorAll('td');
        latestMonthly = __cb_parseNumber(tds[2]?.getAttribute('data-raw')||'0');
        latestDebited = __cb_parseNumber(tds[3]?.getAttribute('data-raw')||'0');
        latestCredited = __cb_parseNumber(tds[4]?.getAttribute('data-raw')||'0');
        latestOutstanding = __cb_parseNumber(tds[5]?.getAttribute('data-raw')||'0');
        // Notes cell may not always be present depending on columns; search for a td with data-field=notes in the row.
        const notesCell = row.querySelector('td[data-field="notes"]');
        if (notesCell) latestNotes = notesCell.textContent||'';
      }
    } catch {}
    console.log('[ClientBilling] New Row baseline derived', {latestMonthly, latestDebited, latestCredited, latestOutstanding, latestNotes});
    // If grid empty (first row), compute outstanding as monthly (to be set after render). We keep it 0 here; UI logic will handle first baseline.
    const now = new Date();
    const dateStr = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
    // New row resets debited & credited to 0 but its outstanding should be the CURRENT outstanding (after prior calculations)
    // Capture full snapshot of current (latest) row so it remains immutable historically
    const newDoc = cleanData({
      clientId,
      month: ym,
      date: dateStr,
      field: 'newRow',
      value: '',
      prevValue: '',
      monthly: latestMonthly,
      debited: 0, // fresh segment starts at 0 debited/credited
      credited: 0,
      outstanding: latestOutstanding, // carry forward current outstanding correctly
      notes: latestNotes || undefined,
      // Snapshot should freeze PRIOR segment final values, not the reset zeros.
      snapshot: { monthly: latestMonthly, debited: latestDebited, credited: latestCredited, outstanding: latestOutstanding, notes: latestNotes || '' }, // If last row had a manual outstanding override, latestOutstanding already reflects it and becomes the new segment base.
      createdAt: now.toISOString(),
      createdBy: auth?.currentUser?.uid || undefined,
      createdByEmail: auth?.currentUser?.email || undefined
    });
    try {
      await addDoc(collection(db,'clientTransactions'), newDoc);
    } catch (err) {
      console.warn('clientTransactions newRow with snapshot failed, retrying without snapshot', err);
      try {
        const clone = { ...newDoc };
        delete clone.snapshot;
        await addDoc(collection(db,'clientTransactions'), clone);
      } catch (err2) {
        console.error('Failed to add new client row after retry', err2);
        showToast && showToast('Failed to add new row (permissions)', 'error');
        return;
      }
    }
    showToast && showToast('New row added','success');
    renderClientTransactions();
  } catch (err) {
    console.warn('Failed to add new client row', err);
    showToast && showToast('Failed to add row','error');
  }
});

// Enable/disable New Row + Payment buttons based on selection
document.addEventListener('change', (e) => {
  if (e.target && (e.target.id === 'clientBillingClient' || e.target.id === 'clientBillingMonth')) {
    const clientId = document.getElementById('clientBillingClient')?.value || '';
    const ym = document.getElementById('clientBillingMonth')?.value || '';
    const valid = clientId && /^\d{4}-\d{2}$/.test(ym);
    const addBtn = document.getElementById('clientBillingNewRowBtn');
    const payBtn = document.getElementById('openClientPaymentBtn');
    if (addBtn) addBtn.disabled = !valid;
    if (payBtn) payBtn.disabled = !valid;
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
  const addContractorBtn = document.getElementById('openContractorModalBtn');
  const addVehicleBtn = document.getElementById('openVehicleModalBtn');
  const addClientBtn = document.getElementById('openClientModalBtn');
  if (addBtn && addTempBtn) {
    if (key === 'temporary') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = '';
      if (addContractorBtn) addContractorBtn.style.display = 'none';
      if (addVehicleBtn) addVehicleBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = 'none';
    } else if (key === 'employees') {
      addBtn.style.display = '';
      addTempBtn.style.display = 'none';
      if (addContractorBtn) addContractorBtn.style.display = 'none';
      if (addVehicleBtn) addVehicleBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = 'none';
    } else if (key === 'contractors') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
      if (addContractorBtn) addContractorBtn.style.display = 'none';
      if (addVehicleBtn) addVehicleBtn.style.display = '';
      if (addClientBtn) addClientBtn.style.display = 'none';
    } else if (key === 'clients') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
      if (addContractorBtn) addContractorBtn.style.display = 'none';
      if (addVehicleBtn) addVehicleBtn.style.display = 'none';
      if (addClientBtn) addClientBtn.style.display = '';
    } else {
      // On dashboard, hide all
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
      if (addContractorBtn) addContractorBtn.style.display = 'none';
      if (addVehicleBtn) addVehicleBtn.style.display = 'none';
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
  const btn = e.target && e.target.closest && e.target.closest('#accountsSubTabOverviewBtn, #accountsSubTabSummaryBtn, #accountsSubTabLedgerBtn, #accountsSubTabSettingsBtn');
  if (!btn) return;
  let which = 'overview';
  if (btn.id === 'accountsSubTabSummaryBtn') which = 'summary';
  if (btn.id === 'accountsSubTabLedgerBtn') which = 'ledger';
  if (btn.id === 'accountsSubTabSettingsBtn') which = 'settings';
  setAccountsSubTab(which);
});

function setAccountsSubTab(which) {
  const tabs = {
    overview: document.getElementById('accountsTabOverview'),
    summary: document.getElementById('accountsTabSummary'),
    ledger: document.getElementById('accountsTabLedger'),
    settings: document.getElementById('accountsTabSettings'),
  };
  const btns = {
    overview: document.getElementById('accountsSubTabOverviewBtn'),
    summary: document.getElementById('accountsSubTabSummaryBtn'),
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
  } else if (which === 'summary') {
    try { renderAccountsSummary(); } catch {}
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

// Summary Tab logic
function renderAccountsSummary() {
  const monthEl = document.getElementById('summaryMonth');
  const tbody = document.getElementById('accountsSummaryTbody');
  const empty = document.getElementById('accountsSummaryEmpty');
  if (!tbody) return;
  // Default month = current month
  const now = new Date();
  const defaultYm = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  if (monthEl && !monthEl.value) monthEl.value = defaultYm;
  const ym = (monthEl?.value || defaultYm);
  const flows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll : (__fundCashflowsCache||[]);
  const accs = __getLocalAccounts() || [];
  const assetIds = accs.filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
  const isAsset = (id) => assetIds.length === 0 || !id || assetIds.includes(id);
  // Group by accountId
  const byAcc = new Map();
  for (const t of flows) {
    if (!t) continue; if (!isAsset(t.accountId)) continue;
    const d = String(t.date||'');
    const ymOf = d.slice(0,7);
    const typ = String(t.type||'').toLowerCase();
    const amt = Math.abs(Number(t.amount||0))||0;
    const key = t.accountId || `name:${t.accountName||''}`;
    if (!byAcc.has(key)) byAcc.set(key, { id: t.accountId||null, name: t.accountName||'', opening: 0, in:0, out:0 });
    const rec = byAcc.get(key);
    // Determine name if missing from accounts list/cache
    if (!rec.name) rec.name = accs.find(a=>a.id===t.accountId)?.name || t.accountName || '';
    if (ymOf === ym) {
      if (['in','income','credit'].includes(typ)) rec.in += amt; else if (['out','expense','debit'].includes(typ)) rec.out += amt;
    } else if (ymOf < ym) {
      // flows before the month contribute to opening
      if (['in','income','credit'].includes(typ)) rec.opening += amt; else if (['out','expense','debit'].includes(typ)) rec.opening -= amt;
    }
  }
  const rows = Array.from(byAcc.values());
  // Sort by account name
  rows.sort((a,b)=> String(a.name||'').localeCompare(String(b.name||'')));
  if (!rows.length) {
    if (empty) empty.style.display = '';
    tbody.innerHTML = '';
    return;
  }
  if (empty) empty.style.display = 'none';
  const fmt = (n)=> `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  tbody.innerHTML = rows.map(r => {
    const closing = Number(r.opening) + Number(r.in) - Number(r.out);
    return `<tr>
      <td class="px-4 py-2">${escapeHtml(r.name||'')}</td>
      <td class="px-4 py-2 text-right">${fmt(r.opening)}</td>
      <td class="px-4 py-2 text-right">${fmt(r.in)}</td>
      <td class="px-4 py-2 text-right">${fmt(r.out)}</td>
      <td class="px-4 py-2 text-right">${fmt(closing)}</td>
    </tr>`;
  }).join('');

  // Render Driver Petrol Summary for selected month
  try { renderDriverPetrolSummary(ym); } catch (e) { console.warn('Driver petrol summary failed', e); }
}

// Month change and refresh button
document.addEventListener('change', (e) => {
  if (e.target && e.target.id === 'summaryMonth') {
    const tab = document.getElementById('accountsTabSummary');
    if (tab && tab.style.display !== 'none') renderAccountsSummary();
  }
});
document.addEventListener('click', (e) => {
  const btn = e.target && e.target.closest && e.target.closest('#summaryRefreshBtn');
  if (!btn) return;
  const tab = document.getElementById('accountsTabSummary');
  if (tab && tab.style.display !== 'none') renderAccountsSummary();
});

// Driver Petrol Summary logic
function renderDriverPetrolSummary(ym) {
  const body = document.getElementById('driverPetrolSummaryTbody');
  const empty = document.getElementById('driverPetrolSummaryEmpty');
  if (!body) return;
  const flows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll : [];
  const isPetrolCat = (c)=> /^(petrol|fuel)$/i.test(String(c||''));
  const rows = flows.filter(f => (f.type||'').toLowerCase()==='out' && isPetrolCat(f.category) && (f.date||'').slice(0,7)===ym);
  // Group by driverId (fallback bucket for unknown)
  const byDriver = new Map();
  for (const t of rows) {
    const id = t.driverId || '';
    const key = id || `__unknown__`;
    const name = resolveDriverName(t.driverId) || inferDriverNameFromNotes(t.notes) || '—';
    if (!byDriver.has(key)) byDriver.set(key, { id, name, total:0, count:0, txns: [] });
    const rec = byDriver.get(key);
    const amt = Math.abs(Number(t.amount||0))||0;
    rec.total += amt;
    rec.count += 1;
    rec.txns.push({ date: t.date || '', amount: amt, notes: t.notes || '', category: t.category || '' });
  }
  const list = Array.from(byDriver.values());
  list.sort((a,b)=> String(a.name||'').localeCompare(String(b.name||'')));
  const fmt = (n)=> `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  if (!list.length) {
    if (empty) empty.style.display = '';
    body.innerHTML = '';
  } else {
    if (empty) empty.style.display = 'none';
    body.innerHTML = list.map(r => {
      const avg = r.count ? (r.total / r.count) : 0;
      // sort txns by date asc
      const tx = (r.txns||[]).slice().sort((a,b)=> String(a.date||'').localeCompare(String(b.date||'')));
      const details = tx.length
        ? `<div class="text-xs">
             <table class="min-w-full table-fixed text-xs">
               <colgroup><col class="w-[40%]" /><col class="w-[30%]" /><col class="w-[30%]" /></colgroup>
               <thead>
                 <tr class="text-gray-500">
                   <th class="text-left font-medium px-4 py-2">Date</th>
                   <th class="text-left font-medium px-4 py-2">Category</th>
                   <th class="text-right font-medium px-4 py-2">Amount</th>
                 </tr>
               </thead>
               <tbody>
                 ${tx.map(t => `<tr>
                     <td class="px-4 py-1">${escapeHtml(t.date)}</td>
                     <td class="px-4 py-1">${escapeHtml(t.category || '')}</td>
                     <td class="px-4 py-1 text-right">${fmt(t.amount)}</td>
                   </tr>`).join('')}
               </tbody>
             </table>
           </div>`
        : '<div class="text-xs text-gray-500 px-4 py-2">No transactions</div>';
      return `
        <tr data-driver-row role="button" tabindex="0" aria-expanded="false" class="cursor-pointer hover:bg-gray-50 select-none">
          <td class="px-4 py-2"><span class="mr-2" data-caret>▸</span>${escapeHtml(r.name||'')}</td>
          <td class="px-4 py-2 text-right">${fmt(r.total)}</td>
          <td class="px-4 py-2 text-right">${r.count}</td>
          <td class="px-4 py-2 text-right">${fmt(avg)}</td>
        </tr>
        <tr class="driver-details hidden bg-gray-50/50">
          <td colspan="4" class="px-0 py-0">${details}</td>
        </tr>`;
    }).join('');
  }
}

function resolveDriverName(id) {
  if (!id) return '';
  try {
    const everyone = [...(employees||[]), ...(temporaryEmployees||[])];
    const emp = everyone.find(e => e.id === id);
    return emp?.name || '';
  } catch { return ''; }
}
function inferDriverNameFromNotes(notes) {
  return '';
}

document.addEventListener('click', (e) => {
  const btn = e.target && e.target.closest && e.target.closest('#driverPetrolExportBtn');
  if (!btn) return;
  const monthEl = document.getElementById('summaryMonth');
  const ym = monthEl?.value || '';
  exportDriverPetrolCsv(ym);
});

// Toggle expand/collapse for Driver Petrol Summary rows
document.addEventListener('click', (e) => {
  // Only respond to clicks on summary rows within Driver Petrol Summary table
  const row = e.target && e.target.closest && e.target.closest('tr[data-driver-row]');
  if (!row) return;
  const table = row.closest('table');
  if (!table || table.id !== 'driverPetrolSummaryTable') return;
  const details = row.nextElementSibling;
  if (details && details.classList && details.classList.contains('driver-details')) {
    const isHidden = details.classList.toggle('hidden');
    // Toggle caret and aria-expanded
    try {
      row.setAttribute('aria-expanded', String(!isHidden));
      const caret = row.querySelector('[data-caret]');
      if (caret) caret.textContent = isHidden ? '▸' : '▾';
    } catch {}
  }
});

// Keyboard: expand/collapse with Enter/Space
document.addEventListener('keydown', (e) => {
  if (e.key !== 'Enter' && e.key !== ' ') return;
  const row = e.target && e.target.closest && e.target.closest('tr[data-driver-row]');
  if (!row) return;
  const table = row.closest('table');
  if (!table || table.id !== 'driverPetrolSummaryTable') return;
  e.preventDefault();
  const details = row.nextElementSibling;
  if (details && details.classList && details.classList.contains('driver-details')) {
    const isHidden = details.classList.toggle('hidden');
    try {
      row.setAttribute('aria-expanded', String(!isHidden));
      const caret = row.querySelector('[data-caret]');
      if (caret) caret.textContent = isHidden ? '▸' : '▾';
    } catch {}
  }
});

function exportDriverPetrolCsv(ym) {
  try {
    const flows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll : [];
    const isPetrolCat = (c)=> /^(petrol|fuel)$/i.test(String(c||''));
    const rows = flows.filter(f => (f.type||'').toLowerCase()==='out' && isPetrolCat(f.category) && (f.date||'').slice(0,7)===ym);
    const by = new Map();
    for (const t of rows) {
      const id = t.driverId || '';
      const name = resolveDriverName(t.driverId) || inferDriverNameFromNotes(t.notes) || '—';
      const key = id || `name:${name}`;
      if (!by.has(key)) by.set(key, { id, name, total:0, count:0 });
      const rec = by.get(key);
      rec.total += Math.abs(Number(t.amount||0))||0;
      rec.count += 1;
    }
    const list = Array.from(by.values()).sort((a,b)=> String(a.name||'').localeCompare(String(b.name||'')));
    const headers = ['Driver','Total Petrol','Transactions','Avg/Txn','Month'];
    const fmt = (n)=> `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
    const out = [headers.join(',')];
    list.forEach(r => {
      const avg = r.count? (r.total/r.count):0;
      out.push([`"${String(r.name||'').replace(/"/g,'""')}"`, fmt(r.total), r.count, fmt(avg), ym].join(','));
    });
    if (list.length===0) out.push(`"No petrol expenses",,,${ym}`);
    const blob = new Blob([out.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `driver-petrol-summary-${ym||'all'}.csv`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
    URL.revokeObjectURL(url);
  } catch (e) {
    console.error('Driver petrol export failed', e);
    showToast && showToast('Failed to export driver petrol summary','error');
  }
}

// Post-render cleanup: remove legacy Balance column cells if ledger module still outputs them
function __stripLedgerBalanceColumn() {
  try {
    const tbl = document.getElementById('ledgerTableBody')?.closest('table');
    if (!tbl) return;
    const ths = tbl.querySelectorAll('thead th');
    // If header still has 5th 'Balance' remove it
    const hasFive = ths.length === 5;
    const isLegacyBalance = hasFive && /balance/i.test(ths[4].textContent||'');
    if (isLegacyBalance) ths[4].remove();
    // If after header adjustment header has 4 columns but body rows still have 5 cells, strip the 5th
    const headerCount = tbl.querySelectorAll('thead th').length;
    if (headerCount === 4) {
      let changed = false;
      tbl.querySelectorAll('tbody tr').forEach(tr=>{ const cells = tr.querySelectorAll('td'); if (cells.length>4) { cells[cells.length-1].remove(); changed=true; } });
      tbl.querySelectorAll('tfoot tr').forEach(tr=>{ const cells = tr.querySelectorAll('td'); if (cells.length>4) { cells[cells.length-1].remove(); changed=true; } });
      // Adjust any colspan=5 empty messages or opening rows to colspan=4
      if (changed) tbl.querySelectorAll('td[colspan="5"]').forEach(td=> td.setAttribute('colspan','4'));
    }
    // Remove any residual 'Amount' header-like row accidentally carrying a balance cell
    const tbody = tbl.querySelector('tbody');
    if (tbody) {
      const firstRow = tbody.querySelector('tr');
      if (firstRow) {
        const txt = (firstRow.children[0]?.textContent||'').trim().toLowerCase();
        if (txt === 'amount' && firstRow.children.length>4) {
          // Remove extra last cell
            firstRow.children[firstRow.children.length-1].remove();
        }
      }
    }
  } catch {}
}
// Observe ledger table mutations briefly after tab switch to apply cleanup
let __ledgerBalanceObserver = null;
function __initLedgerBalanceStrip() {
  try { if (__ledgerBalanceObserver) return; } catch {}
  try {
    const target = document.getElementById('accountsTabLedger');
    if (!target) return;
    const obs = new MutationObserver(()=> { __stripLedgerBalanceColumn(); });
    obs.observe(target,{ childList:true, subtree:true });
    __ledgerBalanceObserver = obs;
  } catch {}
}
document.addEventListener('DOMContentLoaded', ()=>{ __initLedgerBalanceStrip(); setTimeout(__stripLedgerBalanceColumn,400); });
window.addEventListener('accounts:ledger:rendered', ()=> setTimeout(__stripLedgerBalanceColumn,50));

// Continuous observer for dynamic ledger updates
function __ensureLedgerBodyObserver() {
  try {
    const tbody = document.getElementById('ledgerTableBody');
    if (!tbody || tbody.dataset.balanceObserver) return;
    const obs = new MutationObserver((muts)=> {
      // Debounce via rAF
      requestAnimationFrame(()=> { __stripLedgerBalanceColumn(); });
    });
    obs.observe(tbody,{ childList:true, subtree:true });
    tbody.dataset.balanceObserver = '1';
    __stripLedgerBalanceColumn();
  } catch {}
}
document.addEventListener('DOMContentLoaded', ()=> setTimeout(__ensureLedgerBodyObserver,600));
window.addEventListener('accounts:ledger:rendered', ()=> setTimeout(__ensureLedgerBodyObserver,60));

// =====================
// Ledger Printing
// =====================
document.addEventListener('click', (e) => {
  if (e.target && (e.target.id === 'ledgerPdfBtn' || e.target.closest?.('#ledgerPdfBtn'))) {
    try {
      exportLedgerPdf();
    } catch (err) {
      console.error('Ledger PDF export failed', err);
      showToast && showToast('Failed to export PDF','error');
    }
  }
  if (e.target && (e.target.id === 'ledgerExportBtn' || e.target.closest?.('#ledgerExportBtn'))) {
    try {
      exportCurrentLedgerCsv();
    } catch (err) {
      console.error('Ledger export failed', err);
      showToast && showToast('Failed to export ledger','error');
    }
  }
});

function ensureLedgerPrintArea() {
  let host = document.getElementById('ledgerPrintArea');
  if (!host) {
    host = document.createElement('div');
    host.id = 'ledgerPrintArea';
    host.style.display = 'none';
    host.setAttribute('data-auto-created','1');
    document.body.appendChild(host);
  }
  return host;
}

// Renamed from buildAndPrintLedger to printLedgerPopup to avoid duplicate identifier issues
async function printLedgerPopup() {
  const accSel = document.getElementById('ledgerAccountFilter');
  const monthEl = document.getElementById('ledgerMonth');
  const dayEl = document.getElementById('ledgerDay');
  const tableBody = document.getElementById('ledgerTableBody');
  if (!accSel || !tableBody) return;
  const accountName = accSel.options[accSel.selectedIndex]?.text || 'Ledger';
  const monthVal = monthEl?.value || '';
  let dayVal = dayEl?.value || '';
  if (!dayVal && window.__ledgerCurrentView?.day) dayVal = window.__ledgerCurrentView.day; // fast-click fallback

  // Attempt to ensure latest render (especially after day change)
  try { renderLedgerTable?.(); } catch {}

  const startTs = performance.now();
  const maxWaitMs = 900; // < 1s
  const pollInterval = 90;

  function currentReadyState() {
    const view = window.__ledgerCurrentView;
    const visibleRows = tableBody.querySelectorAll('tr').length;
    let structuredReady = false;
    if (dayVal) {
      structuredReady = !!(view && (view.day === dayVal) && Array.isArray(view.transactions));
    } else {
      structuredReady = !!(view && Array.isArray(view.transactions));
    }
    return { view, visibleRows, structuredReady };
  }

  // Wait (poll) until either the structured view is ready or we have visible rows for the chosen day
  await new Promise(res => {
    (function waitLoop(){
      const {view, visibleRows, structuredReady} = currentReadyState();
      const age = performance.now() - startTs;
      if (structuredReady || visibleRows > 0 || age > maxWaitMs) return res();
      setTimeout(waitLoop, pollInterval);
    })();
  });

  const diag = { phase:'pre-build', dayInputValue: dayEl?.value||'', effectiveDay: dayVal };
  let rowsHtml = '';
  const view = window.__ledgerCurrentView;
  try {
    diag.viewDay = view?.day;
    diag.viewTxnCount = Array.isArray(view?.transactions) ? view.transactions.length : -1;
  } catch {}

  const visibleRowsNow = Array.from(tableBody.querySelectorAll('tr'));
  // Build day view
  if (dayVal) {
    if (visibleRowsNow.length >= 2) {
      rowsHtml = visibleRowsNow.map(tr => tr.outerHTML).join('');
      diag.path = 'clone-visible-day';
    } else if (view && Array.isArray(view.transactions)) {
      diag.path = 'structured-day';
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      let dayTxns = view.transactions.filter(t => t.date === dayVal || t.rawDate === dayVal);
      if (!dayTxns.length && view.day === dayVal) dayTxns = view.transactions.slice();
      let running = view.opening;
      let debitDay = 0, creditDay = 0;
      const out = [];
  out.push(`<tr class="opening-row"><td colspan="2">Opening Balance</td><td></td><td></td></tr>`);
      for (const tx of dayTxns) {
        const isIn = String(tx.type).toLowerCase()==='in';
        const amt = Number(tx.amount||0);
        if (isIn) { debitDay += amt; running += amt; } else { creditDay += amt; running -= amt; }
        const desc = (tx.category ? tx.category : '') + (tx.notes ? (tx.category? ' — ': '') + tx.notes : '');
        out.push(`<tr>
          <td>${escapeHtml(tx.date || dayVal)}</td>
          <td>${escapeHtml(desc||'')}</td>
          <td style="text-align:right;">${isIn?fmt(amt):''}</td>
          <td style="text-align:right;">${!isIn?fmt(amt):''}</td>
        </tr>`);
      }
  out.push(`<tr class="grand-total-row"><td colspan="2">Day Totals</td><td style="text-align:right;">${fmt(debitDay)}</td><td style="text-align:right;">${fmt(creditDay)}</td></tr>`);
      rowsHtml = out.join('');
    } else {
      diag.path = 'no-data-day';
    }
  } else { // Month / All
    if (view && Array.isArray(view.transactions) && view.transactions.length) {
      diag.path = 'structured-month';
      const groups = view.transactions.reduce((m, t) => { (m[t.date] = m[t.date] || []).push(t); return m; }, {});
      const orderedDates = Object.keys(groups).sort();
      let running = view.opening;
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      const rows = [];
  rows.push(`<tr class="opening-row"><td colspan="2">Opening Balance</td><td></td><td></td></tr>`);
      for (const d of orderedDates) {
        const dayTxns = groups[d];
        let debitDay=0, creditDay=0;
        for (const tx of dayTxns) {
          const isIn = String(tx.type).toLowerCase()==='in';
          const amt = Number(tx.amount||0);
          if (isIn) { debitDay += amt; running += amt; } else { creditDay += amt; running -= amt; }
          const desc = (tx.category ? tx.category : '') + (tx.notes ? (tx.category? ' — ': '') + tx.notes : '');
          rows.push(`<tr>
            <td>${escapeHtml(d)}</td>
            <td>${escapeHtml(desc||'')}</td>
            <td style="text-align:right;">${isIn?fmt(amt):''}</td>
            <td style="text-align:right;">${!isIn?fmt(amt):''}</td>
          </tr>`);
        }
  rows.push(`<tr class="day-total-row"><td colspan="2">Day Total ${escapeHtml(d)}</td><td style="text-align:right;">${fmt(debitDay)}</td><td style="text-align:right;">${fmt(creditDay)}</td></tr>`);
      }
  rows.push(`<tr class="grand-total-row"><td colspan="2">Grand Totals</td><td style="text-align:right;">${fmt(view.debitSum)}</td><td style="text-align:right;">${fmt(view.creditSum)}</td></tr>`);
      rowsHtml = rows.join('');
    } else {
      diag.path = 'dom-clone-month-fallback';
      rowsHtml = visibleRowsNow.map(tr => tr.outerHTML).join('');
    }
  }

  if (!rowsHtml) rowsHtml = `<tr><td colspan="4" style="text-align:center;padding:12px;">No entries${dayVal? ' for '+escapeHtml(dayVal):''}</td></tr>`;

  diag.rowsHtmlLength = rowsHtml.length;
  try { console.groupCollapsed('[Ledger Print Diagnostic]'); console.log(diag); if (view) console.log('Structured View', view); console.groupEnd(); } catch {}

  const now = new Date();
  const fmtDate = (d) => d.toLocaleString(undefined, { year:'numeric', month:'short', day:'numeric', hour:'2-digit', minute:'2-digit' });
  const rangeLabel = dayVal ? `Day: ${dayVal}` : (monthVal ? `Month: ${monthVal}` : 'All');
  // Summary removed
  const summary = '';
  const host = ensureLedgerPrintArea();
  host.innerHTML = `
    <div class="lp-header">
      <div>
        <h2>Account Ledger</h2>
      </div>
      <div class="lp-meta">
        <div><strong>Account:</strong> ${escapeHtml(accountName)}</div>
        <div><strong>Range:</strong> ${escapeHtml(rangeLabel)}</div>
        <div><strong>Generated:</strong> ${escapeHtml(fmtDate(now))}</div>
      </div>
    </div>
    ${summary ? `<div class="lp-summary"><strong>Summary:</strong> ${escapeHtml(summary)}</div>` : ''}
    <table>
  <thead><tr><th>Date</th><th>Description</th><th>Debit</th><th>Credit</th></tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table>
    <div class="lp-footer">
      <div>Generated ${escapeHtml(fmtDate(now))}</div>
    </div>
    <div class="lp-watermark">CONFIDENTIAL</div>
  `;
  // Make visible (only becomes visible in print due to CSS visibility rules)
  try { host.style.display = 'block'; } catch {}
  // Force a reflow so the browser acknowledges the new DOM before scheduling frames
  try { void host.offsetHeight; } catch {}
  const doPrint = () => {
    try { window.print(); } catch (e) { console.error('Print failed', e); }
    finally { try { host.style.display='none'; } catch {} }
  };
  // Two consecutive rAF calls: first schedules after style & layout, second after paint
  requestAnimationFrame(() => {
    requestAnimationFrame(doPrint);
  });
}

// Expose (optional) for debugging
try { window.printLedgerPopup = printLedgerPopup; } catch {}

// =====================
// Ledger CSV Export
// =====================
function exportCurrentLedgerCsv() {
  const view = window.__ledgerCurrentView;
  const accSel = document.getElementById('ledgerAccountFilter');
  if (!view || !Array.isArray(view.transactions) || !accSel) {
    showToast && showToast('No ledger data to export','warning');
    return;
  }
  const formatCurrency = (n) => `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const accountName = accSel.options[accSel.selectedIndex]?.text || 'Account';
  const monthEl = document.getElementById('ledgerMonth');
  const dayEl = document.getElementById('ledgerDay');
  const rangeLabel = (dayEl?.value) ? `Day ${dayEl.value}` : (monthEl?.value ? `Month ${monthEl.value}` : 'All');
  const headers = ['Date','Description','Debit','Credit','Balance'];
  const rows = [];
  // Opening row
  rows.push(['','Opening Balance','','', formatCurrency(view.opening)]);
  let running = view.opening;
  view.transactions.forEach(t => {
    const isIn = String(t.type).toLowerCase() === 'in';
    const amt = Number(t.amount||0);
    if (isIn) running += amt; else running -= amt;
    const desc = (t.category ? t.category : '') + (t.notes ? (t.category? ' — ': '') + t.notes : '');
    rows.push([
      t.date || '',
      desc,
      isIn ? formatCurrency(amt) : '',
      !isIn ? formatCurrency(amt) : '',
      formatCurrency(running)
    ]);
  });
  rows.push(['','Totals', formatCurrency(view.debitSum), formatCurrency(view.creditSum), formatCurrency(view.closing)]);
  const safe = (s) => '"' + String(s??'').replace(/"/g,'""') + '"';
  const csvLines = [ `"Ledger Export","${accountName.replace(/"/g,'""')}","${rangeLabel.replace(/"/g,'""')}"`, headers.map(safe).join(','), ...rows.map(r=>r.map(safe).join(',')) ];
  const blob = new Blob([csvLines.join('\n')], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const ts = new Date();
  const stamp = ts.toISOString().slice(0,19).replace(/[:T]/g,'-');
  a.href = url;
  a.download = `ledger-${accountName.replace(/[^a-z0-9_-]+/gi,'_')}-${stamp}.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(()=> URL.revokeObjectURL(url), 2000);
  showToast && showToast('Ledger exported','success');
}
try { window.exportCurrentLedgerCsv = exportCurrentLedgerCsv; } catch {}

// =====================
// Ledger Professional Report Popup
// =====================
function openLedgerReportPopup() {
  const view = window.__ledgerCurrentView;
  const accSel = document.getElementById('ledgerAccountFilter');
  if (!view || !accSel || !Array.isArray(view.transactions) || !view.transactions.length) {
    showToast && showToast('Select an account with data first','warning');
    return;
  }
  const accountName = accSel.options[accSel.selectedIndex]?.text || 'Account';
  const monthEl = document.getElementById('ledgerMonth');
  const dayEl = document.getElementById('ledgerDay');
  const rangeLabel = (dayEl?.value) ? `Day ${dayEl.value}` : (monthEl?.value ? `Month ${monthEl.value}` : 'All');
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2})}`;
  // Build rows with running balance (recompute to ensure freshness)
  let running = view.opening;
  const bodyRows = view.transactions.map(t => {
    const isIn = String(t.type).toLowerCase()==='in';
    const amt = Number(t.amount||0);
    if (isIn) running += amt; else running -= amt;
    const desc = (t.category ? t.category : '') + (t.notes ? (t.category? ' — ': '') + t.notes : '');
    return `<tr>
      <td>${escapeHtml(t.date||'')}</td>
      <td>${escapeHtml(desc||'')}</td>
      <td class="num">${isIn?fmt(amt):''}</td>
      <td class="num">${!isIn?fmt(amt):''}</td>
      <td class="num">${fmt(running)}</td>
    </tr>`;
  }).join('');
  const net = view.debitSum - view.creditSum;
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8" />
  <title>Ledger Report - ${escapeHtml(accountName)}</title>
  <style>
    :root { --ink:#1f2937; --muted:#64748B; --brand:#4F46E5; font-family:Inter,Arial,sans-serif; }
    body { margin:24px; color:var(--ink); }
    h1 { font-size:20px; margin:0 0 4px; font-weight:800; letter-spacing:.5px; }
    .meta { font-size:12px; color:var(--muted); line-height:1.4; }
    table { width:100%; border-collapse:separate; border-spacing:0; margin-top:16px; font-size:12px; }
    thead th { text-align:left; background:#EEF2FF; padding:6px 8px; font-weight:600; font-size:11px; border:1px solid #E2E8F0; }
    tbody td { padding:6px 8px; border:1px solid #E2E8F0; vertical-align:top; }
    tbody tr:nth-child(even){ background:#F8FAFC; }
    tfoot td { padding:6px 8px; border:1px solid #E2E8F0; font-weight:600; background:#F1F5F9; }
    .num { text-align:right; white-space:nowrap; font-feature-settings:"tnum"; }
    .summary { margin-top:14px; font-size:12px; background:#F1F5F9; border:1px solid #E2E8F0; border-radius:6px; padding:8px 10px; }
    header { display:flex; justify-content:space-between; align-items:flex-start; }
    .brand { font-size:22px; font-weight:800; color:var(--brand); letter-spacing:.5px; }
    .watermark { position:fixed; top:50%; left:50%; transform:translate(-50%, -50%); font-size:80px; font-weight:800; color:rgba(99,102,241,0.06); pointer-events:none; user-select:none; }
    .footer { margin-top:32px; font-size:11px; color:var(--muted); display:flex; justify-content:space-between; }
    @media print { body { margin:8mm 10mm; } @page { size:A4 portrait; margin:10mm; } .no-print { display:none !important; } }
    .toolbar { margin-top:12px; display:flex; gap:8px; }
    .btn { background:#4F46E5; color:#fff; border:none; padding:6px 12px; border-radius:4px; cursor:pointer; font-size:12px; font-weight:600; }
    .btn.secondary { background:#64748B; }
    .badge { display:inline-block; background:#E0E7FF; color:#3730A3; padding:2px 8px; border-radius:999px; font-size:11px; font-weight:600; letter-spacing:.3px; }
  </style></head><body>
  <div class="watermark">CONFIDENTIAL</div>
  <header>
    <div>
      <h1>Account Ledger Report</h1>
      <div class="meta">
        <div><strong>Account:</strong> ${escapeHtml(accountName)}</div>
        <div><strong>Range:</strong> ${escapeHtml(rangeLabel)}</div>
        <div><strong>Generated:</strong> ${escapeHtml(new Date().toLocaleString())}</div>
      </div>
    </div>
    <div class="meta" style="text-align:right;">
      <div class="badge">Opening ${fmt(view.opening)}</div><br/>
      <div><strong>Debits:</strong> ${fmt(view.debitSum)}</div>
      <div><strong>Credits:</strong> ${fmt(view.creditSum)}</div>
      <div><strong>Closing:</strong> ${fmt(view.closing)}</div>
      <div><strong>Net:</strong> ${net>=0?'+':''}${fmt(net)}</div>
    </div>
  </header>
  <div class="toolbar no-print">
    <button class="btn" onclick="window.print()"><i>🖨</i> Print</button>
    <button class="btn secondary" onclick="window.close()">Close</button>
  </div>
  <!-- Summary line removed per request -->
  <table>
  <thead><tr><th>Date</th><th>Description</th><th>Debit</th><th>Credit</th></tr></thead>
    <tbody>${bodyRows}</tbody>
    <tfoot>
      <tr><td colspan="2">Totals</td><td class="num">${fmt(view.debitSum)}</td><td class="num">${fmt(view.creditSum)}</td><td class="num">${fmt(view.closing)}</td></tr>
    </tfoot>
  </table>
  <div class="footer"><div>NIPON System Ledger Report</div><div>${escapeHtml(new Date().toLocaleDateString())}</div></div>
  </body></html>`;
  const w = window.open('', '_blank', 'noopener,noreferrer,width=1024,height=800');
  if (!w) { showToast && showToast('Popup blocked','error'); return; }
  try {
    w.document.open();
    w.document.write(html);
    w.document.close();
  } catch (err) {
    // Some browsers under strict popup settings may delay document readiness; fallback
    try {
      setTimeout(() => {
        try { w.document.body ? (w.document.body.innerHTML = html) : w.document.write(html); } catch {}
      }, 50);
    } catch {}
  }
  // Attempt deferred print trigger (user clicks inside new window)
}
try { window.openLedgerReportPopup = openLedgerReportPopup; } catch {}

// =====================
// Ledger PDF Export (simple inline generator)
// =====================
function exportLedgerPdf() {
  const view = window.__ledgerCurrentView;
  const accSel = document.getElementById('ledgerAccountFilter');
  if (!view || !accSel || !Array.isArray(view.transactions) || !view.transactions.length) {
    showToast && showToast('Select an account with data first','warning');
    return;
  }
  const accountName = accSel.options[accSel.selectedIndex]?.text || 'Account';
  const monthEl = document.getElementById('ledgerMonth');
  const dayEl = document.getElementById('ledgerDay');
  const rangeLabel = (dayEl?.value) ? `Day ${dayEl.value}` : (monthEl?.value ? `Month ${monthEl.value}` : 'All');
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2})}`;
  let running = view.opening;
  const rows = view.transactions.map(t => {
    const isIn = String(t.type).toLowerCase()==='in';
    const amt = Number(t.amount||0);
    if (isIn) running += amt; else running -= amt;
    const desc = (t.category ? t.category : '') + (t.notes ? (t.category? ' — ': '') + t.notes : '');
    return `<tr><td>${escapeHtml(t.date||'')}</td><td>${escapeHtml(desc||'')}</td><td class="num">${isIn?fmt(amt):''}</td><td class="num">${!isIn?fmt(amt):''}</td><td class="num">${fmt(running)}</td></tr>`;
  }).join('');
  const net = view.debitSum - view.creditSum;
  const style = `:root { --ink:#1f2937; --muted:#64748B; --brand:#4F46E5; font-family:Inter,Arial,sans-serif; } body { font-family:Inter,Arial,sans-serif; margin:16px; color:var(--ink); }
  h1 { font-size:18px; margin:0 0 4px; font-weight:800; letter-spacing:.4px; }
  .meta { font-size:10px; color:var(--muted); line-height:1.3; }
  table { width:100%; border-collapse:separate; border-spacing:0; margin-top:12px; font-size:10px; }
  thead th { text-align:left; background:#EEF2FF; padding:4px 6px; font-weight:600; font-size:10px; border:1px solid #E2E8F0; }
  tbody td { padding:4px 6px; border:1px solid #E2E8F0; }
  tbody tr:nth-child(even){ background:#F8FAFC; }
  tfoot td { padding:4px 6px; border:1px solid #E2E8F0; font-weight:600; background:#F1F5F9; }
  .num { text-align:right; white-space:nowrap; font-feature-settings:"tnum"; }
  .summary { margin-top:10px; font-size:10px; background:#F1F5F9; border:1px solid #E2E8F0; border-radius:4px; padding:6px 8px; }
  .brand { font-size:16px; font-weight:800; color:var(--brand); letter-spacing:.4px; }
  .footer { margin-top:18px; font-size:9px; color:var(--muted); display:flex; justify-content:space-between; }
  @page { size:A4 portrait; margin:10mm; }`;
  const html = `<!DOCTYPE html><html><head><meta charset="utf-8"/><title>Ledger PDF</title><style>${style}</style></head><body>
  <h1>Account Ledger Report</h1>
  <div class="meta"><div><strong>Account:</strong> ${escapeHtml(accountName)}</div><div><strong>Range:</strong> ${escapeHtml(rangeLabel)}</div><div><strong>Generated:</strong> ${escapeHtml(new Date().toLocaleString())}</div></div>
  <!-- Summary line removed per request (duplicate variant) -->
  <table><thead><tr><th>Date</th><th>Description</th><th>Debit</th><th>Credit</th></tr></thead><tbody>${rows}</tbody><tfoot><tr><td colspan="2">Totals</td><td class="num">${fmt(view.debitSum)}</td><td class="num">${fmt(view.creditSum)}</td></tr></tfoot></table>
  <div class="footer"><div>Ledger Report</div><div>${escapeHtml(new Date().toLocaleDateString())}</div></div>
  </body></html>`;
  // Render HTML to canvas via print-to-PDF approach in hidden iframe (more consistent cross-browser)
  const blob = new Blob([html], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  const iframe = document.createElement('iframe');
  iframe.style.position='fixed'; iframe.style.right='0'; iframe.style.bottom='0'; iframe.style.width='0'; iframe.style.height='0'; iframe.style.border='0';
  let printed = false;
  iframe.onload = () => {
    if (printed) return; printed = true;
    try {
      setTimeout(() => {
        try {
          const cw = iframe.contentWindow;
          const cleanup = () => { try { URL.revokeObjectURL(url); iframe.remove(); } catch {} };
          try { cw?.addEventListener('afterprint', () => cleanup(), { once: true }); } catch {}
          cw?.focus();
          cw?.print();
        } catch {}
        setTimeout(() => { try { URL.revokeObjectURL(url); iframe.remove(); } catch {} }, 4000);
      }, 150);
    } catch {}
  };
  iframe.src = url;
  document.body.appendChild(iframe);
  showToast && showToast('Preparing PDF (use system Save as PDF)','info');
}
try { window.exportLedgerPdf = exportLedgerPdf; } catch {}

// (Removed duplicate escapeHtml; single definition earlier in file)

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
  // Use raw flows (may contain entries whose account hasn't synced yet) and keep a filtered copy for totals
  let rawFlows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll.slice() : (__fundCashflowsCache||[]).slice();
  const iso = (v)=> (typeof v === 'string' && v.length >= 10 ? v : '');
  rawFlows.sort((a,b)=>{
    const c = iso(b.createdAt).localeCompare(iso(a.createdAt));
    if (c !== 0) return c; // newer createdAt first
    return String(b.date||'').localeCompare(String(a.date||''));
  });
  const todayYmd = (()=>{const d=new Date();return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;})();
  const monthYm = todayYmd.slice(0,7);
  let tIn=0,tOut=0,mIn=0,mOut=0;
  const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
  // Treat unknown accountIds as asset until accounts list arrives to avoid hiding fresh cashflows (common race right after payslip save)
  const isAsset = (id) => assetIds.length === 0 || !id || assetIds.includes(id);
  // Track net movements for today to compute opening-of-day (cash in hand)
  let netToday = 0; // IN adds, OUT subtracts for entries dated today
  for (const t of rawFlows) {
    const typ = String(t.type||'').toLowerCase();
    const amt = Math.abs(Number(t.amount||0))||0;
    const d = String(t.date||'');
    if (!isAsset(t.accountId)) continue;
    // Today should reflect the transaction date only, not when it was created in the system
    const isToday = (d === todayYmd);
    if (isToday) {
      if (typ==='in'||typ==='income'||typ==='credit') { tIn+=amt; netToday+=amt; }
      else if (typ==='out'||typ==='expense'||typ==='debit') { tOut+=amt; netToday-=amt; }
    }
    if (d.startsWith(monthYm+'-')) { if (typ==='in'||typ==='income'||typ==='credit') mIn+=amt; else if (typ==='out'||typ==='expense'||typ==='debit') mOut+=amt; }
  }
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  const byId = (x)=>document.getElementById(x);
  const setText=(id,val)=>{const el=byId(id); if (el) el.textContent = fmt(val)};
  // Compute Cash in Hand (carry-forward) from only Cash (Asset) transactions.
  // This running balance adds incomes and subtracts expenses recorded to the Cash account and does not reset automatically by day.
  let cashInHand = 0;
  try {
    const accs = __getLocalAccounts() || [];
    const cashAcc = accs.find(a => String(a.type||'').toLowerCase()==='asset' && String(a.name||'')==='Cash')
                  || accs.find(a => String(a.type||'').toLowerCase()==='asset' && String(a.name||'').toLowerCase().includes('cash'));
    const cashId = cashAcc?.id || null;
    const isCash = (t) => {
      if (cashId) return t && t.accountId === cashId;
      // Fallback when accountId is missing or accounts not yet loaded
      const n = String(t?.accountName||'').toLowerCase();
      return n === 'cash' || n.includes('cash');
    };
    for (const t of rawFlows) {
      if (!isCash(t)) continue;
      const typ = String(t.type||'').toLowerCase();
      const amt = Math.abs(Number(t.amount||0))||0;
      if (typ==='in'||typ==='income'||typ==='credit') cashInHand += amt;
      else if (typ==='out'||typ==='expense'||typ==='debit') cashInHand -= amt;
    }
  } catch {}
  setText('overviewCashInHand', cashInHand);
  setText('overviewTodayIn', tIn);
  setText('overviewTodayOut', tOut);
  setText('overviewMonthIn', mIn);
  setText('overviewMonthOut', mOut);
  const recentEmpty = byId('accountsRecentEmpty');
  const tbl = byId('accountsRecentTable');
  const tbody = byId('accountsRecentTbody');
  if (!tbody) return;
  // Salary/Advance prioritization (use raw flows so we don't miss due to transient account filtering)
  const salaryLike = rawFlows.filter(f => /^(salary|advance)$/i.test(String(f.category||'')));
  const primary = rawFlows.slice(0,10);
  const merged = primary.slice();
  for (const ps of salaryLike.slice(0,10)) {
    if (!merged.some(m => m.id === ps.id)) merged.push(ps);
  }
  const rows = merged.slice(0,15);
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
    const highlight = /^(salary|advance)$/i.test(String(t.category||''));
    const newId = (typeof window !== 'undefined') ? window.__recentHighlightedCashflowId : null;
    const isNew = newId && t.id === newId;
    return `<tr class="hover:bg-gray-50 ${highlight?'bg-indigo-50/40':''}">
      <td class="px-4 py-2">${escapeHtml(t.date||'')}</td>
      <td class="px-4 py-2">${escapeHtml(accName(t.accountId) || t.accountName || '')}</td>
      <td class="px-4 py-2">${(typ==='in'||typ==='income'||typ==='credit')?'In':'Out'}</td>
      <td class="px-4 py-2 ${highlight?'font-semibold text-indigo-600':''}">${escapeHtml(t.category||'')}${isNew? ' <span style="display:inline-block;margin-left:4px;padding:2px 6px;font-size:10px;border-radius:12px;background:#4f46e5;color:#fff;line-height:1;">NEW</span>':''}</td>
      <td class="px-4 py-2 text-right">${fmt(amt)}</td>
      <td class="px-4 py-2">${escapeHtml(t.notes||'')}</td>
      <td class="px-4 py-2 text-right">
        <button class="btn btn-danger btn-sm" data-delete-cashflow="${t.id}" title="Delete transaction"><i class="fas fa-trash"></i></button>
      </td>
    </tr>`;
  }).join('');
  // Clear highlight marker after first render so only appears once
  try { if (window.__recentHighlightedCashflowId) setTimeout(()=> { window.__recentHighlightedCashflowId = null; }, 2000); } catch {}
  try {
    const recentPayslip = salaryLike.slice(0,3);
    const missing = recentPayslip.filter(p => !rows.some(r => r.id === p.id));
    if (missing.length) console.debug('[AccountsOverview] Payslip cashflows not in visible rows (post-merge)', missing.map(m=>({id:m.id,date:m.date,amount:m.amount,category:m.category,accountId:m.accountId})));
    // Additional diagnostics: show assetIds and whether each missing was excluded due to account filter
    if (missing.length) {
      const diag = missing.map(m => ({ id:m.id, accountId:m.accountId, treatedAsAsset:isAsset(m.accountId), category:m.category, createdAt:m.createdAt }));
      console.debug('[AccountsOverview] Diagnostic (assetIds length='+assetIds.length+')', diag);
    }
  } catch {}
  try {
    makeTableGrid(tbody, {
      tableId: 'accountsRecent',
      editableCols: [],
      numericCols: [4],
      fields: ['date','account','type','category','amount','notes','actions'],
      getRowKey: (r) => {
        const tr = tbody.querySelectorAll('tr')[r]; if (!tr) return String(r);
        const date = tr.children[0]?.textContent||'';
        const acc = tr.children[1]?.textContent||'';
        const typ = tr.children[2]?.textContent||'';
        const cat = tr.children[3]?.textContent||'';
        const amt = tr.children[4]?.getAttribute('data-raw')||tr.children[4]?.textContent||'';
        const notes = tr.children[5]?.textContent||'';
        const actions = tr.children[6]?.textContent||'';
        return `${date}|${acc}|${typ}|${cat}|${amt}|${notes}|${actions}`;
      }
    });
  } catch {}
}

// Shared helper: delete a cashflow and handle any linked payroll artifacts (advance payslip) to restore balances
window.deleteCashflowWithLinks = async function(id) {
  if (!id) return;
  try {
    // Read the cashflow first to capture linkage
    const cfRef = doc(db, 'cashflows', id);
    let cf = null;
    try {
      const snap = await getDoc(cfRef);
      if (snap && snap.exists()) cf = snap.data();
    } catch {}

    let payslipId = cf?.payslipId || null;
    let linkedEmpId = null;
    let linkedPeriod = null;
    let linkedIsAdvance = false;
    if (payslipId) {
      try {
        const psSnap = await getDoc(doc(db, 'payslips', payslipId));
        if (psSnap && psSnap.exists()) {
          const ps = psSnap.data();
          linkedEmpId = ps.employeeId || null;
          linkedPeriod = ps.period || null;
          linkedIsAdvance = Boolean(ps.isAdvance);
        }
      } catch {}
    } else {
      // Fallback for legacy cashflows that don't carry payslipId
      // Try to infer the linked advance payslip using notes/category/amount/month
      try {
        const cat = String(cf?.category || '').toLowerCase();
        const isAdvanceLike = cat.includes('advance');
        const notes = String(cf?.notes || '');
        const amt = Math.abs(Number(cf?.amount || 0)) || 0;
        const period = (cf?.month && /^\d{4}-\d{2}$/.test(cf.month))
          ? cf.month
          : (String(cf?.date || '').slice(0, 7));
        // Extract employee name from notes like "... for John Doe"
        let inferredName = '';
        const m = /\bfor\s+(.+)$/i.exec(notes);
        if (m && m[1]) inferredName = m[1].trim();
        // Resolve employeeId by name (exact match preferred)
        let inferredEmp = null;
        try {
          const all = Array.isArray(employees) && Array.isArray(temporaryEmployees)
            ? [...employees, ...temporaryEmployees]
            : [];
          inferredEmp = all.find(e => String(e.name || '').trim().toLowerCase() === inferredName.toLowerCase())
            || all.find(e => String(e.name || '').toLowerCase().includes(inferredName.toLowerCase()));
        } catch {}
        if (isAdvanceLike && amt > 0 && period && inferredEmp?.id) {
          // Load this employee's payslips and try to find an advance with matching period and net amount
          const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId', '==', inferredEmp.id)));
          const slips = psSnap.docs.map(d => ({ id: d.id, ...d.data() }));
          const candidates = slips.filter(s => Boolean(s.isAdvance) && (s.period === period) && (Math.abs(Number(s.net || 0)) === amt));
          if (candidates.length) {
            // Pick the most recent by createdAt if present
            candidates.sort((a,b) => (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0));
            const hit = candidates[0];
            payslipId = hit.id;
            linkedEmpId = hit.employeeId || inferredEmp.id;
            linkedPeriod = hit.period || period;
            linkedIsAdvance = true;
          }
        }
      } catch (infErr) {
        console.warn('Payslip inference failed (non-fatal)', infErr);
      }
    }

    // Delete the cashflow itself
    await deleteDoc(cfRef);

    // Optimistically update in-memory caches and notify UI so Summary/Overview update immediately
    try {
      let changed = false;
      if (Array.isArray(window.__cashflowAll)) {
        const next = window.__cashflowAll.filter(x => x && x.id !== id);
        if (next.length !== window.__cashflowAll.length) { window.__cashflowAll = next; changed = true; }
      }
      if (Array.isArray(__fundCashflowsCache)) {
        const nextFund = __fundCashflowsCache.filter(x => x && x.id !== id);
        if (nextFund.length !== __fundCashflowsCache.length) { __fundCashflowsCache = nextFund; changed = true; }
      }
      if (changed) {
        try { window.dispatchEvent(new CustomEvent('cashflow:updated', { detail: window.__cashflowAll || [] })); } catch {}
        try { updateAccountsFundCard && updateAccountsFundCard(); } catch {}
        try { renderAccountsOverview && renderAccountsOverview(); } catch {}
        try { renderAccountsSummary && renderAccountsSummary(); } catch {}
      }
    } catch {}

  // If this was linked to an advance payslip, delete that too and recompute balances
    if (payslipId && linkedIsAdvance) {
      try { await deleteDoc(doc(db, 'payslips', payslipId)); } catch (e) { console.warn('Failed to delete linked payslip', e); }
      try {
        if (linkedEmpId && linkedPeriod) {
          const [y, m] = String(linkedPeriod).split('-');
          const when = (y && m) ? new Date(Number(y), Number(m) - 1, 1) : new Date();
          // Recompute the payslip's month
          await upsertMonthlyBalanceFor(linkedEmpId, when);
          // Also recompute current month to propagate carryover changes
          await upsertMonthlyBalanceFor(linkedEmpId, new Date());
          // Refresh payroll modal if open for this employee
          try {
            const modal = document.getElementById('payrollModal');
            if (modal && modal.classList.contains('show') && currentPayrollView?.id === linkedEmpId) {
              await loadPayslipsForPayrollModal(linkedEmpId);
              try { renderPayrollTable && renderPayrollTable(); } catch {}
            }
          } catch {}
        }
      } catch (e) { console.warn('Balance upsert failed after advance delete', e); }
    }

    // If this was linked to a non-advance salary payslip, delete payslip and try to rollback matching payment
    if (payslipId && !linkedIsAdvance) {
      try { await deleteDoc(doc(db, 'payslips', payslipId)); } catch (e) { console.warn('Failed to delete linked salary payslip', e); }
      try {
        const amt = Math.abs(Number(cf?.amount || 0)) || 0;
        const dateStr = String(cf?.date || '').slice(0,10);
        if (linkedEmpId) {
          // Attempt to find payment by employee+date+total amount
          const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId','==', linkedEmpId)));
          const payments = paySnap.docs.map(d => ({ id:d.id, ...d.data() }));
          const candidates = payments.filter(p => !Boolean(p.isAdvance)
            && (!dateStr || String(p.date||'').slice(0,10) === dateStr)
            && Math.abs(Number(p.amount || 0) + Number(p.overtime || 0)) === amt);
          if (candidates.length) {
            candidates.sort((a,b)=> (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0));
            try { await deleteDoc(doc(db,'payments', candidates[0].id)); } catch (e) { console.warn('Failed to delete matching payment for salary payslip', e); }
          }
          // Recompute balances for payslip month and current month
          try {
            if (linkedPeriod) {
              const [y, m] = String(linkedPeriod).split('-');
              const when = (y && m) ? new Date(Number(y), Number(m) - 1, 1) : new Date();
              await upsertMonthlyBalanceFor(linkedEmpId, when);
            }
            await upsertMonthlyBalanceFor(linkedEmpId, new Date());
          } catch (e) { console.warn('Balance upsert failed (non-advance payslip delete)', e); }
          // Refresh payroll modal if open for this employee
          try {
            const modal = document.getElementById('payrollModal');
            if (modal && modal.classList.contains('show') && currentPayrollView?.id === linkedEmpId) {
              await loadPayslipsForPayrollModal(linkedEmpId);
              try { renderPayrollTable && renderPayrollTable(); } catch {}
            }
          } catch {}
        }
      } catch (e) {
        console.warn('Salary payslip rollback failed (non-fatal)', e);
      }
    }

    // Handle normal Salary payments (no payslipId): find and delete matching payment and non-advance payslip
    if (!payslipId) {
      try {
        const cat = String(cf?.category || '').toLowerCase();
        const isSalaryLike = cat.includes('salary');
        const notes = String(cf?.notes || '');
        const amt = Math.abs(Number(cf?.amount || 0)) || 0;
        const dateStr = String(cf?.date || '').slice(0,10);
        // Extract employee name from notes like "Salary payment for John Doe"
        let inferredName = '';
        const m2 = /\bfor\s+(.+)$/i.exec(notes);
        if (m2 && m2[1]) inferredName = m2[1].trim();
        // Resolve employee id by name
        let inferredEmp = null;
        try {
          const all = Array.isArray(employees) && Array.isArray(temporaryEmployees)
            ? [...employees, ...temporaryEmployees]
            : [];
          inferredEmp = all.find(e => String(e.name || '').trim().toLowerCase() === inferredName.toLowerCase())
            || all.find(e => String(e.name || '').toLowerCase().includes(inferredName.toLowerCase()));
        } catch {}
        if (isSalaryLike && amt > 0 && dateStr && inferredEmp?.id) {
          // Find a payments doc with same employee and date whose amount+overtime equals this cashflow amount
          const paySnap = await getDocs(query(collection(db, 'payments'), where('employeeId','==', inferredEmp.id)));
          const payments = paySnap.docs.map(d => ({ id:d.id, ...d.data() }));
          const candidates = payments.filter(p => !Boolean(p.isAdvance)
            && String(p.date||'').slice(0,10) === dateStr
            && Math.abs(Number(p.amount || 0) + Number(p.overtime || 0)) === amt);
          if (candidates.length) {
            // Delete newest matching payment first (in case of duplicates)
            candidates.sort((a,b)=> (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0));
            const pay = candidates[0];
            try { await deleteDoc(doc(db,'payments', pay.id)); } catch (e) { console.warn('Failed to delete matching salary payment', e); }
            // Try delete a matching non-advance payslip for the month and net=amt
            try {
              const ym = String(dateStr).slice(0,7);
              const psSnap = await getDocs(query(collection(db,'payslips'), where('employeeId','==', inferredEmp.id)));
              const slips = psSnap.docs.map(d => ({ id:d.id, ...d.data() }));
              const nonAdv = slips.filter(s => !Boolean(s.isAdvance) && (s.period === ym) && (Math.abs(Number(s.net||0)) === amt));
              if (nonAdv.length) {
                nonAdv.sort((a,b)=> (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0));
                try { await deleteDoc(doc(db,'payslips', nonAdv[0].id)); } catch (e) { console.warn('Failed to delete matching non-advance payslip', e); }
              }
            } catch {}
            // Recompute balances for payment month and current month
            try {
              const when = new Date(dateStr);
              await upsertMonthlyBalanceFor(inferredEmp.id, when);
              await upsertMonthlyBalanceFor(inferredEmp.id, new Date());
              // If payroll modal is open for this employee, refresh
              try {
                const modal = document.getElementById('payrollModal');
                if (modal && modal.classList.contains('show') && currentPayrollView?.id === inferredEmp.id) {
                  await loadPayslipsForPayrollModal(inferredEmp.id);
                  try { renderPayrollTable && renderPayrollTable(); } catch {}
                }
              } catch {}
            } catch (e) { console.warn('Balance upsert failed after salary payment delete', e); }
          }
        }
      } catch (salErr) {
        console.warn('Salary payment undo inference failed (non-fatal)', salErr);
      }
    }

    // Recompute fund and refresh views
    try { window.__recomputeFund && window.__recomputeFund(); } catch {}
    try { renderAccountsOverview && renderAccountsOverview(); } catch {}
    try { renderLedgerTable && renderLedgerTable(); } catch {}
    try {
      if (payslipId && linkedIsAdvance) {
        showToast && showToast('Transaction deleted — advance payslip removed and payroll balance updated', 'success');
      } else if (payslipId && !linkedIsAdvance) {
        showToast && showToast('Transaction deleted — salary payslip removed and payroll balance updated', 'success');
      } else if ((String(cf?.category||'').toLowerCase().includes('salary'))) {
        showToast && showToast('Transaction deleted — salary payment rolled back and payroll balance updated', 'success');
      } else {
        showToast && showToast('Transaction deleted', 'success');
      }
    } catch {}
  } catch (err) {
    console.error('deleteCashflowWithLinks failed', err);
    try { showToast && showToast('Failed to delete transaction', 'error'); } catch {}
  }
}

// Delete a cashflow from Recent Transactions
document.addEventListener('click', async (e) => {
  const btn = e.target.closest?.('[data-delete-cashflow]');
  if (!btn) return;
  const id = btn.getAttribute('data-delete-cashflow');
  if (!id) return;
  const ok = confirm('Delete this transaction? This will update totals across the app.');
  if (!ok) return;
  try {
    if (typeof window.deleteCashflowWithLinks === 'function') {
      await window.deleteCashflowWithLinks(id);
    } else {
      await deleteDoc(doc(db, 'cashflows', id));
      showToast('Transaction deleted', 'success');
      try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
      try { renderAccountsOverview(); } catch {}
    }
  } catch (err) {
    console.error('Delete transaction failed', err);
    showToast('Failed to delete transaction', 'error');
  }
});

// Initialize grid on Ledger table after it renders
function setupLedgerGrid() {
  const tbody = document.getElementById('ledgerTableBody');
  if (!tbody) return;
  if (!tbody.querySelector('tr')) return; // nothing to grid
  // Strip any lingering 5th balance column cell (runtime output from legacy module)
  try {
    const tbl = tbody.closest('table');
    if (tbl) {
      // Legacy cleanup: only remove the 5th column if it is a Balance column; keep our Actions column intact
      const ths = tbl.querySelectorAll('thead th');
      const hasFive = ths.length === 5;
      const isLegacyBalance = hasFive && /balance/i.test((ths[4]?.textContent||''));
      if (isLegacyBalance) {
        ths[4].remove();
        // Remove the 5th cell from body rows
        tbody.querySelectorAll('tr').forEach(tr=>{ const tds = tr.querySelectorAll('td'); if (tds.length === 5) tds[4].remove(); });
        const tfoot = tbl.querySelector('tfoot');
        if (tfoot) tfoot.querySelectorAll('tr').forEach(tr=>{ const tds = tr.querySelectorAll('td'); if (tds.length === 5) tds[4].remove(); });
        // Fix any colspan=5 to 4
        tbl.querySelectorAll('td[colspan="5"]').forEach(td=> td.setAttribute('colspan','4'));
      }
    }
  } catch {}
  makeTableGrid(tbody, {
    tableId: 'accountsLedger',
    editableCols: [1],
    numericCols: [2,3], // Debit, Credit
    fields: ['date','description','debit','credit','actions'],
    getRowKey: (r) => {
      const tr = tbody.querySelectorAll('tr')[r]; if (!tr) return String(r);
      const date = tr.children[0]?.textContent||'';
      const desc = tr.children[1]?.textContent||'';
      const deb = tr.children[2]?.getAttribute('data-raw')||tr.children[2]?.textContent||'';
      const cred = tr.children[3]?.getAttribute('data-raw')||tr.children[3]?.textContent||'';
      const act = tr.children[4]?.textContent||'';
      return `${date}|${desc}|${deb}|${cred}|${act}`;
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
  // Optimistically signal changes so Overview/Summary recompute immediately
  try { window.dispatchEvent(new Event('cashflow:updated')); } catch {}
      closeTransferModal();
    } catch (err) {
      console.warn('Transfer failed', err);
      showToast('Failed to save transfer', 'error');
    }
  }
});

// Compute and render Current Fund Available
let __cashflowShadow = [];
function __computeFundBreakdown() {
  if (FUND_MODE_FIXED) {
    return { opening: __fundOpening, inSum: 0, outSum: 0, net: 0, total: __fundBalance, flows: [] };
  }
  const openingTotal = Number(__fundOpening || 0);
  const flows = (__fundCashflowsCache && __fundCashflowsCache.length)
    ? __fundCashflowsCache
    : ((Array.isArray(__cashflowShadow) && __cashflowShadow.length)
      ? __cashflowShadow
      : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []));
  let inSum = 0, outSum = 0;
  const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
  const isAsset = (id) => !id || assetIds.includes(id);
  for (const t of flows) {
    if (!t || !isAsset(t.accountId)) continue;
    const typeStr = String(t.type || '').toLowerCase();
    const amt = Math.abs(Number(t.amount || 0)) || 0;
    if (typeStr === 'in' || typeStr === 'income' || typeStr === 'credit') inSum += amt;
    else if (typeStr === 'out' || typeStr === 'expense' || typeStr === 'debit') outSum += amt;
  }
  const net = inSum - outSum;
  return { opening: openingTotal, inSum, outSum, net, total: openingTotal + net, flows };
}

function updateAccountsFundCard() {
  if (FUND_MODE_FIXED) {
    const el = document.getElementById('accountsFundValue');
  if (el) el.textContent = `${CURRENCY_PREFIX}${Number(__fundBalance||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
    const asOf = document.getElementById('accountsFundAsOf');
    if (asOf) asOf.textContent = `as of ${new Date().toLocaleString()}`;
    const comp = document.getElementById('computedFundLabel');
  if (comp) comp.textContent = `${CURRENCY_PREFIX}${Number(__fundBalance||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
    return;
  }
  // Prefer dedicated caches; fallback to module-provided shadows
  const { total, flows } = __computeFundBreakdown();

  // Anomaly detection: if total is exact negation of previous total, and no flow set changed, assume opening fallback bug and restore previous value
  try {
    const currentFlowHash = flows.map(f=>f.id).sort().join('|');
    // Load persisted snapshot if first run this session
    if (__fundPrevTotal === null) {
      try {
        const raw = localStorage.getItem(__FUND_LS_KEY);
        if (raw) {
          const parsed = JSON.parse(raw);
          if (parsed && typeof parsed.total === 'number') {
            __fundPrevTotal = parsed.total;
            __fundPrevFlowHash = parsed.flowHash || null;
          }
        }
      } catch {}
    }

    const inversionDetected = (__fundPrevTotal !== null && total === -1 * __fundPrevTotal && currentFlowHash === (__fundPrevFlowHash||currentFlowHash));
    if (inversionDetected) {
      console.warn('[Fund] Detected sign inversion without flow change (persistent check)');
      const displayTotal = Math.abs(__fundPrevTotal); // prefer positive absolute prior amount
      // Auto-correct: If not yet applied, attempt to flip opening sign so future recomputes are correct.
      if (!__fundSignCorrectionApplied) {
        __fundSignCorrectionApplied = true;
        try {
          // Derive current breakdown again for accuracy
          const br = __computeFundBreakdown();
          const expectedOpening = displayTotal - br.net; // opening + net = displayTotal
          // If flipping sign of current opening gets us closer to expected, adjust opening
          if (Math.abs((-1 * __fundOpening + br.net) - displayTotal) < Math.abs((__fundOpening + br.net) - displayTotal)) {
            const correctedOpening = expectedOpening;
            console.warn('[Fund] Applying one-time opening correction', { from: __fundOpening, to: correctedOpening });
            __fundOpening = correctedOpening;
            // Persist corrected opening (value recomputed implicitly by UI); avoid overwriting other fields
            try { setDoc(doc(db, 'stats', 'fund'), cleanData({ opening: Number(correctedOpening), asOf: new Date().toISOString() }), { merge: true }); } catch (e) { console.warn('Opening correction persist failed', e); }
          }
        } catch (e) { console.warn('Fund correction logic error', e); }
      }
      const el = document.getElementById('accountsFundValue');
  if (el) el.textContent = `${CURRENCY_PREFIX}${Number(displayTotal).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      const asOf = document.getElementById('accountsFundAsOf');
      if (asOf) asOf.textContent = `as of ${new Date().toLocaleString()}`;
      const comp = document.getElementById('computedFundLabel');
  if (comp) comp.textContent = `${CURRENCY_PREFIX}${Number(displayTotal).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      // Do not update prev snapshot so we can re-evaluate after correction
      return;
    }
    // Normal path: persist current snapshot
    __fundPrevTotal = total;
    __fundPrevFlowHash = currentFlowHash;
    try { localStorage.setItem(__FUND_LS_KEY, JSON.stringify({ total, flowHash: currentFlowHash, ts: Date.now() })); } catch {}
  } catch {}

  const fmt = (n) => `${CURRENCY_PREFIX}${Number(n || 0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  // Fund display removed from Accounts tab; keep logic for potential modal only
  const el = document.getElementById('accountsFundValue'); if (el) el.textContent = fmt(total);
  const asOf = document.getElementById('accountsFundAsOf'); if (asOf) asOf.textContent = `as of ${new Date().toLocaleString()}`;
  const comp = document.getElementById('computedFundLabel'); if (comp) comp.textContent = fmt(total);
}

// Dedicated snapshot-based fund updater
function subscribeFundCardSnapshots() {
  // Cashflows snapshot
  try {
    if (__unsubFundCashflows) { __unsubFundCashflows(); __unsubFundCashflows = null; }
  } catch {}
  __unsubFundCashflows = onSnapshot(collection(db, 'cashflows'), async (snap) => {
    const docs = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    if (!FUND_MODE_FIXED) {
      __fundCashflowsCache = docs;
      try { window.__cashflowAll = __fundCashflowsCache.slice(); } catch {}
      try { updateAccountsFundCard(); } catch {}
      return;
    }
    // Fixed mode: compute delta vs previous snapshot to account for adds/updates/deletes
    const prev = Array.isArray(__fundCashflowsCache) ? __fundCashflowsCache : [];
    const prevMap = new Map(prev.map(f => [f.id, f]));
    const nextMap = new Map(docs.map(f => [f.id, f]));
    const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
    const isAsset = (id) => !id || assetIds.includes(id);

    const contrib = (cf) => {
      if (!cf || !isAsset(cf.accountId)) return 0;
      const amt = Math.abs(Number(cf.amount||0))||0;
      const t = String(cf.type||'').toLowerCase();
      if (['in','income','credit'].includes(t)) return amt;
      if (['out','expense','debit'].includes(t)) return -amt;
      return 0;
    };

    let delta = 0;
    // Added or updated
    for (const [id, cf] of nextMap) {
      const before = prevMap.get(id);
      if (!before) {
        delta += contrib(cf);
      } else {
        const diff = contrib(cf) - contrib(before);
        if (diff !== 0) delta += diff;
      }
    }
    // Deleted
    for (const [id, cf] of prevMap) {
      if (!nextMap.has(id)) {
        delta -= contrib(cf);
      }
    }

    // Update cached snapshot and persist new balance
    __fundCashflowsCache = docs;
    if (delta !== 0) {
      __fundBalance = Number(__fundBalance) + delta;
      try { await setDoc(doc(db,'stats','fund'), cleanData({ balance: Number(__fundBalance), asOf: new Date().toISOString() }), { merge: true }); } catch (e) { console.warn('Persist fund balance failed', e); }
    }
    try { window.__cashflowAll = docs.slice(); } catch {}
    try { updateAccountsFundCard(); } catch {}
  }, (err) => { console.warn('Fund cashflows snapshot error', err); });

  // Stats/fund snapshot (opening)
  try {
    if (__unsubFundStats) { __unsubFundStats(); __unsubFundStats = null; }
  } catch {}
  __unsubFundStats = onSnapshot(doc(db, 'stats', 'fund'), async (snap) => {
    if (snap && snap.exists()) {
      const d = snap.data();
      if (FUND_MODE_FIXED) {
        if (Object.prototype.hasOwnProperty.call(d,'balance')) {
          const b = Number(d.balance); if (!isNaN(b)) __fundBalance = b; else __fundBalance = 0;
        } else if (Object.prototype.hasOwnProperty.call(d,'opening')) {
          const legacy = Number(d.opening); __fundBalance = isNaN(legacy)?0:legacy; // migrate silently
          try { await setDoc(doc(db,'stats','fund'), cleanData({ balance: __fundBalance, migratedFromOpening:true, asOf: new Date().toISOString() }), { merge: true }); } catch {}
        }
      } else {
        if (Object.prototype.hasOwnProperty.call(d, 'opening')) {
          const raw = Number(d.opening); if (!isNaN(raw)) __fundOpening = raw; else __fundOpening = 0;
        }
      }
    } else {
      if (FUND_MODE_FIXED) { __fundBalance = 0; } else { __fundOpening = 0; }
    }
    try { updateAccountsFundCard(); } catch {}
  }, (err) => { console.warn('Fund stats snapshot error', err); });
}

// Deterministic recompute: read stats/fund opening + cashflows from Firestore, compute total, and persist to stats/fund
async function recomputeFundFromFirestoreAndPersist() {
  if (FUND_MODE_FIXED) {
    // In fixed mode recompute just ensures stats/fund document exists; no full rebuild.
    try {
      const docSnap = await getDoc(doc(db,'stats','fund'));
      if (!docSnap.exists()) await setDoc(doc(db,'stats','fund'), cleanData({ balance: Number(__fundBalance||0), asOf: new Date().toISOString() }));
    } catch {}
    updateAccountsFundCard();
    return __fundBalance;
  }
  try {
    // Ensure stats/fund exists; read opening
    let openingTotal = 0;
    try {
      const fundSnap = await getDoc(doc(db, 'stats', 'fund'));
      if (fundSnap.exists()) {
        const d = fundSnap.data();
        if (Object.prototype.hasOwnProperty.call(d, 'opening')) openingTotal = Number(d.opening)||0; else if (Object.prototype.hasOwnProperty.call(d,'value')) openingTotal = Number(d.value)||0; else openingTotal = 0;
      } else {
        // initialize with opening=0
        await setDoc(doc(db, 'stats', 'fund'), cleanData({ opening: 0, inSum: 0, outSum: 0, asOf: new Date().toISOString() }));
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
  const payload = cleanData({ opening: Number(openingTotal), inSum: Number(inSum), outSum: Number(outSum), asOf: new Date().toISOString() });
    try { await setDoc(doc(db, 'stats', 'fund'), payload, { merge: true }); } catch {}
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

// ========= Diagnostics =========
try {
  window.__fundDiagnose = async function(verbose = false) {
    const br = __computeFundBreakdown();
    console.group('%c[FUND DIAG]','color:#2563eb;font-weight:600');
    console.log('Opening', br.opening, 'In', br.inSum, 'Out', br.outSum, 'Net', br.net, 'Total', br.total);
    console.log('PrevTotal', __fundPrevTotal, 'PrevFlowHash', __fundPrevFlowHash);
    if (verbose) {
      (br.flows||[]).forEach(f=> console.log(f.id, f.type, f.accountId, f.amount));
    }
    console.groupEnd();
    return br;
  };
} catch {}

// Add UI tooltip / click to show quick breakdown
// Removed alt-click breakdown trigger since fund card UI was removed

// Ensure a default Asset account exists (e.g., Cash/Bank/Fund) and return its id
// Matching strategy:
// 1) Exact name match (case-insensitive) against fallbackName among Asset accounts.
// 2) Fallback to keyword substring match among Asset accounts.
// This avoids picking up similarly named accounts like "Fund Adjustment" when we really want "Fund".
async function ensureAssetAccount(keyword, fallbackName) {
  const list = __getLocalAccounts();
  const lower = (s) => String(s || '').toLowerCase();
  const assets = (list || []).filter(a => lower(a.type) === 'asset');
  let match = assets.find(a => lower(a.name) === lower(fallbackName));
  if (!match) match = assets.find(a => lower(a.name).includes(lower(keyword)));
  if (match) return match.id;
  // Create new (retry logic)
  try {
    const iso = new Date().toISOString();
    const payload = cleanData({ name: fallbackName, type: 'Asset', opening: 0, createdAt: iso });
    const ref = await addDoc(collection(db, 'accounts'), payload);
    // Force refresh if accounts subscribers haven't fired yet
    try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
    return ref.id;
  } catch (e) {
    console.warn('[Accounts] Primary asset account create failed, retrying with fallback', e);
    try {
      const altName = `${fallbackName} ${Date.now().toString().slice(-4)}`;
      const ref2 = await addDoc(collection(db, 'accounts'), cleanData({ name: altName, type: 'Asset', opening: 0, createdAt: new Date().toISOString() }));
      try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
      return ref2.id;
    } catch (e2) {
      console.error('[Accounts] Could not create any asset account', e2);
      showToast && showToast('Unable to create Cash account (permission/rules?)', 'error');
      return '';
    }
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
  if (FUND_MODE_FIXED) {
    __fundBalance = Number(desired)||0;
    await setDoc(doc(db,'stats','fund'), cleanData({ balance: __fundBalance, asOf: new Date().toISOString(), setBy: auth?.currentUser?.uid||undefined, setByEmail: auth?.currentUser?.email||undefined, setReason: 'manual-set-fund' }), { merge: true });
    updateAccountsFundCard();
    return;
  }
  // Legacy path (opening + flows model)
  let inSum = 0, outSum = 0;
  try {
    const flows = Array.isArray(__fundCashflowsCache) && __fundCashflowsCache.length ? __fundCashflowsCache : (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []);
    const assetIds = (__getLocalAccounts()||[]).filter(a => String(a.type||'').toLowerCase()==='asset').map(a=>a.id);
    const isAsset = (id) => !id || assetIds.includes(id);
    for (const t of flows) {
      if (!t) continue; if (!isAsset(t.accountId)) continue;
      const amt = Math.abs(Number(t.amount || 0)) || 0;
      const typeStr = String(t.type || '').toLowerCase();
      if (['in','income','credit'].includes(typeStr)) inSum += amt; else if (['out','expense','debit'].includes(typeStr)) outSum += amt;
    }
  } catch {}
  const requiredOpening = Number(desired) - (inSum - outSum);
  await setDoc(doc(db,'stats','fund'), cleanData({ opening: Number(requiredOpening), inSum:Number(inSum), outSum:Number(outSum), asOf:new Date().toISOString(), setBy: auth?.currentUser?.uid||undefined, setByEmail: auth?.currentUser?.email||undefined, setReason:'manual-reconciliation' }), { merge: true });
  __fundOpening = Number(requiredOpening);
  updateAccountsFundCard();
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
          <p style="font-size:12px;color:#64748b;margin:4px 0 0;">Sets the business fund balance directly. No transactions are created or modified.</p>
        </div>
        <div style="display:flex;justify-content:flex-end;gap:8px;margin-top:16px;">
          <button type="button" data-cancel class="btn btn-secondary"><i class="fas fa-times"></i> Cancel</button>
          <button type="button" data-save class="btn btn-primary"><i class="fas fa-save"></i> <span>Save</span></button>
        </div>
      </div>
    </div>`;
  document.body.appendChild(portal);
  try { document.body.classList.add('modal-open'); } catch {}
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
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
    nationality: (document.getElementById('nationality')?.value || '').trim(),
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

// Handle vehicle form submission
async function handleVehicleFormSubmit(e) {
  e.preventDefault();

  if (!authed) {
    showToast('Please sign in to add vehicles', 'warning');
    return;
  }

  const vehicle = {
    id: document.getElementById('vehicleId').value || null,
    model: document.getElementById('vehicleModel').value.trim(),
    brand: document.getElementById('vehicleBrand').value.trim(),
    year: parseInt(document.getElementById('vehicleYear').value),
    owner: document.getElementById('vehicleOwner').value.trim(),
    vehicleNumber: document.getElementById('vehicleNumber').value.trim(),
    status: 'active', // Default status
    expiryDate: document.getElementById('vehicleExpiryDate').value,
    renewDate: document.getElementById('vehicleRenewDate').value || null,
    registrationDate: document.getElementById('vehicleRegistrationDate').value,
    istimaraURL: vehicleIstimaraURL || null,
    createdAt: new Date().toISOString()
  };

  const isNew = !vehicle.id;

  // Close modal immediately
  closeVehicleModal();
  showToast(isNew ? 'Saving new vehicle…' : 'Updating vehicle…', 'info');

  try {
    if (vehicle.id) {
      // Update existing vehicle
      const vehicleRef = doc(db, "vehicles", vehicle.id);
      const { id, ...updateData } = vehicle;
      await updateDoc(vehicleRef, cleanData(updateData));
      showToast('Vehicle updated successfully', 'success');
    } else {
      // Add new vehicle
      const { id, ...newVehicle } = vehicle;
      await addDoc(collection(db, "vehicles"), cleanData(newVehicle));
      showToast('Vehicle added successfully', 'success');
    }
    
    // Reset the Istimara URL after successful save
    vehicleIstimaraURL = null;
  } catch (err) {
    console.error('Save vehicle failed:', err);
    showToast('Failed to save vehicle. Please try again.', 'error');
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

// Close employee modal (used in various event handlers) - restored to prevent ReferenceError
window.closeEmployeeModal = function() {
  try {
    const modal = document.getElementById('employeeModal');
    if (modal) modal.classList.remove('show');
  } catch {}
};

// Vehicle modal functions
function openVehicleModal(mode = 'add', vehicleData = null) {
  try {
    const modal = document.getElementById('vehicleModal');
    const form = document.getElementById('vehicleForm');
    const title = document.getElementById('vehicleFormTitle');
    
    if (!modal || !form) return;
    
    // Reset form
    form.reset();
    document.getElementById('vehicleId').value = '';
    
    // Set title
    if (title) {
      title.textContent = mode === 'edit' ? 'Edit Vehicle' : 'Add New Vehicle';
    }
    
    // If editing, populate form
    if (mode === 'edit' && vehicleData) {
      document.getElementById('vehicleId').value = vehicleData.id || '';
      document.getElementById('vehicleModel').value = vehicleData.model || '';
      document.getElementById('vehicleBrand').value = vehicleData.brand || '';
      document.getElementById('vehicleYear').value = vehicleData.year || '';
      document.getElementById('vehicleOwner').value = vehicleData.owner || '';
      document.getElementById('vehicleNumber').value = vehicleData.vehicleNumber || '';
      document.getElementById('vehicleExpiryDate').value = vehicleData.expiryDate ? vehicleData.expiryDate.slice(0, 10) : '';
      document.getElementById('vehicleRenewDate').value = vehicleData.renewDate ? vehicleData.renewDate.slice(0, 10) : '';
      document.getElementById('vehicleRegistrationDate').value = vehicleData.registrationDate ? vehicleData.registrationDate.slice(0, 10) : '';
    }
    
    modal.classList.add('show');
  } catch (e) {
    console.error('Error opening vehicle modal:', e);
  }
}

// Handle Istimara PDF upload
async function uploadIstimaraPDF(file) {
  if (!file) return null;
  
  // Validate file type
  if (file.type !== 'application/pdf') {
    showToast('Please upload a PDF file only', 'error');
    return null;
  }
  
  // Validate file size (max 10MB)
  const maxSize = 10 * 1024 * 1024; // 10MB
  if (file.size > maxSize) {
    showToast('File size must be less than 10MB', 'error');
    return null;
  }
  
  try {
    // Create unique filename
    const timestamp = Date.now();
    const fileName = `istimara_${timestamp}_${file.name}`;
    const fileRef = storageRef(storage, `vehicles/istimara/${fileName}`);
    
    // Show progress
    const progressDiv = document.getElementById('istimaraUploadProgress');
    const progressBar = document.getElementById('istimaraProgressBar');
    const progressText = document.getElementById('istimaraProgressText');
    
    if (progressDiv) progressDiv.style.display = 'block';
    
    // Upload file
    const uploadTask = uploadBytesResumable(fileRef, file);
    
    return new Promise((resolve, reject) => {
      uploadTask.on('state_changed',
        (snapshot) => {
          const progress = (snapshot.bytesTransferred / snapshot.totalBytes) * 100;
          if (progressBar) progressBar.style.width = progress + '%';
          if (progressText) progressText.textContent = `Uploading: ${Math.round(progress)}%`;
        },
        (error) => {
          console.error('Upload error:', error);
          if (progressDiv) progressDiv.style.display = 'none';
          showToast('Failed to upload file', 'error');
          reject(error);
        },
        async () => {
          // Upload complete
          const downloadURL = await getDownloadURL(uploadTask.snapshot.ref);
          if (progressDiv) progressDiv.style.display = 'none';
          
          // Show file info
          const fileInfo = document.getElementById('istimaraFileInfo');
          const fileNameSpan = document.getElementById('istimaraFileName');
          if (fileInfo && fileNameSpan) {
            fileNameSpan.textContent = file.name;
            fileInfo.style.display = 'block';
          }
          
          showToast('File uploaded successfully', 'success');
          resolve(downloadURL);
        }
      );
    });
  } catch (error) {
    console.error('Upload error:', error);
    showToast('Failed to upload file', 'error');
    return null;
  }
}

// Remove uploaded Istimara PDF
window.removeIstimaraPDF = function() {
  vehicleIstimaraURL = null;
  const fileInput = document.getElementById('vehicleIstimaraPDF');
  const fileInfo = document.getElementById('istimaraFileInfo');
  
  if (fileInput) fileInput.value = '';
  if (fileInfo) fileInfo.style.display = 'none';
  
  showToast('File removed', 'info');
};

// Add file input change listener
document.addEventListener('DOMContentLoaded', () => {
  const istimaraInput = document.getElementById('vehicleIstimaraPDF');
  if (istimaraInput) {
    istimaraInput.addEventListener('change', async (e) => {
      const file = e.target.files[0];
      if (file) {
        vehicleIstimaraURL = await uploadIstimaraPDF(file);
      }
    });
  }
});

window.closeVehicleModal = function() {
  try {
    const modal = document.getElementById('vehicleModal');
    if (modal) modal.classList.remove('show');
    
    // Reset form and file upload
    const form = document.getElementById('vehicleForm');
    if (form) form.reset();
    vehicleIstimaraURL = null;
    
    // Reset all fields to editable state
    const fieldIds = ['vehicleId', 'vehicleModel', 'vehicleBrand', 'vehicleYear', 
                      'vehicleOwner', 'vehicleNumber', 
                      'vehicleExpiryDate', 'vehicleRenewDate', 'vehicleRegistrationDate'];
    fieldIds.forEach(fid => {
      const el = document.getElementById(fid);
      if (el) {
        el.readOnly = false;
        el.style.backgroundColor = '';
        el.style.cursor = '';
      }
    });
    
    // Show file upload input and save button again
    const fileInput = document.getElementById('vehicleIstimaraPDF');
    if (fileInput) fileInput.style.display = '';
    
    const saveBtn = document.querySelector('#vehicleForm button[type="submit"]');
    if (saveBtn) saveBtn.style.display = '';
    
    // Hide file info
    const fileInfo = document.getElementById('istimaraFileInfo');
    if (fileInfo) fileInfo.style.display = 'none';
    
    // Reset title
    const title = document.getElementById('vehicleFormTitle');
    if (title) title.textContent = 'Add Vehicle';
    
  } catch {}
};

// Vehicle action functions
window.viewVehicle = function(id) {
  try {
    const vehicle = vehicles.find(v => v.id === id);
    if (!vehicle) {
      showToast('Vehicle not found', 'error');
      return;
    }
    
    // Open view modal
    const modal = document.getElementById('viewVehicleModal');
    if (modal) modal.classList.add('show');
    
    // Helper to set text content
    const setText = (id, text) => {
      const el = document.getElementById(id);
      if (el) el.textContent = text || '—';
    };
    
    // Populate vehicle information
    setText('viewVehicleNameDisplay', `${vehicle.brand} ${vehicle.model}`);
    setText('viewVehicleYearDisplay', `Year: ${vehicle.year}`);
    setText('viewVehicleOwner', vehicle.owner);
    setText('viewVehicleNumber', vehicle.vehicleNumber);
    
    // Status chip with color
    const statusChip = document.getElementById('viewVehicleStatusChip');
    if (statusChip) {
      statusChip.textContent = vehicle.status.charAt(0).toUpperCase() + vehicle.status.slice(1);
      if (vehicle.status === 'active') {
        statusChip.className = 'px-2 py-0.5 rounded text-xs font-semibold bg-emerald-100 text-emerald-800';
      } else if (vehicle.status === 'maintenance') {
        statusChip.className = 'px-2 py-0.5 rounded text-xs font-semibold bg-amber-100 text-amber-800';
      } else {
        statusChip.className = 'px-2 py-0.5 rounded text-xs font-semibold bg-gray-100 text-gray-800';
      }
    }
    
    // Format dates
    const formatDate = (dateStr) => {
      if (!dateStr) return '—';
      try {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
      } catch {
        return dateStr;
      }
    };
    
    setText('viewVehicleRegistrationDate', formatDate(vehicle.registrationDate));
    setText('viewVehicleExpiryDate', formatDate(vehicle.expiryDate));
    setText('viewVehicleRenewDate', formatDate(vehicle.renewDate));
    
    // Handle Istimara PDF
    const istimaraSection = document.getElementById('viewVehicleIstimaraSection');
    if (vehicle.istimaraURL) {
      if (istimaraSection) istimaraSection.style.display = '';
      
      const iframe = document.getElementById('viewVehicleIstimaraPDF');
      const link = document.getElementById('viewVehicleIstimaraLink');
      const download = document.getElementById('viewVehicleIstimaraDownload');
      
      if (iframe) iframe.src = vehicle.istimaraURL;
      if (link) link.href = vehicle.istimaraURL;
      if (download) {
        download.href = vehicle.istimaraURL;
        download.download = `istimara_${vehicle.vehicleNumber}.pdf`;
      }
    } else {
      if (istimaraSection) istimaraSection.style.display = 'none';
    }
    
    // Store current vehicle ID for edit button
    window.currentViewVehicleId = id;
    
  } catch (e) {
    console.warn('viewVehicle failed', e);
  }
};

window.closeViewVehicleModal = function() {
  try {
    const modal = document.getElementById('viewVehicleModal');
    if (modal) modal.classList.remove('show');
    
    // Clear iframe
    const iframe = document.getElementById('viewVehicleIstimaraPDF');
    if (iframe) iframe.src = '';
    
    window.currentViewVehicleId = null;
  } catch {}
};

window.editVehicleFromView = function() {
  try {
    closeViewVehicleModal();
    if (window.currentViewVehicleId) {
      editVehicle(window.currentViewVehicleId);
    }
  } catch (e) {
    console.warn('editVehicleFromView failed', e);
  }
};

window.editVehicle = function(id) {
  try {
    const vehicle = vehicles.find(v => v.id === id);
    if (!vehicle) {
      showToast('Vehicle not found', 'error');
      return;
    }
    
    // Open modal in edit mode
    const modal = document.getElementById('vehicleModal');
    const title = document.getElementById('vehicleFormTitle');
    if (title) title.textContent = 'Edit Vehicle';
    if (modal) modal.classList.add('show');
    
    // Populate form fields and ensure they're editable
    const setVal = (fid, val) => {
      const el = document.getElementById(fid);
      if (el) {
        el.value = val ?? '';
        el.readOnly = false; // Ensure editable
        el.style.backgroundColor = '';
        el.style.cursor = '';
      }
    };
    
    setVal('vehicleId', vehicle.id);
    setVal('vehicleModel', vehicle.model);
    setVal('vehicleBrand', vehicle.brand);
    setVal('vehicleYear', vehicle.year);
    setVal('vehicleOwner', vehicle.owner);
    setVal('vehicleNumber', vehicle.vehicleNumber);
    setVal('vehicleExpiryDate', vehicle.expiryDate ? vehicle.expiryDate.slice(0, 10) : '');
    setVal('vehicleRenewDate', vehicle.renewDate ? vehicle.renewDate.slice(0, 10) : '');
    setVal('vehicleRegistrationDate', vehicle.registrationDate ? vehicle.registrationDate.slice(0, 10) : '');
    
    // Show existing file if available
    if (vehicle.istimaraURL) {
      vehicleIstimaraURL = vehicle.istimaraURL;
      const fileInfo = document.getElementById('istimaraFileInfo');
      const fileNameSpan = document.getElementById('istimaraFileName');
      if (fileInfo && fileNameSpan) {
        fileNameSpan.textContent = 'Existing file';
        fileInfo.style.display = 'block';
      }
    }
    
    // Ensure file input and save button are visible
    const fileInput = document.getElementById('vehicleIstimaraPDF');
    if (fileInput) fileInput.style.display = '';
    
    const saveBtn = document.querySelector('#vehicleForm button[type="submit"]');
    if (saveBtn) saveBtn.style.display = '';
    
  } catch (e) {
    console.warn('editVehicle failed', e);
  }
};

window.deleteVehicle = function(id) {
  try {
    const vehicle = vehicles.find(v => v.id === id);
    if (!vehicle) {
      showToast('Vehicle not found', 'error');
      return;
    }
    
    // Confirm deletion
    if (!confirm(`Are you sure you want to delete ${vehicle.model} ${vehicle.brand}?`)) {
      return;
    }
    
    deleteVehicleFromDB(id, vehicle);
  } catch (e) {
    console.warn('deleteVehicle failed', e);
  }
};

// Delete vehicle from database and storage
async function deleteVehicleFromDB(id, vehicle) {
  try {
    // Delete Istimara PDF from storage if it exists
    if (vehicle.istimaraURL) {
      try {
        // Extract the file path from the download URL
        // URL format: https://firebasestorage.googleapis.com/v0/b/bucket/o/vehicles%2Fistimara%2Ffile.pdf?token=...
        const url = new URL(vehicle.istimaraURL);
        const pathMatch = url.pathname.match(/\/o\/(.+)$/);
        
        if (pathMatch) {
          // Decode the URL-encoded path
          const filePath = decodeURIComponent(pathMatch[1]);
          const fileRef = storageRef(storage, filePath);
          await deleteObject(fileRef);
          console.log('Istimara PDF deleted from storage:', filePath);
        } else {
          console.warn('Could not extract file path from URL:', vehicle.istimaraURL);
        }
      } catch (storageErr) {
        console.error('Failed to delete Istimara PDF from storage:', storageErr);
        // Continue with vehicle deletion even if file deletion fails
      }
    }
    
    // Delete vehicle document from Firestore
    await deleteDoc(doc(db, "vehicles", id));
    showToast('Vehicle deleted successfully', 'success');
  } catch (err) {
    console.error('Delete vehicle failed:', err);
    showToast('Failed to delete vehicle', 'error');
  }
}

// Global editEmployee used by table action buttons (prevent ReferenceError)
window.editEmployee = function(id, which = 'employees') {
  try {
    const list = which === 'temporary' ? temporaryEmployees : employees;
    const emp = list.find(e => e.id === id);
    if (!emp) { showToast && showToast('Employee not found', 'error'); return; }
    openEmployeeModal(which, 'edit');
    // Populate form fields
    const setVal = (fid, val) => { const el = document.getElementById(fid); if (el) el.value = val ?? ''; };
    setVal('employeeId', emp.id);
    setVal('name', emp.name);
    setVal('email', emp.email);
    setVal('position', emp.position);
    setVal('department', emp.department);
  setVal('nationality', emp.nationality);
    setVal('salary', emp.salary);
    setVal('joinDate', emp.joinDate ? emp.joinDate.slice(0,10) : '');
    setVal('qid', emp.qid);
    setVal('phone', emp.phone);
    setVal('qidExpiry', emp.qidExpiry ? emp.qidExpiry.slice(0,10) : '');
    setVal('passportExpiry', emp.passportExpiry ? emp.passportExpiry.slice(0,10) : '');
    setVal('bankName', emp.bankName);
    setVal('bankAccountNumber', emp.bankAccountNumber);
    setVal('bankIban', emp.bankIban);
  } catch (e) {
    console.warn('editEmployee failed', e);
  }
};

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
  // Post-render: ensure the first action button shows "Pay Advance"
  try {
    const tbody = document.getElementById('payrollTableBody');
    if (tbody) {
      const btns = tbody.querySelectorAll('button.btn-compact');
      btns.forEach(btn => {
        const oc = btn.getAttribute('onclick') || '';
        if (oc.startsWith('openPayslipForm(')) {
          btn.innerHTML = '<i class="fas fa-sack-dollar"></i> Pay Advance';
        }
      });
    }
  } catch {}
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
  const fmtCurrency = (n) => `${CURRENCY_PREFIX}${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

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
            <div class="text-sm text-gray-700">${d.isAdvance ? 'Advance' : 'Salary'}: ${CURRENCY_PREFIX}${amount}</div>
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
  // Net logic: if an advance entered (>0) use that; otherwise treat full basic as payable for deduction purposes
  // This aligns deduction with user expectation that a payslip with zero advance still represents a liability.
  const net = advance > 0 ? advance : basic;
  return { basic, advance, net };
}

function updatePayslipNet() {
  const { net } = getPayslipNumbers();
  const el = document.getElementById('psNet');
  if (el) el.textContent = `${CURRENCY_PREFIX}${Number(net).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

// Wire payslip input change handlers and print button
document.addEventListener('input', (e) => {
  const ids = ['psBasic','psAdvanceAmount'];
  if (e.target && ids.includes(e.target.id)) {
    updatePayslipNet();
  }
});

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'payslipPdfBtn') {
    e.preventDefault();
    const ok = confirm('Generate payslip, update balances, and open PDF dialog?');
    if (!ok) return;
    savePayslipRecord().then((res) => {
      try { window.__payrollBalancesInvalidate?.(); } catch {}
      try { exportPayslipPdf(res?.balance); } catch (err) { console.error(err); showToast('Payslip PDF export failed', 'error'); }
      // After triggering the browser print dialog, proactively refresh Accounts Overview
      try { setTimeout(()=> { if (typeof renderAccountsOverview === 'function') renderAccountsOverview(); }, 300); } catch {}
      // Close the payslip modal (keeps UI tidy once saved)
      try { setTimeout(()=> { closePayslipModal && closePayslipModal(); }, 400); } catch {}
    }).catch(err => {
      console.error(err);
      showToast('Failed to save payslip; attempting PDF anyway', 'warning');
      try { exportPayslipPdf(); } catch (err2) { console.error(err2); showToast('Payslip PDF export failed', 'error'); }
    });
  } else if (e.target && e.target.id === 'paymentProPdfBtn') {
    e.preventDefault();
    (async () => {
      // Pre-validation to avoid noisy error stacks when amount missing
      const amountVal = Number(document.getElementById('payAmount')?.value || 0);
      const overtimeVal = Number(document.getElementById('payOvertime')?.value || 0);
      const payDateVal = document.getElementById('payDate')?.value || '';
      if (amountVal + overtimeVal <= 0) {
        showToast('Enter a payment amount or overtime > 0', 'warning');
        try { document.getElementById('payAmount')?.focus(); } catch {}
        return;
      }
      if (!payDateVal) {
        showToast('Select a payment date', 'warning');
        try { document.getElementById('payDate')?.focus(); } catch {}
        return;
      }
      const ok = confirm('Save salary payment and generate professional PDF?');
      if (!ok) return;      
      try {
  const paymentCtx = await savePaymentRecord();
  try { await createPayslipFromSalaryPayment({ emp: paymentCtx.employee, paymentRecord: paymentCtx.record }); } catch {}
  await exportProfessionalSalaryPaymentPdf(paymentCtx); // now async with Firestore enrichment
        try { setTimeout(()=> { if (typeof renderAccountsOverview === 'function') renderAccountsOverview(); }, 300); } catch {}
        try { setTimeout(()=> { closePaymentModal && closePaymentModal(); }, 450); } catch {}
      } catch (err) {
        if (err && /Invalid amount/.test(String(err.message||''))) {
          // Already surfaced via toast; suppress extra console noise
          return;
        }
        console.error('Salary payment professional PDF flow failed', err);
        showToast('Failed to save or export salary payment PDF', 'error');
      }
    })();
  }
});

// Extract base save logic from savePaymentThenPrint for reuse in professional PDF export (no slip HTML version)
async function savePaymentRecord() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); throw new Error('No employee'); }
  const payDate = document.getElementById('payDate')?.value || '';
  const amount = Number(document.getElementById('payAmount')?.value || 0);
  const method = document.getElementById('payMethod')?.value || 'cash';
  const ref = document.getElementById('payRef')?.value || '';
  const notes = document.getElementById('payNotes')?.value || '';
  const overtime = Number(document.getElementById('payOvertime')?.value || 0);
  const overtimeHours = Number(document.getElementById('payOvertimeHours')?.value || 0);
  const totalEntered = amount + (overtime||0);
  if (!(totalEntered > 0)) {
    showToast('Enter a payment amount or overtime > 0', 'warning');
    return Promise.reject(new Error('Invalid amount input'));
  }
  if (!payDate) { showToast('Select a payment date', 'warning'); throw new Error('No date'); }
  const isAdvance = false;
  const deductFrom = document.getElementById('payDeductFrom')?.value || 'none';
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
    isAdvance,
    deductFrom,
    notes,
    date: payDate,
    createdAt: serverTimestamp(),
    createdBy: auth?.currentUser?.uid || undefined,
    createdByEmail: auth?.currentUser?.email || undefined,
  });
  let paymentDocRef = null;
  try {
    paymentDocRef = await addDoc(collection(db, 'payments'), record);
    showToast('Salary payment saved', 'success');
  } catch (e) {
    console.error('Save payment failed', e);
    showToast('Failed to save salary payment', 'error');
    throw e;
  }
  try { window.__payrollBalancesInvalidate?.(); } catch {}
  try {
    const paymentMonth = String((record.date||'')).slice(0,7);
    if (paymentMonth && /^\d{4}-\d{2}$/.test(paymentMonth)) {
      await updateEmployeeBalance(emp.id, paymentMonth, { ...emp, _type: emp._which === 'temporary' ? 'Temporary' : 'Permanent' });
    }
    document.dispatchEvent(new Event('payroll:recompute-balances'));
  } catch {}
  try { await upsertMonthlyBalanceFor(emp.id, new Date(payDate)); } catch {}
  // Mirror salary payment cashflow posting (OUT for amount + overtime)
  try {
    const totalPaid = amount + (overtime||0);
    if (totalPaid > 0) {
      const accId = await ensureAssetAccount('cash','Cash');
      if (accId) {
        await addDoc(collection(db, 'cashflows'), cleanData({
          date: payDate,
          type: 'out',
          accountId: accId,
          amount: Math.abs(Number(totalPaid)||0),
          category: 'Salary',
          notes: `Salary payment for ${emp.name||''}`,
          createdAt: new Date().toISOString(),
        }));
        try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
        try { window.__recentHighlightedCashflowId = paymentDocRef?.id || null; } catch {}
        try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
      }
    }
  } catch (cfErr) { console.warn('Payment cashflow failed', cfErr); }
  return { record, employee: emp };
}

// Helper: create a non-advance payslip entry for a normal salary payment (used by both legacy & professional flows)
async function createPayslipFromSalaryPayment({ emp, paymentRecord }) {
  try {
    if (!emp || !paymentRecord) return;
    const payDate = paymentRecord.date;
    if (!payDate) return;
    const dObj = new Date(payDate);
    if (isNaN(dObj.getTime())) return;
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
    const totalPaid = Number(paymentRecord.amount || 0) + Number(paymentRecord.overtime || 0);
    if (!(totalPaid > 0)) return; // nothing meaningful to record as payslip
    const overtime = Number(paymentRecord.overtime || 0);
    const overtimeHours = Number(paymentRecord.overtimeHours || 0);
    const method = paymentRecord.method || 'cash';
    const ref = paymentRecord.reference || '';
    const notesParts = [
      `Payment: ${method}`,
      ref ? `Ref: ${ref}` : null,
  overtime > 0 ? `OT: ${CURRENCY_PREFIX}${Number(overtime).toLocaleString(undefined,{maximumFractionDigits:2})}${overtimeHours>0?` (${overtimeHours}h)`:''}` : null,
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
    // If Payroll Details modal open for this employee, refresh it
    try {
      const modal = document.getElementById('payrollModal');
      if (modal && modal.classList.contains('show') && currentPayrollView?.id === emp.id) {
        await loadPayslipsForPayrollModal(emp.id);
      }
    } catch {}
  } catch (e) {
    console.warn('[Payroll] Failed to create payslip from salary payment (pro flow)', e);
  }
}


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
  overtime > 0 ? `OT: ${CURRENCY_PREFIX}${Number(overtime).toLocaleString(undefined,{maximumFractionDigits:2})}${overtimeHours>0?` (${overtimeHours}h)`:''}` : null,
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

// Save payslip record only (extracted from previous savePayslipThenPrint)
async function savePayslipRecord() {
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
  let payslipRef = null;
  let updatedBalance = undefined;
  try {
    payslipRef = await addDoc(collection(db, 'payslips'), record);
    showToast('Payslip saved', 'success');
  } catch (e) {
    console.error('Save payslip failed', e);
    showToast('Failed to save payslip', 'error');
    throw e;
  }
  // Update balance after successful payslip save
  try {
    updatedBalance = await updateEmployeeBalance(emp.id, period, { ...emp, _type: emp._which === 'temporary' ? 'Temporary' : 'Permanent' });
    try { document.dispatchEvent(new Event('payroll:recompute-balances')); } catch {}
  } catch (e) { console.warn('Balance update failed (payslip)', e); }
  // Refresh payslips tab in payroll modal if open for this employee
  try {
    const modal = document.getElementById('payrollModal');
    if (modal && modal.classList.contains('show') && currentPayrollView?.id === emp.id) {
      await loadPayslipsForPayrollModal(emp.id);
    }
  } catch {}
  // Persist monthly balance snapshot
  try {
    const [y,m] = (record.period || '').split('-');
    const when = (y && m) ? new Date(Number(y), Number(m)-1, 1) : new Date();
    await upsertMonthlyBalanceFor(emp.id, when);
    try { window.__payrollBalancesInvalidate?.(); } catch {}
  } catch (e) { console.warn('Balance upsert failed (payslip)', e); }
  // Always post a cashflow OUT entry for the net amount (advance or full basic if no advance)
  try {
    if (Number(net) > 0) {
      // Use today's actual date for the cashflow (user expectation: transaction happens now)
      // even if the payslip period is a past month.
      const now = new Date();
      const txDate = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
      const accId = await ensureAssetAccount('cash', 'Cash');
      if (accId) {
        try {
          await addDoc(collection(db, 'cashflows'), cleanData({
            date: txDate,
            type: 'out',
            accountId: accId,
            amount: Math.abs(Number(net) || 0),
            category: isAdvance ? 'Advance' : 'Salary',
            notes: `${isAdvance ? 'Salary advance' : 'Salary payment'} for ${emp.name || ''}`,
            payslipId: (payslipRef && typeof payslipRef.id === 'string') ? payslipRef.id : undefined,
            createdAt: new Date().toISOString(),
          }));
          try { if (window.__recomputeFund) window.__recomputeFund(); } catch {}
          try { window.__recentHighlightedCashflowId = (payslipRef && payslipRef.id) ? payslipRef.id : null; } catch {}
          try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
        } catch (inner) {
          console.warn('Cashflow add (payslip) failed', inner);
          showToast('Payslip saved; cashflow not recorded', 'warning');
        }
      } else {
        showToast('No cash account available; fund not updated', 'warning');
      }
    }
  } catch (cfErr) {
    console.warn('Failed to post cashflow for payslip (non-fatal)', cfErr);
    showToast('Payslip saved but fund not updated (cashflow error)', 'warning');
  }
  // Proactively refresh payroll table if visible so current salary balance reflects deduction
  try {
    const payrollSection = document.getElementById('payrollSection');
    if (payrollSection && payrollSection.style.display !== 'none') {
      renderPayrollTable();
    }
  } catch {}
  return { balance: (typeof updatedBalance === 'number') ? updatedBalance : undefined };
}

// Append a cashflow for a payslip and aggressively refresh accounts + ledger
async function appendPayslipCashflow({ accId, empName, isAdvance, net, dt, payslipId, period }) {
  if (window.__debugPayslip) console.debug('[Payslip] appendPayslipCashflow start', { accId, empName, isAdvance, net, dt, payslipId, period });
  // Guard: ensure we actually have an account id; otherwise creating a cashflow will hide it from ledger filters
  if (!accId) {
    try {
      accId = await ensureAssetAccount('cash','Cash');
    } catch {}
  }
  if (!accId) {
    console.warn('[Payslip] Aborting cashflow creation: missing asset account id');
    showToast && showToast('Cashflow not recorded (no Cash account)', 'error');
    return null;
  }
  const flowPayload = cleanData({
    date: `${dt.getFullYear()}-${String(dt.getMonth()+1).padStart(2,'0')}-${String(dt.getDate()).padStart(2,'0')}`,
    type: 'out',
    accountId: accId,
    amount: Math.abs(Number(net) || 0),
    category: isAdvance ? 'Advance' : 'Salary',
    notes: `${isAdvance ? 'Salary advance' : 'Salary payment'} for ${empName}`,
    payslipId: payslipId || undefined,
    month: (period && /\d{4}-\d{2}/.test(period)) ? period : `${dt.getFullYear()}-${String(dt.getMonth()+1).padStart(2,'0')}`,
    accountName: 'Cash',
    createdBy: auth?.currentUser?.uid || undefined,
    createdByEmail: auth?.currentUser?.email || undefined,
    createdAt: new Date().toISOString(),
  });
  try {
    // Dedupe: if a cashflow with same payslipId already in cache, skip creating again
    if (payslipId) {
      const existing = (Array.isArray(window.__cashflowAll) ? window.__cashflowAll : []).find(cf => cf.payslipId === payslipId && cf.accountId === accId && String(cf.type).toLowerCase()==='out');
      if (existing) {
        try { console.debug('[Payslip] Cashflow already exists locally, skipping duplicate add', payslipId); } catch {}
        return existing.id ? existing : existing; // do not add duplicate
      }
    }
    const ref = await addDoc(collection(db, 'cashflows'), flowPayload);
    if (window.__debugPayslip) console.debug('[Payslip] cashflow addDoc success', ref.id);
    // Immediate local inject so UI updates even before snapshot
    const enriched = { id: ref.id, ...flowPayload };
  try { window.__recentHighlightedCashflowId = ref.id; } catch {}
    try { if (Array.isArray(window.__cashflowAll)) window.__cashflowAll.push(enriched); else window.__cashflowAll = [enriched]; } catch {}
    try { if (Array.isArray(__fundCashflowsCache)) __fundCashflowsCache.push(enriched); } catch {}
    // Optimistic fund decrement: directly adjust displayed fund before recompute
    try {
      const fundEl = document.getElementById('accountsFundValue');
      if (fundEl && fundEl.textContent) {
        const raw = fundEl.textContent.replace(/[^0-9.\-]/g,'');
        const cur = Number(raw)||0;
        const next = cur - (Math.abs(Number(net)||0));
  fundEl.textContent = `${CURRENCY_PREFIX}${next.toLocaleString(undefined,{maximumFractionDigits:2})}`;
      }
    } catch {}
  try { updateAccountsFundCard(); } catch {}
  // Fire synthetic events for modules relying on event-driven refresh
  try { window.dispatchEvent(new CustomEvent('cashflow:updated', { detail: window.__cashflowAll.slice?.() || [] })); } catch {}
  try { window.dispatchEvent(new Event('accounts:updated')); } catch {}
    // Auto-select Cash account in ledger if none selected so user sees the new entry
    try {
      const sel = document.getElementById('ledgerAccountFilter');
      if (sel && !sel.value) {
        sel.value = accId;
        renderLedgerTable && renderLedgerTable();
      }
    } catch {}
    // Force ledger refresh if current account selected matches
    try {
      const sel = document.getElementById('ledgerAccountFilter');
      if (sel && sel.value === accId) { renderLedgerTable(); }
    } catch {}
    // Deterministic recompute (non-blocking) & account list refresh
    try { window.__recomputeFund && window.__recomputeFund(); } catch {}
    try { refreshLedgerAccounts && refreshLedgerAccounts(); } catch {}
    showToast && showToast('Cashflow recorded for payslip', 'success');
    // Dispatch a dedicated event so other components can react specifically to payslip postings
    try { window.dispatchEvent(new CustomEvent('payslip:cashflow-recorded', { detail: enriched })); } catch {}
    // If Accounts Overview visible, force an immediate refresh for visibility
    try {
      const ovTab = document.getElementById('accountsTabOverview');
      if (ovTab && ovTab.style.display !== 'none' && typeof renderAccountsOverview === 'function') {
        renderAccountsOverview();
      }
    } catch {}
    return enriched;
  } catch (e) {
    console.warn('appendPayslipCashflow failed', e);
    showToast && showToast('Failed to record cashflow for payslip', 'error');
    if (window.__debugPayslip) console.debug('[Payslip] appendPayslipCashflow error', e);
  }
}

// Debug helper: verify linkage between a payslip and its cashflow
try {
  window.__verifyPayslipLink = async function(payslipId) {
    if (!payslipId) { console.warn('No payslipId provided'); return; }
    const qSnap = await getDocs(collection(db, 'cashflows'));
    const match = qSnap.docs.map(d=>({id:d.id,...d.data()})).filter(d=> d.payslipId === payslipId);
    console.log('[Payslip Verify] cashflows for', payslipId, match);
    return match;
  };
  // Diagnostic: list recent payslip cashflows missing accountId (should be zero)
  window.__findOrphanPayslipCashflows = async function(limit=10) {
    const qSnap = await getDocs(collection(db, 'cashflows'));
    const all = qSnap.docs.map(d=>({id:d.id,...d.data()}));
    const orphans = all.filter(c => (!c.accountId || c.accountId==='') && c.payslipId).slice(0, limit);
    console.log('[Payslip Diagnostic] Orphan cashflows (no accountId)', orphans);
    return orphans;
  };
} catch {}

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
function renderPayslipHtml({ emp, period, notes, basic, advance, net, currentBalance }) {
  const fmt = (n) => `${CURRENCY_PREFIX}${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const monthTitle = period ? new Date(Number(period.split('-')[0]), Number(period.split('-')[1]) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Period';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  // Removed advance & balance rows per requirement
  return `
    <section class="payslip">
      <header class="payslip-header">
        <div>
          <div class="title">Payslip</div>
          <div class="subtitle">${monthTitle}</div>
        </div>
  <div class="brand"></div>
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
          <tr><td>Total Paid (Advance)</td><td class="text-right">${fmt(net)}</td></tr>
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <div class="payslip-signature">
        <div class="sig-block">
          <div class="sig-line"></div>
          <div class="sig-label">Employee Signature</div>
        </div>
      </div>
  <footer class="payslip-footer">Generated • ${new Date().toLocaleString()}</footer>
    </section>
  `;
}

// Export Payslip as PDF (uses hidden iframe + browser print to allow Save as PDF)
function exportPayslipPdf(balanceOverride) {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const period = document.getElementById('psPeriod')?.value || '';
  const notes = document.getElementById('psNotes')?.value || '';
  const { basic, advance, net } = getPayslipNumbers();
  const htmlInner = renderPayslipHtml({ emp, period, notes, basic, advance, net, currentBalance: balanceOverride });
  const periodLabel = period || new Date().toISOString().slice(0,7);
  const safeName = (emp.name || 'Employee').replace(/[^a-z0-9-_]+/gi,'_');
  const docTitle = `Payslip_${safeName}_${periodLabel}`;
  const fullHtml = `<!DOCTYPE html><html><head><meta charset="utf-8" />
    <title>${docTitle}</title>
    <style>
      body { font-family: Inter, Arial, sans-serif; padding:16mm; color:#111827; }
      .payslip-header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:14px; }
      .payslip-header .title { font-size:24px; font-weight:800; letter-spacing:.5px; }
      .payslip-header .subtitle { font-size:12px; color:#6B7280; }
      .payslip-header .brand { font-size:22px; font-weight:800; color:#4F46E5; }
      .payslip-grid { display:grid; grid-template-columns: repeat(3, 1fr); gap:8px 16px; font-size:12px; margin:8px 0 10px; }
      .payslip-grid .label { color:#64748B; font-weight:500; }
      .payslip-grid .value { font-weight:600; color:#111827; }
      table { width:100%; border-collapse:collapse; font-size:12px; margin-top:4px; }
      th, td { border:1px solid #E5E7EB; padding:6px 8px; }
      thead th { background:#F3F4F6; color:#374151; text-align:left; }
      .total td { font-weight:700; background:#F8FAFC; }
      .payslip-notes { margin-top:12px; font-size:12px; border:1px solid #E5E7EB; background:#F9FAFB; padding:8px 10px; border-radius:6px; }
      .payslip-notes .label { font-weight:600; color:#475569; margin-bottom:4px; }
      .payslip-footer { margin-top:18px; font-size:10px; color:#64748B; text-align:right; }
      .payslip-signature { margin-top:16px; display:flex; gap:28px; }
      .payslip-signature .sig-block { width:260px; }
      .payslip-signature .sig-line { height:36px; border-bottom:1px solid #94A3B8; }
      .payslip-signature .sig-label { margin-top:6px; font-size:11px; color:#64748B; }
      .watermark { position:fixed; top:50%; left:50%; transform:translate(-50%, -50%); font-size:72px; font-weight:800; color:rgba(99,102,241,0.06); pointer-events:none; user-select:none; }
      @page { size:A4 portrait; margin:12mm; }
      @media print { body { padding:0; } }
    </style>
    </head><body>
  
      ${htmlInner}
    </body></html>`;
  const iframe = document.createElement('iframe');
  iframe.style.position = 'fixed';
  iframe.style.right = '0';
  iframe.style.bottom = '0';
  iframe.style.width = '0';
  iframe.style.height = '0';
  iframe.style.border = '0';
  document.body.appendChild(iframe);
  let printed = false;
  const triggerPrint = () => {
    if (printed) return; printed = true;
    try { iframe.contentWindow?.focus(); iframe.contentWindow?.print(); } catch {}
  };
  try {
    const doc = iframe.contentDocument || iframe.contentWindow.document;
    doc.open();
    doc.write(fullHtml);
    doc.close();
    doc.title = docTitle;
    if (doc.fonts && doc.fonts.ready) {
      doc.fonts.ready.then(() => setTimeout(triggerPrint, 60));
    } else {
      setTimeout(triggerPrint, 300);
    }
  } catch (err) {
    console.error('Payslip PDF export failed', err);
    showToast('Payslip PDF export failed', 'error');
    // Attempt fallback print once
    setTimeout(triggerPrint, 400);
  }
  // Cleanup after print (single listener)
  const cleanup = () => { try { document.body.removeChild(iframe); } catch {}; window.removeEventListener('afterprint', cleanup); };
  window.addEventListener('afterprint', cleanup, { once: true });
}

try { window.exportPayslipPdf = exportPayslipPdf; } catch {}

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

  const fmt = (n) => `${CURRENCY_PREFIX}${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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
  <div class="brand"></div>
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
          <tr><td>${isAdvance ? 'Advance Paid' : 'Salary Paid'}</td><td class="text-right">${fmt(amount)}</td></tr>
          ${overtime > 0 ? `<tr><td>Overtime</td><td class="text-right">${fmt(overtime)}</td></tr>` : ''}
          ${overtimeHours > 0 ? `<tr><td>Overtime Hours</td><td class="text-right">${Number(overtimeHours).toLocaleString()}</td></tr>` : ''}
          ${overtime > 0 ? `<tr class="total"><td>Total Paid</td><td class="text-right">${fmt(totalPaid)}</td></tr>` : ''}
          
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <div class="payslip-signature">
        <div class="sig-block">
          <div class="sig-line"></div>
          <div class="sig-label">Employee Signature</div>
        </div>
      </div>
  <footer class="payslip-footer">Generated • ${new Date().toLocaleString()}</footer>
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

async function exportProfessionalSalaryPaymentPdf(ctx) {
  if (!ctx) return;
  const { record, employee } = ctx;
  const emp = employee;
  const fmt = (n)=>`${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  // Derive month period and compute basic salary + advances like the standard salary slip print
  const d = new Date(record.date);
  const ym = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
  let advancesThisPeriod = 0;
  let basicForMonth = Number(emp.salary || 0);
  try {
    const { collection, getDocs, query, where } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
    const psSnap = await getDocs(query(collection(db, 'payslips'), where('employeeId','==', emp.id)));
    const all = psSnap.docs.map(doc=>({ id: doc.id, ...doc.data() }));
    const thisMonth = all.filter(p => (p.period||'') === ym);
    if (thisMonth.length) {
      const latest = thisMonth.sort((a,b)=>(b.createdAt?.seconds||0) - (a.createdAt?.seconds||0))[0];
      if (latest && Number(latest.basic)) basicForMonth = Number(latest.basic);
      advancesThisPeriod = thisMonth.reduce((s,p)=> s + Number(p.advance||0), 0);
    }
  } catch (e) { console.warn('Salary PDF monthly computation failed', e); }
  const balanceAfterAdvances = Math.max(0, basicForMonth - advancesThisPeriod); // retained variable for possible future use (row removed)
  const totalPaid = Number(record.amount||0) + Number(record.overtime||0);
  const dateStr = record.date ? new Date(record.date).toLocaleDateString(undefined,{year:'numeric',month:'long',day:'numeric'}) : '';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  const rows = [
    `<tr><td>Basic Salary (Monthly)</td><td class="text-right">${fmt(basicForMonth)}</td></tr>`,
    `<tr><td>Advances This Period (${ym})</td><td class="text-right">${fmt(advancesThisPeriod)}</td></tr>`,
    `<tr><td>Salary Paid</td><td class="text-right">${fmt(record.amount)}</td></tr>`
  ];
  if (record.overtime) rows.push(`<tr><td>Overtime</td><td class="text-right">${fmt(record.overtime)}</td></tr>`);
  if (record.overtimeHours) rows.push(`<tr><td>Overtime Hours</td><td class="text-right">${Number(record.overtimeHours).toLocaleString()}</td></tr>`);
  if (record.overtime) rows.push(`<tr class="total"><td>Total Paid</td><td class="text-right">${fmt(totalPaid)}</td></tr>`);
  const htmlInner = `
    <section class="payslip">
      <header class="payslip-header">
        <div>
          <div class="title">Salary Slip</div>
          <div class="subtitle">${dateStr}</div>
        </div>
  <div class="brand"></div>
      </header>
      <div class="payslip-grid">
        <div><div class="label">Employee</div><div class="value">${emp.name||''}</div></div>
        <div><div class="label">Type</div><div class="value">${typeLabel}</div></div>
        <div><div class="label">Company</div><div class="value">${emp.department||'-'}</div></div>
        <div><div class="label">Position</div><div class="value">${emp.position||'-'}</div></div>
        <div><div class="label">Payment Method</div><div class="value">${record.method||''}</div></div>
        <div><div class="label">Reference</div><div class="value">${record.reference||'-'}</div></div>
      </div>
      <table class="payslip-table">
        <thead><tr><th>Detail</th><th class="text-right">Amount</th></tr></thead>
        <tbody>${rows.join('')}</tbody>
      </table>
      ${record.notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${record.notes}</div></div>` : ''}
      <div class="payslip-signature">
        <div class="sig-block">
          <div class="sig-line"></div>
          <div class="sig-label">Employee Signature</div>
        </div>
      </div>
  <footer class="payslip-footer">Generated • ${new Date().toLocaleString()}</footer>
    </section>`;
  const docTitle = `SalaryPayment_${(emp.name||'Employee').replace(/[^a-z0-9-_]+/gi,'_')}_${record.date}`;
  const fullHtml = `<!DOCTYPE html><html><head><meta charset="utf-8" /><title>${docTitle}</title>
  <style>
    body { font-family: Inter, Arial, sans-serif; padding:16mm; color:#111827; }
    .payslip-header { display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:14px; }
    .payslip-header .title { font-size:24px; font-weight:800; letter-spacing:.5px; }
    .payslip-header .subtitle { font-size:12px; color:#6B7280; }
    .payslip-header .brand { font-size:22px; font-weight:800; color:#4F46E5; }
    .payslip-grid { display:grid; grid-template-columns: repeat(3, 1fr); gap:8px 16px; font-size:12px; margin:8px 0 10px; }
    .payslip-grid .label { color:#64748B; font-weight:500; }
    .payslip-grid .value { font-weight:600; color:#111827; }
    table { width:100%; border-collapse:collapse; font-size:12px; margin-top:4px; }
    th, td { border:1px solid #E5E7EB; padding:6px 8px; }
    thead th { background:#F3F4F6; color:#374151; text-align:left; }
    .total td { font-weight:700; background:#F8FAFC; }
    .payslip-notes { margin-top:12px; font-size:12px; border:1px solid #E5E7EB; background:#F9FAFB; padding:8px 10px; border-radius:6px; }
    .payslip-notes .label { font-weight:600; color:#475569; margin-bottom:4px; }
    .payslip-footer { margin-top:18px; font-size:10px; color:#64748B; text-align:right; }
    .payslip-signature { margin-top:16px; display:flex; gap:28px; }
    .payslip-signature .sig-block { width:260px; }
    .payslip-signature .sig-line { height:36px; border-bottom:1px solid #94A3B8; }
    .payslip-signature .sig-label { margin-top:6px; font-size:11px; color:#64748B; }
    .watermark { position:fixed; top:50%; left:50%; transform:translate(-50%, -50%); font-size:72px; font-weight:800; color:rgba(99,102,241,0.06); pointer-events:none; user-select:none; }
    @page { size:A4 portrait; margin:12mm; }
    @media print { body { padding:0; } }
  </style></head><body>
  
  ${htmlInner}
  </body></html>`;
  const iframe = document.createElement('iframe');
  iframe.style.position='fixed'; iframe.style.right='0'; iframe.style.bottom='0'; iframe.style.width='0'; iframe.style.height='0'; iframe.style.border='0';
  document.body.appendChild(iframe);
  try {
    const doc = iframe.contentDocument || iframe.contentWindow.document;
    doc.open(); doc.write(fullHtml); doc.close(); doc.title = docTitle;
    doc.fonts?.ready?.then(()=> { setTimeout(()=> { try { iframe.contentWindow?.focus(); iframe.contentWindow?.print(); } catch {} }, 150); });
  } catch (err) {
    console.error('Salary payment PDF export failed', err);
    showToast('Salary payment PDF export failed', 'error');
  }
  const cleanup = () => { try { document.body.removeChild(iframe); } catch {}; window.removeEventListener('afterprint', cleanup); };
  window.addEventListener('afterprint', cleanup);
}
try { window.exportProfessionalSalaryPaymentPdf = exportProfessionalSalaryPaymentPdf; } catch {}


// Render employee table
function renderEmployeeTable() {
  employeesRenderTable({
    getEmployees: () => employees,
    getSearchQuery: () => currentSearch,
    getDepartmentFilter: () => currentDepartmentFilter,
    getShowTerminated: () => true,
  });
}

// =====================
// Accounts Summary PDF Export (professional template)
// =====================
(function(){
  // Helpers
  // (Click handler moved to a simple bubbling listener alongside Ledger's, below)
  const fmtMoney = (n, currencyPrefix) => `${currencyPrefix || (typeof CURRENCY_PREFIX!=='undefined'?CURRENCY_PREFIX:'QAR ')}${Number(n||0).toLocaleString(undefined,{minimumFractionDigits:0, maximumFractionDigits:2})}`;
  const ymd = (d)=> (d||'').slice(0,10);
  const ym = (d)=> (d||'').slice(0,7);
  const ensureMonth = (val)=> {
    if (!val) return (()=>{const d=new Date(); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;})();
    if (/^\d{4}-\d{2}$/.test(val)) return val;
    if (/^\d{4}-\d{2}-\d{2}$/.test(val)) return val.slice(0,7);
    try { const d=new Date(val); if (!isNaN(d)) return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`; } catch {}
    return String(val).slice(0,7);
  };

  function getSelectedSummaryMonth() {
    // Try common IDs first
    const ids = ['summaryMonth','accountsSummaryMonth','driverPetrolMonth','ledgerMonth'];
    for (const id of ids) { const el=document.getElementById(id); if (el && el.value) return ensureMonth(el.value); }
    // Fallback: any visible month input under Accounts section
    try {
      const accountsSection = document.querySelector('#accountsSection, [data-accounts-section]') || document;
      const monthEl = accountsSection.querySelector('input[type="month"]');
      if (monthEl && monthEl.value) return ensureMonth(monthEl.value);
    } catch {}
    return ensureMonth('');
  }

  function getAllCashflows() {
    const list = Array.isArray(window.__cashflowAll) ? window.__cashflowAll : (window.__fundCashflowsCache || []);
    return Array.isArray(list) ? list : [];
  }

  function getLocalAccounts() {
    try { return (typeof __getLocalAccounts === 'function') ? (__getLocalAccounts() || []) : []; } catch { return []; }
  }

  function typeIsIn(t){ const s=String(t||'').toLowerCase(); return s==='in' || s==='income' || s==='credit'; }
  function typeIsOut(t){ const s=String(t||'').toLowerCase(); return s==='out' || s==='expense' || s==='debit'; }
  function isPetrolCategory(cat){ const s=String(cat||'').toLowerCase(); return s.includes('petrol') || s.includes('fuel'); }

  function summarizeAccountsByMonth(monthYm){
    const accs = getLocalAccounts().filter(a => String(a.type||'').toLowerCase()==='asset');
    const flows = getAllCashflows();
    const startYm = monthYm;
    const start = `${startYm}-01`;
    // Opening is base opening + net of all flows before start of month
    const rows = accs.map(a => {
      const id = a.id;
      const openingBase = Number(a.opening||0);
      let priorNet = 0, inSum=0, outSum=0;
      for (const t of flows) {
        if (t.accountId !== id) continue;
        const d = ymd(t.date||''); if (!d) continue;
        if (d < start) { if (typeIsIn(t.type)) priorNet += Number(t.amount||0); else if (typeIsOut(t.type)) priorNet -= Number(t.amount||0); continue; }
        if (ym(d) === startYm) { if (typeIsIn(t.type)) inSum += Number(t.amount||0); else if (typeIsOut(t.type)) outSum += Number(t.amount||0); }
      }
      const opening = openingBase + priorNet;
      const closing = opening + inSum - outSum;
      return { id, name: a.name || 'Account', opening, inSum, outSum, closing };
    });
    return rows;
  }

  function buildDriverPetrolSummary(monthYm){
    const flows = getAllCashflows();
    const rows = flows.filter(f => ym(ymd(f.date||''))===monthYm && typeIsOut(f.type) && isPetrolCategory(f.category));
    const byDriver = new Map();
    for (const t of rows) {
      const driverId = t.driverId || t.employeeId || 'unknown';
      const cur = byDriver.get(driverId) || { driverId, total:0, count:0, notes:[] };
      cur.total += Number(t.amount||0);
      cur.count += 1;
      if (t.notes) cur.notes.push(String(t.notes));
      byDriver.set(driverId, cur);
    }
    const nameFor = (id, notesHint)=>{
      if (!id || id==='unknown') {
        // Try to infer from notes first
        try { if (typeof inferDriverNameFromNotes === 'function') { const n = inferDriverNameFromNotes(notesHint||''); if (n) return n; } } catch {}
        return '\u2014';
      }
      // Prefer shared resolver used by on-screen table
      try { if (typeof resolveDriverName === 'function') { const n = resolveDriverName(id); if (n) return n; } } catch {}
      // Fall back to in-scope arrays (module vars), then window vars as last resort
      try {
        const list1 = Array.isArray(typeof employees!=='undefined'?employees:[]) ? employees : [];
        const list2 = Array.isArray(typeof temporaryEmployees!=='undefined'?temporaryEmployees:[]) ? temporaryEmployees : [];
        const found = [...list1, ...list2].find(e => e.id === id);
        if (found) return found.name || found.employeeName || '';
      } catch {}
      try {
        const w1 = Array.isArray(window.employees)? window.employees : [];
        const w2 = Array.isArray(window.temporaryEmployees)? window.temporaryEmployees : [];
        const found2 = [...w1, ...w2].find(e => e.id === id);
        if (found2) return found2.name || found2.employeeName || '';
      } catch {}
      // Try inference from notes as a last attempt
      try { if (typeof inferDriverNameFromNotes === 'function') { const n2 = inferDriverNameFromNotes(notesHint||''); if (n2) return n2; } } catch {}
      return '\u2014';
    };
    return Array.from(byDriver.values()).map(r => ({
      driver: nameFor(r.driverId, (r.notes||[]).join(' ')),
      total: r.total,
      count: r.count,
      avg: r.count ? (r.total / r.count) : 0
    })).sort((a,b)=> a.driver.localeCompare(b.driver));
  }

  function exportAccountsSummaryPdf() {
    const monthYm = getSelectedSummaryMonth();
    const currencyPrefix = (typeof CURRENCY_PREFIX!=='undefined'?CURRENCY_PREFIX:'QAR ');
    const accRows = summarizeAccountsByMonth(monthYm);
    const drvRows = buildDriverPetrolSummary(monthYm);

    const accTbody = accRows.length ? accRows.map(r => `
      <tr>
        <td>${escapeHtml(r.name)}</td>
        <td class="num">${escapeHtml(fmtMoney(r.opening, currencyPrefix))}</td>
        <td class="num">${escapeHtml(fmtMoney(r.inSum, currencyPrefix))}</td>
        <td class="num">${escapeHtml(fmtMoney(r.outSum, currencyPrefix))}</td>
        <td class="num">${escapeHtml(fmtMoney(r.closing, currencyPrefix))}</td>
      </tr>`).join('\n') : '<tr><td colspan="5" class="empty">No accounts data for this month</td></tr>';

    const drvTbody = drvRows.length ? drvRows.map(r => `
      <tr>
        <td>${escapeHtml(r.driver)}</td>
        <td class="num">${escapeHtml(fmtMoney(r.total, currencyPrefix))}</td>
        <td class="num">${r.count}</td>
        <td class="num">${escapeHtml(fmtMoney(r.avg, currencyPrefix))}</td>
      </tr>`).join('\n') : '<tr><td colspan="4" class="empty">No driver petrol data for this month</td></tr>';

    const html = `<!doctype html><html><head><meta charset="utf-8"/>
      <title>Accounts Summary — ${escapeHtml(monthYm)}</title>
      <style>
        :root{ --brand:#4F46E5; }
        body{ font-family: Inter, Arial, sans-serif; color:#0f172a; margin:24px; }
        .header{ display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:10px; }
        .brand{ font-size:20px; font-weight:800; color:var(--brand); letter-spacing:.4px; }
        .meta{ text-align:right; color:#475569; font-size:12px; }
        h2{ margin:12px 0 6px; font-size:16px; font-weight:800; letter-spacing:.3px; color:#111827; }
        table{ width:100%; border-collapse:separate; border-spacing:0; font-size:12px; }
        thead th{ background:#EEF2FF; color:#1e293b; font-weight:700; font-size:11px; padding:6px 8px; border:1px solid #e2e8f0; text-align:left; }
        tbody td{ padding:6px 8px; border:1px solid #e2e8f0; vertical-align:top; }
        tbody tr:nth-child(even){ background:#F8FAFC; }
        .num{ text-align:right; white-space:nowrap; font-feature-settings:"tnum"; }
        .empty{ text-align:center; color:#64748B; padding:10px; }
        .section{ margin-top:16px; }
        .footer{ margin-top:14px; color:#64748B; font-size:11px; display:flex; justify-content:space-between; align-items:center; }
        @media print{ @page { size:A4 portrait; margin:12mm; } }
      </style>
    </head><body>
      <div class="header">
        <div class="brand">Accounts Summary</div>
        <div class="meta">
          <div>Month: <strong>${escapeHtml(monthYm)}</strong></div>
          <div>${escapeHtml(new Date().toLocaleString())}</div>
        </div>
      </div>
      <div class="section">
        <h2>Accounts</h2>
        <table>
          <thead><tr><th>Account</th><th>Opening</th><th>In</th><th>Out</th><th>Closing</th></tr></thead>
          <tbody>${accTbody}</tbody>
        </table>
      </div>
      <div class="section">
        <h2>Driver Petrol Summary</h2>
        <table>
          <thead><tr><th>Driver</th><th>Total Petrol</th><th>Transactions</th><th>Avg/Txn</th></tr></thead>
          <tbody>${drvTbody}</tbody>
        </table>
      </div>
      <div class="footer"><div>CRM System Summary Report</div><div>Generated by web app</div></div>
    </body></html>`;

    try {
      // Use the same Blob + hidden iframe print pipeline as Ledger PDF
      const blob = new Blob([html], { type: 'text/html' });
      const url = URL.createObjectURL(blob);
      const iframe = document.createElement('iframe');
      iframe.style.position='fixed'; iframe.style.right='0'; iframe.style.bottom='0'; iframe.style.width='0'; iframe.style.height='0'; iframe.style.border='0';
      let printed = false;
      iframe.onload = () => {
        if (printed) return;
        printed = true;
        try {
          setTimeout(() => {
            try {
              // Attach afterprint handler to cleanup regardless of user action
              const cw = iframe.contentWindow;
              if (cw) {
                const cleanup = () => { try { URL.revokeObjectURL(url); iframe.remove(); } catch {} };
                try { cw.addEventListener('afterprint', () => cleanup(), { once: true }); } catch {}
              }
              cw?.focus();
              cw?.print();
            } catch {}
            // Safety cleanup in case afterprint doesn't fire
            setTimeout(() => { try { URL.revokeObjectURL(url); iframe.remove(); } catch {} }, 4000);
          }, 150);
        } catch {}
      };
      iframe.src = url;
      document.body.appendChild(iframe);
      try { showToast && showToast('Preparing PDF (use system Save as PDF)','info'); } catch {}
    } catch (e) {
      console.error('Summary PDF export failed', e);
      try { showToast && showToast('Summary PDF export failed','error'); } catch {}
    }
  }

  try { window.exportAccountsSummaryPdf = exportAccountsSummaryPdf; } catch {}

  // Auto-inject a PDF button on the Summary tab; robust anchors and observers
  function createSummaryPdfButton() {
    const pdfBtn = document.createElement('button');
    pdfBtn.id = 'summaryPdfBtn';
    pdfBtn.className = 'btn btn-secondary';
    pdfBtn.type = 'button';
    pdfBtn.innerHTML = '<i class="fas fa-file-pdf"></i> <span class="hidden sm:inline">PDF</span>';
    pdfBtn.addEventListener('click', (e)=>{ e.preventDefault(); try { exportAccountsSummaryPdf(); } catch (err) { console.error(err); } });
    return pdfBtn;
  }

  function findButtonByText(regex) {
    const nodes = Array.from(document.querySelectorAll('button, a, [role="button"]'));
    for (const n of nodes) {
      const txt = (n.textContent||'').replace(/\s+/g,' ').trim();
      if (regex.test(txt)) return n;
    }
    return null;
  }

  function attachSummaryPdfButton() {
    // If a static button exists or we've already injected, do nothing
    if (document.getElementById('accountsSummaryPdfBtn') || document.getElementById('summaryPdfBtn')) return true;
    // Prefer placing next to "Export Drivers"
    let anchor = findButtonByText(/export drivers/i);
    // Otherwise, place next to "Refresh"
    if (!anchor) anchor = findButtonByText(/^refresh$/i);
    if (!anchor) return false;
    const btn = createSummaryPdfButton();
    anchor.insertAdjacentElement('afterend', btn);
    return true;
  }

  function scheduleAttachSummaryBtn(retries=40, delay=300) {
    let attempts = 0; const iv = setInterval(()=>{ attempts++; if (attachSummaryPdfButton() || attempts >= retries) clearInterval(iv); }, delay);
  }

  // Run on DOM ready, and also observe future DOM updates (Summary UI is dynamic)
  document.addEventListener('DOMContentLoaded', ()=>{
    scheduleAttachSummaryBtn();
    try {
      const mo = new MutationObserver(() => { attachSummaryPdfButton(); });
      mo.observe(document.body, { childList: true, subtree: true });
    } catch {}
    // Re-run when user switches Accounts sub-tabs or clicks in Accounts area
    const relaunch = () => scheduleAttachSummaryBtn(20, 250);
    const maybeIds = ['accountsSubTabOverviewBtn','accountsSubTabLedgerBtn','accountsSubTabSettingsBtn'];
    maybeIds.forEach(id=>{ const el=document.getElementById(id); if (el && !el.__summaryPdfBound){ el.addEventListener('click', relaunch); el.__summaryPdfBound=true; } });
    // Button wiring mirrored to Ledger via document click listener below; no per-node binding needed here.
    document.addEventListener('click', (e)=>{
      const acc = e.target && e.target.closest && e.target.closest('#accountsSection, [data-accounts-section]');
      if (acc) relaunch();
    });
  });
})();

// Mirror Ledger PDF click handler: simple bubbling listener on document
document.addEventListener('click', (e) => {
  if (e.target && (e.target.id === 'accountsSummaryPdfBtn' || e.target.closest?.('#accountsSummaryPdfBtn'))) {
    e.preventDefault();
    try { exportAccountsSummaryPdf(); } catch (err) { console.error('Summary PDF click failed', err); }
  }
});

// View employee (read-only modal)
window.viewEmployee = async function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  let emp = list.find(e => e.id === id);
  if (!emp) return;
  // Always fetch a fresh copy to ensure we display the latest values (including nationality and expiries)
  try {
    const base = which === 'temporary' ? 'temporaryEmployees' : 'employees';
    const snap = await getDoc(doc(db, base, id));
    if (snap && snap.exists()) {
      const fresh = snap.data();
      // Normalize possible legacy/variant field names
      const qidEx = fresh.qidExpiry || fresh.qidExpiryDate || fresh.qatarIdExpiry || fresh.qid_expiry || fresh.qidExpire || fresh.qidExpiryDt || fresh.qidExpiryDateStr;
      const passEx = fresh.passportExpiry || fresh.passportExpiryDate || fresh.passport_expiry || fresh.passportExpire || fresh.passportExpiryDt || fresh.passportExpiryDateStr;
      const nat = (fresh.nationality || fresh.Nationality || fresh.nation || fresh.country || '').trim();
      emp = { ...emp, ...fresh, qidExpiry: emp.qidExpiry || qidEx, passportExpiry: emp.passportExpiry || passEx, nationality: nat || emp.nationality };
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

  const fmtCurrency = (n) => `${CURRENCY_PREFIX}${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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
  const natChip = byId('viewNationalityChip');
  if (natChip) {
    const nat = (emp.nationality && String(emp.nationality).trim()) || '';
    if (nat) { natChip.textContent = nat; natChip.style.display = ''; } else { natChip.style.display = 'none'; }
  }
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
    // Nationality: show value or a clear placeholder when not set
    try {
      const elNat = byId('viewNationality');
      const natVal = (emp.nationality && String(emp.nationality).trim()) || '';
      // Debug trace to verify values at runtime (safe/no-op for users)
      try { console.debug('ViewEmployee nationality:', { id: emp.id, which, natVal }); } catch {}
      if (elNat) {
        if (natVal) {
          elNat.textContent = natVal;
          elNat.classList.remove('text-gray-500');
        } else {
          elNat.textContent = 'Not set';
          elNat.classList.add('text-gray-500');
        }
      }
    } catch {}
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
    // First, check if the document has a URL saved in Firestore; if not, avoid hitting Storage to prevent 404 noise
    try {
      const { doc: fdoc, getDoc: fgetDoc } = await import('https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js');
      const { db } = await import('../firebase-config.js');
      const whichColl = w === 'temporary' ? 'temporaryEmployees' : 'employees';
      const dref = fdoc(db, whichColl, id);
      const dsnap = await fgetDoc(dref);
      if (dsnap.exists()) {
        const data = dsnap.data() || {};
        const urlField = kind === 'qid' ? 'qidPdfUrl' : (kind === 'passport' ? 'passportPdfUrl' : null);
        const hasUrl = urlField && typeof data[urlField] === 'string' && data[urlField].length > 0;
        if (hasUrl) {
          // Use saved download URL directly; avoid extra Storage lookups
          const url = String(data[urlField]);
          preview.src = url;
          preview.style.display = '';
          link.href = url;
          link.style.display = '';
          if (loading) loading.style.display = 'none';
          return;
        } else {
          // No URL saved; show empty and skip Storage calls
          preview.removeAttribute('src');
          preview.style.display = 'none';
          link.style.display = 'none';
          if (loading) loading.style.display = 'none';
          empty.style.display = '';
          return;
        }
      }
    } catch {}

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

// Render contractor table
function renderVehicleTable() {
  vehiclesRenderTable({
    getContractors: () => vehicles,
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
  avgEl.textContent = `${CURRENCY_PREFIX}${avgSalary.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
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

// (Removed duplicate formatDate; using imported helper from utils.js)

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
  // 0 Date (ro), 1 Assigned (ro), 2 Monthly (num), 3 Debited (num), 4 Credited (num), 5 Outstanding (num, now manually overridable), 6 Notes (text)
  return c === 2 || c === 3 || c === 4 || c === 5 || c === 6;
}
function __cb_isNumericCol(c) { return c === 2 || c === 3 || c === 4 || c === 5; }
function __cb_fmtCurrency(n) { return `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`; }
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
async function __cb_commitEdit(save) {
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
      if (field === 'debited') { try { __cb_reconcileDebitedAR(r); } catch {} }
      // Persist edit as standalone transaction so it doesn't get lost when a new row is created
      try {
        const ym = __clientBillingGridCtx?.ym || '';
        const clientSelect = document.getElementById('clientBillingClient');
        const clientId = clientSelect?.value || '';
  if (clientId && /^\d{4}-\d{2}$/.test(ym) && field && ['debited','credited','monthly','notes','outstanding'].includes(field)) {
          const mcell = __cb_getCell(r,2);
          const oc = __cb_getCell(r,5);
          const segBase = Number(oc?.getAttribute('data-prev-outstanding')||0);
          const deb = Number(bucket.debited||0);
          const cred = Number(bucket.credited||0);
          const mRaw = Number(bucket.monthly || __cb_parseNumber(mcell?.getAttribute('data-raw')||'0'));
          const mDef = Number(mcell?.getAttribute('data-default-monthly')||0);
          const effMonthly = mRaw>0? mRaw: mDef;
          const addsMonthly = oc?.getAttribute('data-segment-adds-monthly')==='1';
          // Debited should NOT change outstanding. In SIMPLE mode we skip recalculation for any non-outstanding field.
          let out;
          if (field === 'outstanding') {
            out = Math.max(0, __cb_parseNumber(toStore));
          } else if (window.__SIMPLE_BILLING_MODE) {
            // Preserve current displayed outstanding (no change)
            out = __cb_parseNumber(oc?.getAttribute('data-raw') || oc?.textContent || '0');
          } else {
            out = Math.max(0, segBase + (addsMonthly?effMonthly:0) - cred);
          }
          // Use today's date for edit log (not forced month-01) so chronological order is clear
          const now = new Date();
          const todayStr = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`;
          await addDoc(collection(db,'clientTransactions'), cleanData({
            clientId,
            month: ym,
            date: todayStr,
            field,
            value: __cb_isNumericCol(c) ? __cb_parseNumber(toStore) : text,
            prevValue: original,
            monthly: mRaw,
            debited: deb,
            credited: cred,
            // Persist the effective outstanding after this edit. For overrides we persist the override value itself.
            outstanding: out,
            notes: field==='notes'? text: undefined,
            createdAt: now.toISOString(),
            createdBy: auth?.currentUser?.uid || undefined,
            createdByEmail: auth?.currentUser?.email || undefined
          }));
        }
      } catch(e){ console.warn('Persist edit transaction failed', e); }
      try {
        const mcell = __cb_getCell(r,2);
        const oc = __cb_getCell(r,5);
        // Determine segment base: walk upwards to the closest row with data-segment-reset or the first row.
        let segBase = 0;
        for (let rr=r; rr>=0; rr--) {
          const baseCell = __cb_getCell(rr,5);
          if (!baseCell) continue;
            if (rr===0 || baseCell.hasAttribute('data-segment-reset')) {
              const raw = baseCell.getAttribute('data-prev-outstanding');
              segBase = Number(raw||0)||0;
              break;
            }
        }
        const deb = Number(bucket.debited||0);
        const cred = Number(bucket.credited||0);
        const mRaw = Number((bucket.monthly ?? __cb_parseNumber(mcell?.getAttribute('data-raw') || '0')) || 0);
        const mDef = Number(mcell?.getAttribute('data-default-monthly') || 0);
        const effMonthly = mRaw > 0 ? mRaw : mDef;
  const addsMonthly = oc?.getAttribute('data-segment-adds-monthly') === '1';
  const monthlyContribution = addsMonthly ? Number(effMonthly) : 0;
        let out;
        if (field === 'outstanding') {
          out = Math.max(0, __cb_parseNumber(toStore));
        } else if (window.__SIMPLE_BILLING_MODE) {
          out = __cb_parseNumber(oc?.getAttribute('data-raw') || oc?.textContent || '0');
        } else {
          out = Math.max(0, Number(segBase) + monthlyContribution - Number(cred));
        }
        if (oc && out != null) { oc.textContent = __cb_fmtCurrency(out); oc.setAttribute('data-raw', String(out)); }
        // Edits no longer auto-create new rows; they modify the current (latest) row view only.
      } catch {}
    }
  } else {
    if (__cb_isNumericCol(c)) display = __cb_fmtCurrency(__cb_parseNumber(original)); else display = original;
  }
  cell.textContent = display;
}
async function __cb_stopEdit(save) {
  if (!__clientBillingEditing) return;
  try { await __cb_commitEdit(save); } catch {}
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
        if (field === 'debited') { try { __cb_reconcileDebitedAR(r); } catch { console.warn('Debited reconcile failed'); } }
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
    // Prevent rapid duplicate postings (same client+month debited value within session)
    window.__debitedLastPosted ||= new Map(); // key: clientId|ym -> debited
    const rowMeta = __clientBillingGridCtx?.rows?.[rowIndex];
    const ym = __clientBillingGridCtx?.ym || '';
    if (!rowMeta || !rowMeta.clientId || !/^\d{4}-\d{2}$/.test(ym)) return;
    const key = rowMeta.key;
    const bucket = (__clientBillingGridCache[key] ||= {});
    const debited = Math.abs(Number(bucket.debited || 0)) || 0;
    const lpKey = rowMeta.clientId + '|' + ym;
    if (window.__debitedLastPosted.get(lpKey) === debited) {
      // Already reconciled this exact value; skip
      return;
    }
    // Resolve client name for human-friendly notes
    let clientName = '';
    try {
      const list = (typeof getClients === 'function') ? getClients() : (window.getClients ? window.getClients() : []);
      clientName = (list.find(x => x.id === rowMeta.clientId)?.name) || rowMeta.clientName || rowMeta.clientEmail || rowMeta.clientId || '';
    } catch {}
  // Ensure Fund (Asset) account exists; client debited must hit Fund as well
  const fundAdjId = await ensureAssetAccount('fund', 'Fund');
  if (!fundAdjId) return;
    // Upsert a single net debited row (always type=in). Remove old adjustment style noise.
    const qInv = query(collection(db,'cashflows'), where('clientId','==', rowMeta.clientId));
    const snap = await getDocs(qInv);
    let existingId=null;
  snap.forEach(docu=>{ const d=docu.data(); if((d.month||'')===ym && d.accountId===fundAdjId && String(d.category||'')==='Client Debited (Applied)' && String(d.type||'').toLowerCase()==='in') existingId=docu.id; });
    // Migrate and neutralize any legacy adjustment rows (best-effort)
    try {
      const batchFix = [];
      snap.forEach(docu=>{
        const d = docu.data();
        if ((d.month||'')!==ym) return;
        const cat = String(d.category||'');
        const typ = String(d.type||'').toLowerCase();
        if ((cat==='Fund Adjustment (Debited)' || cat==='Fund Adjustment (Debited Adjustment)') && d.accountId !== fundAdjId) {
          batchFix.push({ id: docu.id, data: d });
        }
      });
      for (const item of batchFix) {
        try {
          await updateDoc(doc(db,'cashflows', item.id), cleanData({ accountId: fundAdjId, accountName: 'Fund' }));
        } catch {}
      }
    } catch {}
    const today = new Date();
    const dateStr = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
    const accName='Fund';
    const payload = cleanData({
      date: dateStr,
      type: 'in',
      accountId: fundAdjId,
      accountName: accName,
      amount: debited,
      category: 'Client Debited (Applied)',
      clientId: rowMeta.clientId,
      month: ym,
      notes: `Debited update (net) — Client: ${clientName}`,
      updatedAt: today.toISOString(),
      createdBy: auth?.currentUser?.uid || undefined,
      createdByEmail: auth?.currentUser?.email || undefined
    });
  if (existingId) { payload.updatedAt = today.toISOString(); await updateDoc(doc(db,'cashflows', existingId), payload); }
  else { payload.createdAt = today.toISOString(); await addDoc(collection(db,'cashflows'), payload); }
    window.__debitedLastPosted.set(lpKey, debited);
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
  // Guard: if SIMPLE mode active, delegate to overridden simple version and skip legacy logic.
  if (window.__SIMPLE_BILLING_MODE && typeof window.renderClientTransactions === 'function' && window.renderClientTransactions !== renderClientTransactions) {
    return window.renderClientTransactions();
  }
  // Segment logic explanation:
  // - A 'segment' represents an isolated running calculation of outstanding for the current month.
  // - The first segment starts with basePrevOutstanding (carry-forward from prior months + current month debited/credited sums where appropriate).
  // - Each 'newRow' document starts a new segment whose base outstanding equals the stored outstanding in that newRow doc.
  // - Within a segment, edits to debited/credited/monthly only affect that segment's current snapshot (latest row) outstanding; earlier segment rows remain static.
  // - Payments (field='payment') increment credited only inside the segment they occur.
  // This ensures user adjustments do not retroactively change earlier rows' displayed outstanding.
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
  const fmt = (n) => `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
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
    const creditedByMonth = new Map(); // ym => total client payments
    if (snap) {
      snap.forEach(docu => {
        const d = docu.data(); const mon = String(d.month || ''); if (!/^\d{4}-\d{2}$/.test(mon)) return;
        const cat = String(d.category || ''); const typ = String(d.type || '').toLowerCase(); const amt = Math.abs(Number(d.amount || 0)) || 0;
        if (typ==='in' && cat==='Fund Adjustment (Debited)') debitedByMonth.set(mon, (debitedByMonth.get(mon)||0) + amt);
        if (typ==='out' && cat==='Fund Adjustment (Debited Adjustment)') debitedByMonth.set(mon, (debitedByMonth.get(mon)||0) - amt);
        if (typ==='in' && cat==='Client Payment') creditedByMonth.set(mon, (creditedByMonth.get(mon)||0) + amt);
      });
    }
    // Collect manual outstanding overrides for prior months so they become the new carry-forward baseline.
    const outstandingOverrideByMonth = new Map(); // ym => latest override value
    try {
      const txSnapAll = await getDocs(query(collection(db,'clientTransactions'), where('clientId','==', id)));
      txSnapAll.forEach(docu => {
        const data = docu.data();
        const mon = String(data.month || '');
        if (!/^\d{4}-\d{2}$/.test(mon)) return;
        if (mon >= ym) return; // only earlier months affect base carry-forward
        if (data.field === 'outstanding') {
          const val = Number(data.value != null ? data.value : data.outstanding);
            if (!isFinite(val)) return;
          // keep the latest by createdAt
          const prev = outstandingOverrideByMonth.get(mon);
          if (!prev || (prev._createdAt||'') < (data.createdAt||'')) {
            outstandingOverrideByMonth.set(mon, { value: val, _createdAt: data.createdAt||'' });
          }
        }
        // If a newRow snapshot has an outstanding and no explicit override doc for that month, we can treat the final locked snapshot as implicit baseline.
        // (We only need explicit override docs; snapshot outstanding is already product of formula or prior override.)
      });
    } catch {}
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
        const credForM = Math.max(0, Number(creditedByMonth.get(mYm)||0));
  // Updated logic: Debited no longer reduces Outstanding. Outstanding now = cumulative previous outstanding + monthly - credited.
        prevOutstanding = Math.max(0, prevOutstanding + monthlyForM - credForM);
        // Apply manual outstanding override if present for this month (becomes authoritative closing balance)
        const ov = outstandingOverrideByMonth.get(mYm);
        if (ov && typeof ov.value === 'number' && isFinite(ov.value)) {
          prevOutstanding = Math.max(0, ov.value);
        }
      }
    }
    prevOutstandingByClient.set(id, prevOutstanding);
  }

  // Build snapshot rows: base row + one row per clientTransactions doc (ascending)
  const gridRowsMeta = [];
  let finalHtmlParts = [];
  for (let baseIndex=0; baseIndex<rows.length; baseIndex++) {
    const { client, count } = rows[baseIndex];
  const clientId = client.id || client.clientId;
  const keyBase = `${clientId}|${ym}`;
    // Determine the client's creation date (YYYY-MM-DD) if available
    const clientCreationDateStr = (() => {
      try {
        const raw = client.createdAt;
        if (!raw) return '';
        let d = null;
        if (typeof raw === 'string') {
          const parsed = new Date(raw);
            if (!isNaN(parsed.getTime())) d = parsed;
        } else if (raw && typeof raw.toDate === 'function') {
          const td = raw.toDate();
          if (td instanceof Date && !isNaN(td.getTime())) d = td;
        } else if (raw && typeof raw.seconds === 'number') {
          const tsd = new Date(raw.seconds * 1000);
          if (!isNaN(tsd.getTime())) d = tsd;
        }
        if (!d) return '';
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth()+1).padStart(2,'0');
        const dd = String(d.getDate()).padStart(2,'0');
        return `${yyyy}-${mm}-${dd}`;
      } catch { return ''; }
    })();
    // Base (first) row date should reflect client creation date if present; fallback to month start
    const baseRowDateStr = clientCreationDateStr || monthDateStr;
    // Compute default monthly due
    let assignedMonthlySum = 0;
    try { for (const a of assignments) { if (!a || a.clientId !== clientId) continue; const rt=String(a.rateType||'').toLowerCase(); if (rt==='monthly' && activeForMonth(a)) assignedMonthlySum += Number(a.rate||0)||0; } } catch {}
    const clientMonthlyOverride = Number(client.monthly||0)||0;
    const defaultMonthly = clientMonthlyOverride>0?clientMonthlyOverride:assignedMonthlySum;
  const basePrevOutstanding = Math.max(0, Number(prevOutstandingByClient.get(clientId) || 0));
  const sums = sumsByClient.get(clientId) || { debited:0, credited:0 };
  // Determine whether there are explicit per-doc entries for debited/credited (so we don't double count with month aggregates)
  // We intentionally avoid using month aggregate credited for the first segment when payment docs exist; otherwise later payments would
  // retroactively alter historical (pre-newRow) snapshots because initialCredited included future payments.
  // Fallback: if no payment docs exist for this month (legacy), we seed initialCredited from sums.
  // Similarly for debited adjustments.
  let initialDebited = 0;
  let initialCredited = 0;
  // We will fill these after we load docs.
  let initialNotes = '';
  // Defensive initialization in case legacy path executes unexpectedly.
  let segmentMonthlyOverride = typeof segmentMonthlyOverride === 'number' ? segmentMonthlyOverride : 0;
  const effMonthlyBase = segmentMonthlyOverride>0? segmentMonthlyOverride: defaultMonthly;
  // Outstanding no longer subtracts debited. Formula: base + monthly - credited.
  let initialOutstanding = Math.max(0, segmentBaseOutstanding + effMonthlyBase - initialCredited);
  // Fetch transaction docs for this client/month
    let txnSnap = null; try { txnSnap = await getDocs(query(collection(db,'clientTransactions'), where('clientId','==', clientId), where('month','==', ym))); } catch {}
    const rawDocs = [];
    if (txnSnap) txnSnap.forEach(docu=> { rawDocs.push(docu.data()); });
    rawDocs.sort((a,b)=> (a.createdAt||'').localeCompare(b.createdAt||''));
  const hasPaymentDocs = rawDocs.some(d => d.field === 'payment');
  const hasDebitedDocs = rawDocs.some(d => d.field === 'debited');
  if (!hasPaymentDocs) initialCredited = Number(sums.credited||0); // legacy fallback
  if (!hasDebitedDocs) initialDebited = Number(sums.debited||0);   // legacy fallback
  // Segment state: a segment begins at month start (basePrevOutstanding) or a newRow document.
  let segmentBaseOutstanding = basePrevOutstanding; // carried forward starting outstanding
  segmentMonthlyOverride = 0; // Non-zero if user overrides monthly within this segment
  let segmentAddsMonthly = true; // only first segment adds monthly
    const today = new Date();
    const todayStr = `${today.getFullYear()}-${String(today.getMonth()+1).padStart(2,'0')}-${String(today.getDate()).padStart(2,'0')}`;
    // Normalize displayDate: if date is month-start placeholder but createdAt day !=1 use createdAt day
    const normDocs = rawDocs.map(d => {
      let displayDate = d.date || todayStr;
      try {
        if (/^\d{4}-\d{2}-01$/.test(displayDate) && d.createdAt) {
          const cd = new Date(d.createdAt);
            if (!isNaN(cd.getTime()) && cd.getFullYear() === Number(ym.slice(0,4)) && (cd.getMonth()+1) === Number(ym.slice(5,7)) && cd.getDate() !== 1) {
              const mm = String(cd.getMonth()+1).padStart(2,'0');
              const dd = String(cd.getDate()).padStart(2,'0');
              displayDate = `${cd.getFullYear()}-${mm}-${dd}`;
            }
        }
      } catch {}
      return { ...d, __displayDate: displayDate };
    });

    // === SAFETY: Simple immutable snapshot model (flag) ===
    const SIMPLE_BILLING_MODE = true;
    if (SIMPLE_BILLING_MODE) {
  const fmtMoney = (n)=> `${CURRENCY_PREFIX}${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      const paymentDocs = normDocs.filter(d=>d.field==='payment').sort((a,b)=>(a.createdAt||'').localeCompare(b.createdAt||''));
      const newRowDocsSimple = normDocs.filter(d=>d.field==='newRow').sort((a,b)=>(a.createdAt||'').localeCompare(b.createdAt||''));
      // Monthly due (baseline) from client override or assignment sum already computed: defaultMonthly
      const monthlyDue = defaultMonthly;
      // Build historical rows strictly from stored snapshot
      const hist = newRowDocsSimple.map(nr => {
        const snap = nr.snapshot || {};
        return {
          date: nr.__displayDate || nr.date || todayStr,
          monthly: Number(snap.monthly || nr.monthly || monthlyDue || 0) || 0,
          debited: Number(snap.debited||0)||0,
          credited: Number(snap.credited||0)||0,
          outstanding: Number(snap.outstanding||0)|| Math.max(0,(Number(snap.monthly||nr.monthly||monthlyDue||0)||0) - (Number(snap.credited||0)||0)),
          notes: String(snap.notes || nr.notes || ''),
          locked: true
        };
      });
      // Determine base outstanding for active segment
      const lastSnapshot = hist.length ? hist[hist.length-1] : null;
      // Payments after last snapshot
      const lastSnapshotCreatedAt = newRowDocsSimple.length ? (newRowDocsSimple[newRowDocsSimple.length-1].createdAt||'') : '';
      const activePayments = paymentDocs.filter(p => (p.createdAt||'') > lastSnapshotCreatedAt);
      const activePaymentSum = activePayments.reduce((s,p)=> s + Math.abs(Number(p.value||0)||0), 0);
      // Active credited resets to 0 at segment start; accumulate only new payments
      const activeBaseOutstanding = lastSnapshot ? lastSnapshot.outstanding : Math.max(0, monthlyDue - (paymentDocs.reduce((s,p)=> s + Math.abs(Number(p.value||0)||0),0)) );
      // If no snapshot yet, baseOutstanding should be monthlyDue - all payments so far (mirrors single-row behavior)
      let activeOutstanding;
      if (lastSnapshot) {
        activeOutstanding = Math.max(0, activeBaseOutstanding - activePaymentSum);
      } else {
        // In first segment activeOutstanding already computed above as baseOutstanding; no further subtraction (we already subtracted all payments)
        activeOutstanding = activeBaseOutstanding;
      }
      const activeRow = {
        date: todayStr,
        monthly: monthlyDue,
        debited: 0,
        credited: lastSnapshot ? activePaymentSum : paymentDocs.reduce((s,p)=> s + Math.abs(Number(p.value||0)||0),0),
        outstanding: activeOutstanding,
        notes: '',
        locked: false
      };
      // When there is at least one snapshot, active credited shows only payments after last snapshot; otherwise all payments.
      const displayRows = hist.concat([activeRow]);
      displayRows.forEach((rowObj, idxR) => {
        const rowKey = `${keyBase}|${idxR}`;
        gridRowsMeta.push({ key: rowKey, clientId });
        const rIndex = gridRowsMeta.length - 1;
        const td = (cIndex, text, raw, editable, field) => `\n      <td class="px-3 py-2 grid-cell ${editable?'' : 'readonly'}" data-row="${rIndex}" data-col="${cIndex}" ${field?`data-field=\"${field}\"`:''} data-raw="${raw}">${text}</td>`;
        finalHtmlParts.push(`\n        <tr class="${rowObj.locked?'opacity-95':''}">\n          ${td(0, escapeHtml(rowObj.date), rowObj.date, false, '')}\n          ${td(1, String(count), String(count), false, '')}\n          ${td(2, rowObj.monthly?fmtMoney(rowObj.monthly):'-', String(rowObj.monthly||0), (!rowObj.locked), 'monthly')}\n          ${td(3, fmtMoney(rowObj.debited), String(rowObj.debited||0), (!rowObj.locked), 'debited')}\n          ${td(4, fmtMoney(rowObj.credited), String(rowObj.credited||0), (!rowObj.locked), 'credited')}\n          ${td(5, fmtMoney(rowObj.outstanding), String(rowObj.outstanding||0), false, '')}\n          ${td(6, escapeHtml(rowObj.notes||''), rowObj.notes||'', (!rowObj.locked), 'notes')}\n        </tr>`);
      });
      tbody.innerHTML = finalHtmlParts.join('');
      tbody.querySelectorAll('td').forEach(el=> el.classList.add('grid-cell'));
      setupClientBillingGrid(tbody, gridRowsMeta, ym);
      continue; // proceed next client
    }
    const snapshots = [];
    // ===== Snapshot-Based Historical Rows + Isolated Active Row =====
    const newRowDocs = normDocs.filter(d => d.field === 'newRow').sort((a,b)=>(a.createdAt||'').localeCompare(b.createdAt||''));
    // Build historical locked rows directly from stored snapshots (authoritative, immutable)
    newRowDocs.forEach(nr => {
      const snap = nr.snapshot || {};
      snapshots.push({
        monthlyOverride: Number(snap.monthly||nr.monthly||0)||0,
        debited: Number(snap.debited||0)||0,
        credited: Number(snap.credited||0)||0,
        notes: String(snap.notes || nr.notes || ''),
        outstanding: Number(snap.outstanding||0)||0,
        frozenMonthly: Number(snap.monthly||nr.monthly||0)||0,
        locked: true,
        __editable: false,
        date: nr.__displayDate || nr.date || todayStr
      });
    });
    // Active docs = everything AFTER last newRow (strictly greater createdAt) excluding newRow docs themselves
    const lastSnapshotTime = newRowDocs.length ? (newRowDocs[newRowDocs.length-1].createdAt||'') : '';
    const activeDocs = normDocs.filter(d => d.field !== 'newRow' && (d.createdAt||'') > lastSnapshotTime);
    // Derive active row values
    const paymentsActive = activeDocs.filter(d => d.field==='payment');
    const paymentSumActive = paymentsActive.reduce((s,d)=> s + Math.abs(Number(d.value||0)||0), 0);
    const monthlyOverrideDoc = [...activeDocs.filter(d=>d.field==='monthly')].pop();
    const creditedEditDoc = [...activeDocs.filter(d=>d.field==='credited')].pop();
    const debitedEditDoc = [...activeDocs.filter(d=>d.field==='debited')].pop();
    const notesDoc = [...activeDocs.filter(d=>d.field==='notes')].pop();
    const outstandingOverrideDoc = [...activeDocs.filter(d=>d.field==='outstanding')].pop();
    const activeMonthlyOverride = monthlyOverrideDoc ? Number(monthlyOverrideDoc.value||0)||0 : 0;
    const activeEffMonthly = activeMonthlyOverride>0? activeMonthlyOverride: defaultMonthly;
    const monthlyContributionActive = newRowDocs.length ? 0 : activeEffMonthly; // only if no previous snapshot
    const activeCredited = creditedEditDoc ? Number(creditedEditDoc.value||0)||0 : paymentSumActive;
    const activeDebited = debitedEditDoc ? Number(debitedEditDoc.value||0)||0 : 0; // display only
    const activeBase = newRowDocs.length ? Number(newRowDocs[newRowDocs.length-1].outstanding||0)||0 : basePrevOutstanding;
    const computedActiveOutstanding = Math.max(0, activeBase + monthlyContributionActive - activeCredited);
    const activeOutstanding = outstandingOverrideDoc ? Math.max(0, Number(outstandingOverrideDoc.value||outstandingOverrideDoc.outstanding||0)||0) : computedActiveOutstanding;
    const activeNotes = notesDoc ? String(notesDoc.notes || notesDoc.value || '') : '';
    snapshots.push({
      monthlyOverride: activeMonthlyOverride,
      debited: activeDebited,
      credited: activeCredited,
      notes: activeNotes,
      outstanding: activeOutstanding,
      frozenMonthly: activeEffMonthly,
      locked: false,
      __editable: true,
      date: todayStr
    });
    // Render snapshots; prev-outstanding for each row is the segment base outstanding.
    snapshots.forEach((snap, idx) => {
      const isLast = snap.__editable === true;
      const debited = snap.debited || 0;
      const credited = snap.credited || 0;
      const outstanding = snap.outstanding || 0;
      // For historical locked rows, show the frozen monthly exactly as captured; do not substitute defaultMonthly.
      let monthlyDisplay, monthlyRaw;
      if (snap.locked) {
        monthlyDisplay = snap.frozenMonthly ? fmt(snap.frozenMonthly) : (snap.frozenMonthly === 0 ? '-' : fmt(0));
        monthlyRaw = snap.frozenMonthly;
      } else {
        const effMonthly = snap.monthlyOverride>0? snap.monthlyOverride: defaultMonthly;
        monthlyDisplay = effMonthly ? fmt(effMonthly) : '-';
        monthlyRaw = snap.monthlyOverride || 0;
      }
      const notes = snap.notes || '';
      const rowKey = `${keyBase}|${idx}`;
      gridRowsMeta.push({ key: rowKey, clientId });
      const rowIndex = gridRowsMeta.length - 1;
      const td = (cIndex, text, raw, editable, field, extraCls='', extraAttrs='') => `\n      <td class="px-3 py-2 grid-cell ${editable?'' : 'readonly'} ${extraCls}" data-row="${rowIndex}" data-col="${cIndex}" ${field?`data-field="${field}"`:''} ${raw!==undefined?`data-raw="${raw}"`:''} ${extraAttrs}>${text}</td>`;
      // For locked rows, prev-outstanding is their own outstanding (frozen); for active row it's the carried forward base
      const prevBaseVal = snap.locked ? outstanding : activeBaseOutstanding;
      const segmentBaseAttr = `data-prev-outstanding="${prevBaseVal}"`;
      const addsMonthlyAttr = `data-segment-adds-monthly="${(!snap.locked && !monthlyAlreadyApplied)? '1':'0'}"`;
      const rowLocked = snap.locked && !isLast; // locked historical snapshot
  const dateCellValue = snap.date || todayStr;
      finalHtmlParts.push(`\n        <tr class="${rowLocked?'opacity-95':''}">\n          ${td(0, escapeHtml(dateCellValue), dateCellValue, false, '', 'text-left')}\n          ${td(1, Number(count).toLocaleString(), String(Number(count)||0), false, '', 'text-left')}\n          ${td(2, monthlyDisplay, String(monthlyRaw||0), (!rowLocked)&&isLast, 'monthly', 'text-right', `data-default-monthly="${defaultMonthly}"`)}\n          ${td(3, fmt(debited), String(debited||0), (!rowLocked)&&isLast, 'debited', 'text-right')}\n          ${td(4, fmt(credited), String(credited||0), (!rowLocked)&&isLast, 'credited', 'text-right')}\n          ${td(5, fmt(outstanding), String(outstanding||0), false, '', 'text-right', `${segmentBaseAttr} ${addsMonthlyAttr}`)}\n          ${td(6, escapeHtml(notes), notes, (!rowLocked)&&isLast, 'notes', 'text-left')}\n        </tr>`);
    });
  }
  tbody.innerHTML = finalHtmlParts.join('');
  tbody.querySelectorAll('td').forEach(el=> el.classList.add('grid-cell'));
  setupClientBillingGrid(tbody, gridRowsMeta, ym);
}
