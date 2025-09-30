import { db, auth, storage } from './firebase-config.js?v=20250929-05';
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
} from './modules/payroll.js?v=20250929-12';
// bump cache
// Utilities used in this file (masking account numbers in Payroll modal)
import { maskAccount } from './modules/utils.js?v=20250929-08';
import { renderEmployeeTable as employeesRenderTable, sortEmployees } from './modules/employees.js?v=20250929-10';
import { renderTemporaryTable as temporaryRenderTable, sortTemporary } from './modules/temporary.js?v=20250929-09';
import { initClients, subscribeClients, renderClientsTable, getClients } from './modules/clients.js?v=20250930-05';
import { initAssignments, subscribeAssignments, renderAssignmentsTable, getAssignments } from './modules/assignments.js?v=20250930-03';
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
      // Ensure cashflow tab is visible
      try { setAccountsSubTab('transactions'); } catch {}
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
    try {
      const trTab = document.getElementById('accountsTabTransactions');
      if (trTab && trTab.style.display !== 'none') {
        populateCategoryFilter?.(document.getElementById('cashflowCategoryFilter'));
        renderCashflowTable?.();
        postFilterCashflowTable?.();
      }
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
    initClients({ db, collection, query, onSnapshot, addDoc, serverTimestamp, showToast, cleanData });
    subscribeClients();
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
        renderClientsTable();
      } else if (target === 'clients-billing') {
        // Default month to current if empty and render client transactions view
        try {
          const m = document.getElementById('clientBillingMonth');
          if (m && !m.value) {
            const now = new Date();
            m.value = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
          }
        } catch {}
        try { renderClientTransactions(); } catch {}
      } else if (target === 'assignments') {
        renderAssignmentsTable();
      } else if (target === 'accounts') {
        renderAccountsTable();
        renderCashflowTable?.();
        try { renderLedgerTable?.(); } catch {}
        // ensure default sub-tab shown and controls visibility updated
        setAccountsSubTab('overview');
        // re-wire in case this section was hidden before
        try { wireAccountsTabButtons(); } catch {}
        try { updateAccountsFundCard(); } catch {}
      }
    });
  });

  // Edit Fund removed: no manual adjustments; fund is transaction-driven

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
  ['employeeModal', 'deleteModal', 'viewModal', 'payrollModal', 'payslipModal', 'paymentModal'].forEach((id) => {
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
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl) && !anyOpen(paymentModalEl) && !anyOpen(payslipPreviewModalEl)) return;

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
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl) && !anyOpen(paymentModalEl) && !anyOpen(payslipPreviewModalEl)) return;

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
      if (deleteModalEl && deleteModalEl.classList.contains('show')) closeModal();
      if (employeeModalEl && employeeModalEl.classList.contains('show')) closeEmployeeModal();
      if (viewModalEl && viewModalEl.classList.contains('show')) closeViewModal();
      if (payrollModalEl && payrollModalEl.classList.contains('show')) closePayrollModal();
      if (payslipModalEl && payslipModalEl.classList.contains('show')) closePayslipModal();
      if (paymentModalEl2 && paymentModalEl2.classList.contains('show')) closePaymentModal();
      if (payslipPreviewModalEl2 && payslipPreviewModalEl2.classList.contains('show')) closePayslipPreviewModal();
    }
  });

  // Track modal visibility to toggle body.modal-open for overlay/scroll control
  try {
    const body = document.body;
  const modalIds = ['employeeModal','deleteModal','viewModal','payrollModal','payslipModal','paymentModal','payslipPreviewModal','transferModal','cashTxnModal','clientModal','assignmentModal'];
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

  // Payroll Frame controls
  const payrollMonthEl = document.getElementById('payrollMonth');
  if (payrollMonthEl) {
    // Default to current month
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    payrollMonthEl.value = payrollMonthEl.value || ym;
    payrollMonthEl.addEventListener('change', () => renderPayrollFrame());
  }
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

  // Auto-refresh Client Transactions when clients/assignments change
  document.addEventListener('clients:updated', () => {
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
      // Ensure cashflow tab is visible for context
      setAccountsSubTab('transactions');
    } catch {}
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
}

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

// Accounts sub-tabs (redesigned) — Overview, Transactions, Ledger, Settings
document.addEventListener('click', (e) => {
  const btn = e.target && e.target.closest && e.target.closest('#accountsSubTabOverviewBtn, #accountsSubTabTransactionsBtn, #accountsSubTabLedgerBtn, #accountsSubTabSettingsBtn');
  if (!btn) return;
  let which = 'overview';
  if (btn.id === 'accountsSubTabTransactionsBtn') which = 'transactions';
  if (btn.id === 'accountsSubTabLedgerBtn') which = 'ledger';
  if (btn.id === 'accountsSubTabSettingsBtn') which = 'settings';
  setAccountsSubTab(which);
});

function setAccountsSubTab(which) {
  const tabs = {
    overview: document.getElementById('accountsTabOverview'),
    transactions: document.getElementById('accountsTabTransactions'),
    ledger: document.getElementById('accountsTabLedger'),
    settings: document.getElementById('accountsTabSettings'),
  };
  const btns = {
    overview: document.getElementById('accountsSubTabOverviewBtn'),
    transactions: document.getElementById('accountsSubTabTransactionsBtn'),
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
  } else if (which === 'transactions') {
    try { renderCashflowTable?.(); } catch {}
    try { wireTransactionsFilters(); } catch {}
    try { postFilterCashflowTable(); } catch {}
  } else if (which === 'ledger') {
    try { refreshLedgerAccounts?.(); } catch {}
    try { renderLedgerTable?.(); } catch {}
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
  const ids = ['accountsSubTabOverviewBtn','accountsSubTabTransactionsBtn','accountsSubTabLedgerBtn','accountsSubTabSettingsBtn'];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (el && !el.__wired) {
      el.addEventListener('click', (e) => {
        e.preventDefault();
        const which = id.includes('Overview') ? 'overview' : id.includes('Transactions') ? 'transactions' : id.includes('Ledger') ? 'ledger' : 'settings';
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
  for (const t of flows) {
    const typ = String(t.type||'').toLowerCase();
    const amt = Math.abs(Number(t.amount||0))||0;
    const d = String(t.date||'');
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
}

// Transactions tab: date range and category filters + export CSV
function wireTransactionsFilters() {
  const startEl = document.getElementById('cashflowStartDate');
  const endEl = document.getElementById('cashflowEndDate');
  const catEl = document.getElementById('cashflowCategoryFilter');
  const exportBtn = document.getElementById('exportCashflowCsvBtn');
  if (startEl && !startEl.__wired) { startEl.addEventListener('change', () => { renderCashflowTable?.(); postFilterCashflowTable(); }); startEl.__wired = true; }
  if (endEl && !endEl.__wired) { endEl.addEventListener('change', () => { renderCashflowTable?.(); postFilterCashflowTable(); }); endEl.__wired = true; }
  if (catEl && !catEl.__wired) { catEl.addEventListener('change', () => { renderCashflowTable?.(); postFilterCashflowTable(); }); catEl.__wired = true; populateCategoryFilter(catEl); }
  if (exportBtn && !exportBtn.__wired) { exportBtn.addEventListener('click', exportCashflowCsv); exportBtn.__wired = true; }
}

function populateCategoryFilter(selectEl) {
  const flows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll : [];
  const cats = Array.from(new Set(flows.map(f => (f.category||'').trim()).filter(Boolean))).sort();
  const opts = ['<option value="">All Categories</option>'].concat(cats.map(c=>`<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`));
  selectEl.innerHTML = opts.join('');
}

// Post-filter cashflow table rows in DOM based on date range/category filters
function postFilterCashflowTable() {
  try {
    const start = document.getElementById('cashflowStartDate')?.value || '';
    const end = document.getElementById('cashflowEndDate')?.value || '';
    const cat = document.getElementById('cashflowCategoryFilter')?.value || '';
    const tbody = document.getElementById('cashflowTableBody');
    const rows = tbody ? Array.from(tbody.querySelectorAll('tr')) : [];
    let inSum = 0, outSum = 0;
    rows.forEach(tr => {
      const tds = tr.querySelectorAll('td');
      const d = tds[0]?.textContent || '';
      const typ = (tds[2]?.textContent || '').toLowerCase();
      const catTxt = (tds[3]?.textContent || '').trim();
      const amtTxt = (tds[4]?.textContent || '').replace(/[^0-9.\-]/g,'');
      const amt = Math.abs(Number(amtTxt)||0);
      let ok = true;
      if (start && d < start) ok = false;
      if (end && d > end) ok = false;
      if (cat && catTxt !== cat) ok = false;
      tr.style.display = ok ? '' : 'none';
      if (ok) { if (typ==='in') inSum += amt; else if (typ==='out') outSum += amt; }
    });
    const sumEl = document.getElementById('cashflowSummary');
    if (sumEl) {
      const fmt=(n)=>`$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
      sumEl.textContent = `In: ${fmt(inSum)} • Out: ${fmt(outSum)} • Net: ${fmt(inSum-outSum)}`;
    }
  } catch {}
}

function exportCashflowCsv() {
  const flows = Array.isArray(window.__cashflowAll) ? window.__cashflowAll.slice() : [];
  const start = document.getElementById('cashflowStartDate')?.value || '';
  const end = document.getElementById('cashflowEndDate')?.value || '';
  const accId = document.getElementById('cashflowAccountFilter')?.value || '';
  const cat = document.getElementById('cashflowCategoryFilter')?.value || '';
  const rows = flows.filter(f => {
    if (start && (f.date||'') < start) return false;
    if (end && (f.date||'') > end) return false;
    if (accId && (f.accountId||'') !== accId) return false;
    if (cat && (f.category||'') !== cat) return false;
    return true;
  }).sort((a,b)=>String(a.date||'').localeCompare(String(b.date||'')));
  const head = ['Date','Account','Type','Category','Amount','Notes'];
  const accs = __getLocalAccounts();
  const accName = (id)=> (accs.find(a=>a.id===id)?.name || '');
  const csv = [head].concat(rows.map(r=>[
    r.date||'',
    accName(r.accountId),
    (String(r.type||'').toLowerCase()==='in'?'In':'Out'),
    r.category||'',
    Number(r.amount||0),
    (r.notes||'').replace(/\n/g,' ')
  ])).map(cols=>cols.map(quoteCsv).join(',')).join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href=url; a.download=`cashflows_${new Date().toISOString().slice(0,10)}.csv`; a.click();
  setTimeout(()=>URL.revokeObjectURL(url), 2000);
}

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
  for (const t of flows) {
    if (!t) continue;
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
  // computedFundLabel removed with Edit Fund modal
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
    let inSum = 0, outSum = 0;
    for (const t of flows) {
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

// Edit Fund modal removed: manual fund adjustments disabled

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
function renderPayrollFrame() {
  payrollRenderFrame({
    getEmployees: () => employees,
    getTemporaryEmployees: () => temporaryEmployees,
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
  const employee = list.find(emp => emp.id === id);
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
    setVal('phone', employee.phone || '');
  setVal('bankName', employee.bankName || '');
  setVal('bankAccountNumber', employee.bankAccountNumber || '');
  setVal('bankIban', employee.bankIban || '');

    // PDF statuses (buttons removed from form)
    const qidStatus = document.getElementById('qidPdfStatus');
    const passStatus = document.getElementById('passportPdfStatus');
    if (qidStatus) qidStatus.textContent = employee.qidPdfUrl ? 'Uploaded' : '';
    if (passStatus) passStatus.textContent = employee.passportPdfUrl ? 'Uploaded' : '';
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
  const emp = list.find(e => e.id === id);
  if (!emp) return;
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
function renderClientTransactions() {
  const monthEl = document.getElementById('clientBillingMonth');
  const tbody = document.getElementById('clientBillingTableBody');
  const empty = document.getElementById('clientBillingEmpty');
  if (!monthEl || !tbody || !empty) return;
  const ym = monthEl.value || '';
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

  const rows = Array.from(byClient.values()).sort((A,B)=> (A.client.name||'').localeCompare(B.client.name||''));
  if (!rows.length) {
    tbody.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  const fmt = (n) => `$${Number(n||0).toLocaleString(undefined,{maximumFractionDigits:2})}`;
  tbody.innerHTML = rows.map(({ client, count }) => {
    // Placeholder for Monthly Amount until rate/invoice logic is defined
    const monthly = 0;
    const notes = '';
    return `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-2 font-semibold text-gray-900">${escapeHtml(client.name || '')}</td>
        <td class="px-4 py-2">${escapeHtml(client.email || '')}</td>
        <td class="px-4 py-2">${escapeHtml(client.phone || '-')}</td>
        <td class="px-4 py-2">${Number(count).toLocaleString()}</td>
        <td class="px-4 py-2 text-right">${monthly ? fmt(monthly) : '-'}</td>
        <td class="px-4 py-2">${escapeHtml(notes)}</td>
      </tr>
    `;
  }).join('');
}
