import { db, auth, storage } from './firebase-config.js?v=20250929-04';
import {
  collection,
  addDoc,
  doc,
  updateDoc,
  deleteDoc,
  onSnapshot,
  query,
  orderBy
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
} from './modules/payroll.js?v=20250929-07';
// Utilities used in this file (masking account numbers in Payroll modal)
import { maskAccount } from './modules/utils.js?v=20250929-07';
import { renderEmployeeTable as employeesRenderTable, sortEmployees } from './modules/employees.js?v=20250929-07';
import { renderTemporaryTable as temporaryRenderTable, sortTemporary } from './modules/temporary.js?v=20250929-07';

let employees = [];
let temporaryEmployees = [];
// Sorting is handled by modules (employees/temporary)
let deleteEmployeeId = null;
let currentSearch = '';
let currentDepartmentFilter = '';
let unsubscribeEmployees = null;
let unsubscribeTemporary = null;
let authed = false;
let authInitialized = false;
let payrollInited = false;
// Track current View modal context to lazy-load documents and manage blob URLs
let currentViewCtx = { id: null, which: 'employees', docsLoaded: false, revoke: [] };
// Track which payroll sub-tab is active: 'table' or 'report' (always default to table)
let currentPayrollSubTab = 'table';
// Track current payroll view data for payslip
let currentPayrollView = null;

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
      employees = [];
      temporaryEmployees = [];
      renderEmployeeTable();
      renderTemporaryTable();
  renderPayrollTable();
      updateStats();
      updateDepartmentFilter();
      // Reset section visibility for next sign-in
      setActiveSection('dashboard');
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
    if (v !== undefined) out[k] = v;
  }
  return out;
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
      }
    });
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
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl2) && !anyOpen(paymentModalEl)) return;

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
    }
  }, true);

  // Use pointerdown (captures mouse/touch/pen) to close even if click is prevented
  document.addEventListener('pointerdown', (e) => {
  const employeeModalEl = document.getElementById('employeeModal');
  const deleteModalEl = document.getElementById('deleteModal');
  const viewModalEl = document.getElementById('viewModal');
  const payrollModalEl = document.getElementById('payrollModal');
  const payslipModalEl2 = document.getElementById('payslipModal');
  const anyOpen = (el) => el && el.classList.contains('show');
  if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl) && !anyOpen(payrollModalEl) && !anyOpen(payslipModalEl2)) return;

    // Close when pointerdown lands on overlay element itself
    if (anyOpen(employeeModalEl) && e.target === employeeModalEl) {
      closeEmployeeModal();
    } else if (anyOpen(deleteModalEl) && e.target === deleteModalEl) {
      closeModal();
    } else if (anyOpen(viewModalEl) && e.target === viewModalEl) {
      closeViewModal();
    } else if (anyOpen(payrollModalEl) && e.target === payrollModalEl) {
      closePayrollModal();
    } else if (anyOpen(payslipModalEl2) && e.target === payslipModalEl2) {
      closePayslipModal();
    } else if (anyOpen(paymentModalEl) && e.target === paymentModalEl) {
      closePaymentModal();
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
      if (deleteModalEl && deleteModalEl.classList.contains('show')) closeModal();
      if (employeeModalEl && employeeModalEl.classList.contains('show')) closeEmployeeModal();
      if (viewModalEl && viewModalEl.classList.contains('show')) closeViewModal();
      if (payrollModalEl && payrollModalEl.classList.contains('show')) closePayrollModal();
      if (payslipModalEl && payslipModalEl.classList.contains('show')) closePayslipModal();
      if (paymentModalEl2 && paymentModalEl2.classList.contains('show')) closePaymentModal();
    }
  });

  // Payroll Frame controls
  const payrollMonthEl = document.getElementById('payrollMonth');
  if (payrollMonthEl) {
    // Default to current month
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    payrollMonthEl.value = payrollMonthEl.value || ym;
    payrollMonthEl.addEventListener('change', () => renderPayrollFrame());
  }
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
  if (addBtn && addTempBtn) {
    if (key === 'temporary') {
      addBtn.style.display = 'none';
      addTempBtn.style.display = '';
    } else if (key === 'employees') {
      addBtn.style.display = '';
      addTempBtn.style.display = 'none';
    } else {
      // On dashboard, hide both
      addBtn.style.display = 'none';
      addTempBtn.style.display = 'none';
    }
  }

  // When navigating to Payroll, ensure the table/frame are freshly rendered
  if (key === 'payroll') {
    // Always open Payroll on the Table sub-tab
    setPayrollSubTab('table');
    renderPayrollTable();
  }
}

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
};

window.closePayrollModal = function () {
  const modal = document.getElementById('payrollModal');
  if (modal) modal.classList.remove('show');
  currentPayrollView = null;
};

// Payslip Form modal controls
window.openPayslipForm = function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const emp = list.find(e => e.id === id);
  if (!emp) { showToast('Employee not found for payslip', 'error'); return; }

  // Prefill form fields
  const nameEl = document.getElementById('psEmployeeName');
  const typeEl = document.getElementById('psEmployeeType');
  const periodEl = document.getElementById('psPeriod');
  const basicEl = document.getElementById('psBasic');
  const allowEl = document.getElementById('psAllowances');
  const dedEl = document.getElementById('psDeductions');
  const notesEl = document.getElementById('psNotes');
  if (nameEl) nameEl.value = emp.name || '';
  if (typeEl) typeEl.value = which === 'temporary' ? 'Temporary' : 'Permanent';
  if (basicEl) basicEl.value = Number(emp.salary || 0);
  if (allowEl) allowEl.value = Number(allowEl?.value || 0);
  if (dedEl) dedEl.value = Number(dedEl?.value || 0);
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
  if (!emp) { showToast('Employee not found for payment', 'error'); return; }

  const nameEl = document.getElementById('payEmployeeName');
  const typeEl = document.getElementById('payEmployeeType');
  const dateEl = document.getElementById('payDate');
  const amountEl = document.getElementById('payAmount');
  const methodEl = document.getElementById('payMethod');
  const refEl = document.getElementById('payRef');
  const advEl = document.getElementById('payIsAdvance');
  const deductEl = document.getElementById('payDeductFrom');
  const notesEl = document.getElementById('payNotes');
  if (nameEl) nameEl.value = emp.name || '';
  if (typeEl) typeEl.value = which === 'temporary' ? 'Temporary' : 'Permanent';
  if (dateEl) dateEl.valueAsDate = new Date();
  if (amountEl) amountEl.value = '';
  if (methodEl) methodEl.value = 'cash';
  if (refEl) refEl.value = '';
  if (advEl) advEl.value = 'no';
  if (deductEl) deductEl.value = 'none';
  if (notesEl) notesEl.value = '';

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
  const allowances = Number(document.getElementById('psAllowances')?.value || 0);
  const deductions = Number(document.getElementById('psDeductions')?.value || 0);
  const net = basic + allowances - deductions;
  return { basic, allowances, deductions, net };
}

function updatePayslipNet() {
  const { net } = getPayslipNumbers();
  const el = document.getElementById('psNet');
  if (el) el.textContent = `$${Number(net).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

// Wire payslip input change handlers and print button
document.addEventListener('input', (e) => {
  const ids = ['psBasic','psAllowances','psDeductions'];
  if (e.target && ids.includes(e.target.id)) {
    updatePayslipNet();
  }
});

document.addEventListener('click', (e) => {
  if (e.target && e.target.id === 'payslipPrintBtn') {
    e.preventDefault();
    try { renderAndPrintPayslip(); } catch (err) { console.error(err); showToast('Failed to print payslip', 'error'); }
  } else if (e.target && e.target.id === 'paymentPrintBtn') {
    e.preventDefault();
    try { renderAndPrintPaymentSlip(); } catch (err) { console.error(err); showToast('Failed to print payment slip', 'error'); }
  }
});

function renderAndPrintPayslip() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const period = document.getElementById('psPeriod')?.value || '';
  const notes = document.getElementById('psNotes')?.value || '';
  const { basic, allowances, deductions, net } = getPayslipNumbers();
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

  const area = document.getElementById('payslipPrintArea');
  if (!area) { showToast('Print area missing in DOM', 'error'); return; }

  // Simple payslip template (A4-friendly)
  const monthTitle = period ? new Date(Number(period.split('-')[0]), Number(period.split('-')[1]) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Period';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  area.innerHTML = `
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
          <div class="label">Department</div>
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
          <tr><th>Earning</th><th class="text-right">Amount</th></tr>
        </thead>
        <tbody>
          <tr><td>Basic Salary</td><td class="text-right">${fmt(basic)}</td></tr>
          <tr><td>Allowances</td><td class="text-right">${fmt(allowances)}</td></tr>
          <tr><td>Deductions</td><td class="text-right">-${fmt(deductions)}</td></tr>
          <tr class="total"><td>Net Pay</td><td class="text-right">${fmt(net)}</td></tr>
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <footer class="payslip-footer">Generated by CRM • ${new Date().toLocaleString()}</footer>
    </section>
  `;

  // Trigger print
  const afterPrint = () => {
    window.removeEventListener('afterprint', afterPrint);
    // optional cleanup
  };
  window.addEventListener('afterprint', afterPrint);
  setTimeout(() => window.print(), 50);
}

function renderAndPrintPaymentSlip() {
  const emp = currentPayrollView;
  if (!emp) { showToast('No employee selected', 'warning'); return; }
  const payDate = document.getElementById('payDate')?.value || '';
  const amount = Number(document.getElementById('payAmount')?.value || 0);
  const method = document.getElementById('payMethod')?.value || 'cash';
  const ref = document.getElementById('payRef')?.value || '';
  const isAdvance = document.getElementById('payIsAdvance')?.value === 'yes';
  const deductFrom = document.getElementById('payDeductFrom')?.value || 'none';
  const notes = document.getElementById('payNotes')?.value || '';

  if (!amount || amount <= 0) { showToast('Enter a valid payment amount', 'warning'); return; }
  if (!payDate) { showToast('Select a payment date', 'warning'); return; }

  const area = document.getElementById('payslipPrintArea');
  if (!area) { showToast('Print area missing in DOM', 'error'); return; }

  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const dateStr = payDate ? new Date(payDate).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '';
  const typeLabel = emp._which === 'temporary' ? 'Temporary' : 'Permanent';
  const advText = isAdvance ? 'Yes (Advance)' : 'No';
  const deductText = {
    none: 'Do not deduct',
    next: 'Deduct from next payroll',
    installments: 'Deduct in 3 installments'
  }[deductFrom] || '—';

  area.innerHTML = `
    <section class="payslip">
      <header class="payslip-header">
        <div>
          <div class="title">Payment Slip</div>
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
          <div class="label">Department</div>
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
          <tr><td>${isAdvance ? 'Advance Paid' : 'Payment Made'}</td><td class="text-right">${fmt(amount)}</td></tr>
          <tr><td>Deduction Plan</td><td class="text-right">${advText} • ${deductText}</td></tr>
        </tbody>
      </table>
      ${notes ? `<div class="payslip-notes"><div class="label">Notes</div><div class="value">${notes}</div></div>` : ''}
      <footer class="payslip-footer">Generated by CRM • ${new Date().toLocaleString()}</footer>
    </section>
  `;

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
