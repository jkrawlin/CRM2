import { db, auth, storage } from './firebase-config.js?v=20250928-03';
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

let employees = [];
let temporaryEmployees = [];
let currentSortColumn = '';
let currentSortOrder = 'asc';
let currentTempSortColumn = '';
let currentTempSortOrder = 'asc';
let payrollSortColumn = '';
let payrollSortOrder = 'asc';
let deleteEmployeeId = null;
let currentSearch = '';
let currentDepartmentFilter = '';
let unsubscribeEmployees = null;
let unsubscribeTemporary = null;
let authed = false;
let authInitialized = false;
// Track current View modal context to lazy-load documents and manage blob URLs
let currentViewCtx = { id: null, which: 'employees', docsLoaded: false, revoke: [] };
// Track which payroll sub-tab is active: 'table' or 'report'
let currentPayrollSubTab = (typeof localStorage !== 'undefined' && localStorage.getItem('payrollSubTab')) || 'table';

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
  // Initial payroll render
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

// Add or update employee in Firestore
async function saveEmployee(employee, isNew = false) {
  try {
    if (employee.id) {
      const employeeRef = doc(db, "employees", employee.id);
      const { id, ...updateData } = employee;
      await updateDoc(employeeRef, updateData);
      showToast(isNew ? 'Employee added successfully' : 'Employee updated successfully', 'success');
    } else {
      const { id, ...newEmployee } = employee;
  await addDoc(collection(db, "employees"), newEmployee);
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
      await updateDoc(employeeRef, updateData);
      showToast(isNew ? 'Temporary employee added successfully' : 'Temporary employee updated successfully', 'success');
    } else {
      const { id, ...newEmployee } = employee;
  await addDoc(collection(db, "temporaryEmployees"), newEmployee);
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
  ['employeeModal', 'deleteModal', 'viewModal'].forEach((id) => {
    const modal = document.getElementById(id);
    if (modal) {
      modal.addEventListener('click', (e) => {
        // Close when clicking anywhere outside the dialog content
        const clickedInsideContent = e.target.closest('.modal-content');
        if (!clickedInsideContent) {
          if (id === 'employeeModal') closeEmployeeModal();
          if (id === 'deleteModal') closeModal();
          if (id === 'viewModal') closeViewModal();
        }
      });
    }
  });

  // Global capture listener as a fallback to guarantee overlay clicks close modals
  document.addEventListener('click', (e) => {
    const employeeModalEl = document.getElementById('employeeModal');
    const deleteModalEl = document.getElementById('deleteModal');
    const viewModalEl = document.getElementById('viewModal');
    const anyOpen = (el) => el && el.classList.contains('show');
    if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl)) return;

    const clickedInsideContent = e.target.closest('.modal-content');
    if (clickedInsideContent) return; // do nothing if click inside dialog

    // If the click is inside an open modal overlay but outside content, close it
    if (anyOpen(employeeModalEl) && e.target.closest('#employeeModal')) {
      closeEmployeeModal();
    } else if (anyOpen(deleteModalEl) && e.target.closest('#deleteModal')) {
      closeModal();
    } else if (anyOpen(viewModalEl) && e.target.closest('#viewModal')) {
      closeViewModal();
    }
  }, true);

  // Use pointerdown (captures mouse/touch/pen) to close even if click is prevented
  document.addEventListener('pointerdown', (e) => {
    const employeeModalEl = document.getElementById('employeeModal');
    const deleteModalEl = document.getElementById('deleteModal');
    const viewModalEl = document.getElementById('viewModal');
    const anyOpen = (el) => el && el.classList.contains('show');
    if (!anyOpen(employeeModalEl) && !anyOpen(deleteModalEl) && !anyOpen(viewModalEl)) return;

    const insideContent = e.target.closest('.modal-content');
    if (insideContent) return;

    if (anyOpen(employeeModalEl) && e.target.closest('#employeeModal')) {
      closeEmployeeModal();
    } else if (anyOpen(deleteModalEl) && e.target.closest('#deleteModal')) {
      closeModal();
    } else if (anyOpen(viewModalEl) && e.target.closest('#viewModal')) {
      closeViewModal();
    }
  }, true);

  // Close open modal on Escape key
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      const deleteModalEl = document.getElementById('deleteModal');
      const employeeModalEl = document.getElementById('employeeModal');
      const viewModalEl = document.getElementById('viewModal');
      if (deleteModalEl && deleteModalEl.classList.contains('show')) closeModal();
      if (employeeModalEl && employeeModalEl.classList.contains('show')) closeEmployeeModal();
      if (viewModalEl && viewModalEl.classList.contains('show')) closeViewModal();
    }
  });

  // Payroll Frame controls
  const payrollMonthEl = document.getElementById('payrollMonth');
  const exportCsvBtn = document.getElementById('exportPayrollCsvBtn');
  const printBtn = document.getElementById('printPayrollBtn');
  if (payrollMonthEl) {
    // Default to current month
    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
    payrollMonthEl.value = payrollMonthEl.value || ym;
    payrollMonthEl.addEventListener('change', () => renderPayrollFrame());
  }
  if (exportCsvBtn) exportCsvBtn.addEventListener('click', exportPayrollCsv);
  if (printBtn) printBtn.addEventListener('click', () => window.print());

  // Payroll sub-tab buttons
  const tabTableBtn = document.getElementById('payrollTabTableBtn');
  const tabReportBtn = document.getElementById('payrollTabReportBtn');
  if (tabTableBtn) tabTableBtn.addEventListener('click', () => setPayrollSubTab('table'));
  if (tabReportBtn) tabReportBtn.addEventListener('click', () => setPayrollSubTab('report'));
  // Initialize sub-tab visibility according to stored preference
  setPayrollSubTab(currentPayrollSubTab);
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
    renderPayrollTable();
    // Restore previously selected sub-tab
    setPayrollSubTab(currentPayrollSubTab);
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
    bankIban: ((document.getElementById('bankIban')?.value || '').trim() || '').toUpperCase().replace(/\s+/g, ''),
    profileImageUrl: undefined
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
    const { id, ...newData } = employee;
    const docRef = await addDoc(collection(db, basePath), newData);
    employee.id = docRef.id;
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

// Payroll sorting
window.sortPayroll = function(column) {
  if (payrollSortColumn === column) {
    payrollSortOrder = payrollSortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    payrollSortColumn = column;
    payrollSortOrder = 'asc';
  }
  renderPayrollTable();
}

// Render payroll table (combines employees and temporary)
function renderPayrollTable() {
  const tbody = document.getElementById('payrollTableBody');
  const emptyState = document.getElementById('payrollEmptyState');
  const totalEl = document.getElementById('totalPayroll');
  if (!tbody || !emptyState) return;

  // Merge with type tag
  const combined = [
    ...employees.map(e => ({ ...e, _type: 'Employee' })),
    ...temporaryEmployees.map(e => ({ ...e, _type: 'Temporary' })),
  ];

  // Apply current search (same logic as others)
  const filtered = combined.filter(emp => {
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
    return matchesSearch;
  });

  // Sort
  const sorted = [...filtered];
  if (payrollSortColumn) {
    sorted.sort((a, b) => {
      let va;
      let vb;
      switch (payrollSortColumn) {
        case 'salary':
          va = Number(a.salary);
          vb = Number(b.salary);
          break;
        case 'type':
          va = (a._type || '').toLowerCase();
          vb = (b._type || '').toLowerCase();
          break;
        default:
          va = (a[payrollSortColumn] ?? '').toString().toLowerCase();
          vb = (b[payrollSortColumn] ?? '').toString().toLowerCase();
      }
      if (va < vb) return payrollSortOrder === 'asc' ? -1 : 1;
      if (va > vb) return payrollSortOrder === 'asc' ? 1 : -1;
      return 0;
    });
  }

  // Total payroll
  const total = filtered.reduce((sum, e) => sum + Number(e.salary || 0), 0);
  if (totalEl) totalEl.textContent = `$${total.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

  if (sorted.length === 0) {
    tbody.innerHTML = '';
    emptyState.classList.remove('hidden');
    return;
  }
  emptyState.classList.add('hidden');

  // Group by type: Permanent (Employee) and Temporary
  const groups = {
    Employee: [],
    Temporary: []
  };
  sorted.forEach(emp => {
    if (emp._type === 'Temporary') groups.Temporary.push(emp);
    else groups.Employee.push(emp);
  });

  const typeBadge = (t) => {
    const isTemp = t === 'Temporary';
    const cls = isTemp ? 'bg-amber-100 text-amber-800' : 'bg-emerald-100 text-emerald-800';
    const label = isTemp ? 'Temporary' : 'Permanent';
    return `<span class="px-2 py-1 rounded text-xs font-semibold ${cls}">${label}</span>`;
  };

  const section = (label, items) => {
    if (!items.length) return '';
    const tone = label.includes('Temporary') ? 'bg-amber-50 text-amber-800' : 'bg-emerald-50 text-emerald-800';
    const header = `<tr class="${tone}"><td colspan="6" class="px-4 py-2 font-bold">${label} (${items.length})</td></tr>`;
    const rows = items.map(emp => `
      <tr class="hover:bg-gray-50">
        <td class="px-4 py-3 font-semibold text-gray-900">${emp.name}</td>
        <td class="px-4 py-3">${typeBadge(emp._type)}</td>
        <td class="px-4 py-3">${emp.department || '-'}</td>
        <td class="px-4 py-3 text-right tabular-nums">$${Number(emp.salary || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
        <td class="px-4 py-3 whitespace-nowrap">${formatDate(emp.joinDate)}</td>
        <td class="px-4 py-3">${emp.qid || '-'}</td>
      </tr>
    `).join('');
    return header + rows;
  };

  tbody.innerHTML = [
    section('Permanent Employees', groups.Employee),
    section('Temporary Employees', groups.Temporary)
  ].join('');

  // Also render the printable payroll frame whenever table renders
  renderPayrollFrame();
}

// Render a printable payroll frame
function renderPayrollFrame() {
  const frame = document.getElementById('payrollFrame');
  if (!frame) return;
  const monthEl = document.getElementById('payrollMonth');
  const ym = monthEl && monthEl.value ? monthEl.value : '';
  const [yr, mo] = ym ? ym.split('-') : ['',''];

  // Gather combined list, sorted by name
  const combined = [
    ...employees.map(e => ({ ...e, _type: 'Employee' })),
    ...temporaryEmployees.map(e => ({ ...e, _type: 'Temporary' })),
  ].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  const total = combined.reduce((sum, e) => sum + Number(e.salary || 0), 0);
  const fmt = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

  const monthTitle = ym ? new Date(Number(yr), Number(mo) - 1, 1).toLocaleDateString(undefined, { year: 'numeric', month: 'long' }) : 'Current Month';

  const byType = {
    Employee: combined.filter(e => e._type === 'Employee'),
    Temporary: combined.filter(e => e._type === 'Temporary')
  };

  const badge = (t) => t === 'Temporary'
    ? '<span class="px-2 py-1 rounded text-xs font-semibold bg-amber-100 text-amber-800">Temporary</span>'
    : '<span class="px-2 py-1 rounded text-xs font-semibold bg-emerald-100 text-emerald-800">Permanent</span>';

  const renderGroup = (label, items) => {
    if (!items.length) return '';
    const tone = label.includes('Temporary') ? 'bg-amber-50 text-amber-800' : 'bg-emerald-50 text-emerald-800';
    const header = `<tr class="${tone}"><td colspan="9" class="px-4 py-2 font-bold">${label} (${items.length})</td></tr>`;
    const rows = items.map((e, i) => `
      <tr class="border-b border-gray-100">
        <td class="px-4 py-2 text-gray-600">${i + 1}</td>
        <td class="px-4 py-2 font-semibold text-gray-900">${e.name || ''}</td>
        <td class="px-4 py-2">${badge(e._type)}</td>
        <td class="px-4 py-2">${e.department || '-'}</td>
        <td class="px-4 py-2">${e.qid || '-'}</td>
        <td class="px-4 py-2">${e.bankName || '-'}</td>
        <td class="px-4 py-2">${maskAccount(e.bankAccountNumber)}</td>
        <td class="px-4 py-2 break-all">${e.bankIban || '-'}</td>
        <td class="px-4 py-2 text-right tabular-nums">${fmt(e.salary)}</td>
      </tr>
    `).join('');
    return header + rows;
  };

  const rows = [
    renderGroup('Permanent Employees', byType.Employee),
    renderGroup('Temporary Employees', byType.Temporary)
  ].join('');

  frame.innerHTML = `
    <div class="flex items-start justify-between mb-4">
      <div>
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Payroll</div>
        <div class="text-xl font-extrabold text-gray-900">${monthTitle}</div>
      </div>
      <div class="text-right">
        <div class="text-xs font-semibold uppercase tracking-wider text-gray-500">Total</div>
        <div class="text-xl font-extrabold text-gray-900">${fmt(total)}</div>
      </div>
    </div>
    <div class="overflow-x-auto">
      <table class="min-w-full text-sm">
        <thead class="bg-gray-50 text-gray-600 text-xs uppercase tracking-wide">
          <tr>
            <th class="text-left font-semibold px-4 py-2">#</th>
            <th class="text-left font-semibold px-4 py-2">Name</th>
            <th class="text-left font-semibold px-4 py-2">Type</th>
            <th class="text-left font-semibold px-4 py-2">Department</th>
            <th class="text-left font-semibold px-4 py-2">QID</th>
            <th class="text-left font-semibold px-4 py-2">Bank</th>
            <th class="text-left font-semibold px-4 py-2">Account</th>
            <th class="text-left font-semibold px-4 py-2">IBAN</th>
            <th class="text-right font-semibold px-4 py-2">Monthly Salary</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-100">
          ${rows}
        </tbody>
      </table>
    </div>
  `;
}

// Toggle Payroll sub-tabs (Table vs Month Report)
function setPayrollSubTab(which) {
  const tablePane = document.getElementById('payrollTabTable');
  const reportPane = document.getElementById('payrollTabReport');
  const tableBtn = document.getElementById('payrollTabTableBtn');
  const reportBtn = document.getElementById('payrollTabReportBtn');
  if (!tablePane || !reportPane || !tableBtn || !reportBtn) return;

  const activateBtn = (btn, active) => {
    if (active) {
      btn.classList.add('font-semibold', 'text-indigo-600', 'border-b-2', 'border-indigo-600');
      btn.classList.remove('text-gray-600');
    } else {
      btn.classList.remove('font-semibold', 'text-indigo-600', 'border-b-2', 'border-indigo-600');
      btn.classList.add('text-gray-600');
    }
  };

  if (which === 'report') {
    tablePane.style.display = 'none';
    reportPane.style.display = '';
    activateBtn(tableBtn, false);
    activateBtn(reportBtn, true);
    renderPayrollFrame();
    currentPayrollSubTab = 'report';
  } else {
    // default to table
    tablePane.style.display = '';
    reportPane.style.display = 'none';
    activateBtn(tableBtn, true);
    activateBtn(reportBtn, false);
    renderPayrollTable();
    currentPayrollSubTab = 'table';
  }
  try { localStorage.setItem('payrollSubTab', currentPayrollSubTab); } catch {}
}

function maskAccount(acc) {
  if (!acc) return '-';
  const s = String(acc).replace(/\s+/g, '');
  if (s.length <= 4) return s;
  return '•••• ' + s.slice(-4);
}

function exportPayrollCsv() {
  const combined = [
    ...employees.map(e => ({ ...e, _type: 'Employee' })),
    ...temporaryEmployees.map(e => ({ ...e, _type: 'Temporary' })),
  ].sort((a, b) => (a.name || '').localeCompare(b.name || ''));

  const headers = ['Name','Type','Department','QID','Bank Name','Account Number','IBAN','Monthly Salary'];
  const rows = combined.map(e => [
    quoteCsv(e.name || ''),
    quoteCsv(e._type || ''),
    quoteCsv(e.department || ''),
    quoteCsv(e.qid || ''),
    quoteCsv(e.bankName || ''),
    quoteCsv(e.bankAccountNumber || ''),
    quoteCsv(e.bankIban || ''),
    String(Number(e.salary || 0))
  ].join(','));

  const csv = [headers.join(','), ...rows].join('\r\n');
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  const now = new Date();
  const ym = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}`;
  a.href = url;
  a.download = `payroll-${ym}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
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
    if (currentTempSortColumn === column) {
      currentTempSortOrder = currentTempSortOrder === 'asc' ? 'desc' : 'asc';
    } else {
      currentTempSortColumn = column;
      currentTempSortOrder = 'asc';
    }
    renderTemporaryTable();
  } else {
    if (currentSortColumn === column) {
      currentSortOrder = currentSortOrder === 'asc' ? 'desc' : 'asc';
    } else {
      currentSortColumn = column;
      currentSortOrder = 'asc';
    }
    renderEmployeeTable();
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

    // PDF view buttons state
    const qidBtn = document.getElementById('viewQidPdfBtn');
    const passBtn = document.getElementById('viewPassportPdfBtn');
    const qidStatus = document.getElementById('qidPdfStatus');
    const passStatus = document.getElementById('passportPdfStatus');
    if (qidBtn) {
      if (employee.qidPdfUrl) {
        qidBtn.style.display = '';
        qidBtn.onclick = async () => {
          const basePath = which === 'temporary' ? 'temporaryEmployees' : 'employees';
          try {
            const ref = storageRef(storage, `${basePath}/${employee.id}/qidPdf.pdf`);
            const url = await getDownloadURL(ref);
            window.open(url, '_blank');
          } catch (e) {
            console.warn('Open QID via URL failed, trying blob', e?.code || e?.message || e);
            try {
              const ref = storageRef(storage, `${basePath}/${employee.id}/qidPdf.pdf`);
              const blob = await getBlob(ref);
              const blobUrl = URL.createObjectURL(blob);
              window.open(blobUrl, '_blank');
              // No revoke here since it opens in a new tab and browser clears on navigation
            } catch (e2) {
              console.warn('Open QID failed, falling back to stored URL', e2?.code || e2?.message || e2);
              window.open(employee.qidPdfUrl, '_blank');
            }
          }
        };
        if (qidStatus) qidStatus.textContent = 'Uploaded';
      } else {
        qidBtn.style.display = 'none';
        if (qidStatus) qidStatus.textContent = '';
      }
    }
    if (passBtn) {
      if (employee.passportPdfUrl) {
        passBtn.style.display = '';
        passBtn.onclick = async () => {
          const basePath = which === 'temporary' ? 'temporaryEmployees' : 'employees';
          try {
            const ref = storageRef(storage, `${basePath}/${employee.id}/passportPdf.pdf`);
            const url = await getDownloadURL(ref);
            window.open(url, '_blank');
          } catch (e) {
            console.warn('Open Passport via URL failed, trying blob', e?.code || e?.message || e);
            try {
              const ref = storageRef(storage, `${basePath}/${employee.id}/passportPdf.pdf`);
              const blob = await getBlob(ref);
              const blobUrl = URL.createObjectURL(blob);
              window.open(blobUrl, '_blank');
            } catch (e2) {
              console.warn('Open Passport failed, falling back to stored URL', e2?.code || e2?.message || e2);
              window.open(employee.passportPdfUrl, '_blank');
            }
          }
        };
        if (passStatus) passStatus.textContent = 'Uploaded';
      } else {
        passBtn.style.display = 'none';
        if (passStatus) passStatus.textContent = '';
      }
    }
  }
}

// Render employee table
function renderEmployeeTable() {
  const tbody = document.getElementById('employeeTableBody');
  const emptyState = document.getElementById('emptyState');
  if (!tbody || !emptyState) return;

  const filtered = employees.filter(emp => {
    const matchesDept = !currentDepartmentFilter || emp.department === currentDepartmentFilter;

    // Prioritize Qatar ID search when input is mostly digits
    const query = (currentSearch || '').trim();
    const queryDigits = query.replace(/\D/g, '');
    const empQidDigits = String(emp.qid || '').replace(/\D/g, '');
    let matchesSearch = true;
    if (query) {
      if (queryDigits.length >= 4) {
        // If user enters at least 4 digits, match against QID digits specifically
        matchesSearch = empQidDigits.includes(queryDigits);
      } else {
        // Fallback to generic text search across fields
        const text = `${emp.name} ${emp.email} ${emp.qid || ''} ${emp.phone || ''} ${emp.position} ${emp.department}`.toLowerCase();
        matchesSearch = text.includes(query);
      }
    }
    return matchesDept && matchesSearch;
  });

  const sorted = [...filtered];
  if (currentSortColumn) {
    sorted.sort((a, b) => {
      let va = a[currentSortColumn];
      let vb = b[currentSortColumn];
      if (currentSortColumn === 'salary') {
        va = Number(va);
        vb = Number(vb);
      } else {
        va = (va ?? '').toString().toLowerCase();
        vb = (vb ?? '').toString().toLowerCase();
      }
      if (va < vb) return currentSortOrder === 'asc' ? -1 : 1;
      if (va > vb) return currentSortOrder === 'asc' ? 1 : -1;
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

  tbody.innerHTML = sorted.map((employee) => `
    <tr class="hover:bg-gray-50">
  <td class="px-4 py-3 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer" onclick="viewEmployee('${employee.id}', 'employees')">${employee.name}</td>
      <td class="px-4 py-3">${employee.email}</td>
      <td class="px-4 py-3">${employee.phone || '-'}</td>
      <td class="px-4 py-3">${employee.qid || '-'}</td>
      <td class="px-4 py-3">${employee.position}</td>
      <td class="px-4 py-3">
        <span class="department-badge ${deptClass(employee.department)}">${employee.department}</span>
      </td>
  <td class="px-4 py-3 text-right tabular-nums">$${Number(employee.salary ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td class="px-4 py-3 whitespace-nowrap">${formatDate(employee.joinDate)}</td>
      <td class="px-4 py-3 text-center">
        <div class="action-buttons">
          <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}', 'employees')"><i class="fas fa-edit"></i></button>
          <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}', 'employees')"><i class="fas fa-trash"></i></button>
        </div>
      </td>
    </tr>
  `).join('');
}

// View employee (read-only modal)
window.viewEmployee = async function(id, which) {
  // Resolve employee data
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const employee = list.find(e => e.id === id);
  if (!employee) return;

  const modal = document.getElementById('viewModal');
  const modalBody = modal ? modal.querySelector('.modal-body') : null;
  if (!modal || !modalBody) return;

  // Open modal immediately
  modal.classList.add('show');

  // Prepare display helpers
  const formattedSalary = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number(employee.salary || 0));
  const joinDate = employee.joinDate ? new Date(employee.joinDate).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }) : '-';
  const initials = (employee.name || 'U').trim().split(/\s+/).map(n => n[0]).join('').slice(0, 2).toUpperCase();
  const deptClass = (employee.department || '').toLowerCase().replace(/\s+/g, '-');
  const basePath = which === 'temporary' ? 'temporaryEmployees' : 'employees';

  // Render enhanced UI
  modalBody.innerHTML = `
    <div class="employee-header">
      <div class="employee-avatar-container">
        <div class="employee-avatar">
          ${employee.profileImageUrl ? `<img src="${employee.profileImageUrl}" alt="${employee.name}" />` : `<span class="avatar-initials">${initials}</span>`}
        </div>
        <div class="online-indicator"></div>
      </div>
      <div class="employee-header-info">
        <h2 class="employee-name">${employee.name || ''}</h2>
        <p class="employee-title">${employee.position || ''}</p>
        <div class="employee-badges">
          <span class="badge badge-${deptClass || 'engineering'}">${employee.department || '-'}</span>
          <span class="badge badge-status">${which === 'temporary' ? 'Temporary' : 'Active'}</span>
        </div>
      </div>
      <div class="employee-header-actions">
        <button class="action-btn-icon" title="Send Email" data-action="email"><i class="fas fa-envelope"></i></button>
        <button class="action-btn-icon" title="Call" data-action="call"><i class="fas fa-phone"></i></button>
        <button class="action-btn-icon" title="More Options"><i class="fas fa-ellipsis-v"></i></button>
      </div>
    </div>

    <div class="info-tabs">
      <button class="tab-btn active" data-tab="overview"><i class="fas fa-user"></i> Overview</button>
      <button class="tab-btn" data-tab="contact"><i class="fas fa-address-card"></i> Contact</button>
      <button class="tab-btn" data-tab="financial"><i class="fas fa-dollar-sign"></i> Financial</button>
      <button class="tab-btn" data-tab="documents"><i class="fas fa-file-alt"></i> Documents</button>
    </div>

    <div class="tab-content">
      <div class="tab-pane active" id="overview-tab">
        <div class="info-grid">
          <div class="info-card">
            <div class="info-card-header"><i class="fas fa-briefcase"></i><h3>Employment Details</h3></div>
            <div class="info-card-body">
              <div class="info-row"><span class="info-label">Employee ID</span><span class="info-value">#${employee.id || 'EMP001'}</span></div>
              <div class="info-row"><span class="info-label">Department</span><span class="info-value"><span class="department-chip ${deptClass || 'engineering'}">${employee.department || '-'}</span></span></div>
              <div class="info-row"><span class="info-label">Position</span><span class="info-value">${employee.position || '-'}</span></div>
              <div class="info-row"><span class="info-label">Join Date</span><span class="info-value">${joinDate}</span></div>
              <div class="info-row"><span class="info-label">Employment Type</span><span class="info-value"><span class="status-chip ${which === 'temporary' ? '' : 'active'}">${which === 'temporary' ? 'Temporary' : 'Full Time'}</span></span></div>
            </div>
          </div>
          <div class="info-card">
            <div class="info-card-header"><i class="fas fa-chart-line"></i><h3>Performance</h3></div>
            <div class="info-card-body">
              <div class="performance-metric"><span class="metric-label">Overall Rating</span><div class="rating-stars"><i class="fas fa-star"></i><i class="fas fa-star"></i><i class="fas fa-star"></i><i class="fas fa-star"></i><i class="far fa-star"></i><span class="rating-value">4.2</span></div></div>
              <div class="performance-metric"><span class="metric-label">Attendance</span><div class="progress-bar"><div class="progress-fill" style="width: 95%"></div></div><span class="metric-value">95%</span></div>
              <div class="performance-metric"><span class="metric-label">Projects Completed</span><span class="metric-value">12</span></div>
            </div>
          </div>
        </div>
      </div>

      <div class="tab-pane" id="contact-tab" style="display:none;">
        <div class="info-grid">
          <div class="info-card">
            <div class="info-card-header"><i class="fas fa-phone"></i><h3>Contact Information</h3></div>
            <div class="info-card-body">
              <div class="info-row"><span class="info-label">Email</span><span class="info-value"><a href="mailto:${employee.email || ''}" class="link"><i class="fas fa-envelope"></i> ${employee.email || '-'}</a></span></div>
              <div class="info-row"><span class="info-label">Phone</span><span class="info-value"><a href="tel:${employee.phone || ''}" class="link"><i class="fas fa-phone"></i> ${employee.phone || 'Not provided'}</a></span></div>
              <div class="info-row"><span class="info-label">Qatar ID</span><span class="info-value">${employee.qid || 'Not provided'}</span></div>
            </div>
          </div>
        </div>
      </div>

      <div class="tab-pane" id="financial-tab" style="display:none;">
        <div class="info-grid">
          <div class="info-card">
            <div class="info-card-header"><i class="fas fa-money-check-alt"></i><h3>Salary Information</h3></div>
            <div class="info-card-body">
              <div class="salary-display"><span class="salary-label">Monthly Salary</span><span class="salary-amount">${formattedSalary}</span></div>
              <div class="info-row"><span class="info-label">Annual Salary</span><span class="info-value">${new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(Number(employee.salary || 0) * 12)}</span></div>
            </div>
          </div>
          <div class="info-card">
            <div class="info-card-header"><i class="fas fa-university"></i><h3>Banking Details</h3></div>
            <div class="info-card-body">
              <div class="info-row"><span class="info-label">Bank Name</span><span class="info-value">${employee.bankName || 'Not provided'}</span></div>
              <div class="info-row"><span class="info-label">Account Number</span><span class="info-value">${employee.bankAccountNumber ? '****' + String(employee.bankAccountNumber).slice(-4) : 'Not provided'}</span></div>
              <div class="info-row"><span class="info-label">IBAN</span><span class="info-value">${employee.bankIban || 'Not provided'}</span></div>
            </div>
          </div>
        </div>
      </div>

      <div class="tab-pane" id="documents-tab" style="display:none;">
        <div class="documents-grid">
          ${employee.qidPdfUrl ? `
            <div class="document-card">
              <i class="fas fa-id-card doc-icon"></i>
              <span class="doc-name">Qatar ID</span>
              <a href="#" class="doc-action" data-kind="qid"><i class="fas fa-download"></i> View</a>
            </div>` : ''}
          ${employee.passportPdfUrl ? `
            <div class="document-card">
              <i class="fas fa-passport doc-icon"></i>
              <span class="doc-name">Passport</span>
              <a href="#" class="doc-action" data-kind="passport"><i class="fas fa-download"></i> View</a>
            </div>` : ''}
          ${!employee.qidPdfUrl && !employee.passportPdfUrl ? `
            <div class="no-documents">
              <i class="fas fa-folder-open"></i>
              <p>No documents uploaded</p>
            </div>` : ''}
        </div>
      </div>
    </div>
  `;

  // Wire header actions
  const emailBtn = modal.querySelector('[data-action="email"]');
  if (emailBtn && employee.email) emailBtn.addEventListener('click', () => window.open(`mailto:${employee.email}`, '_self'));
  const callBtn = modal.querySelector('[data-action="call"]');
  if (callBtn && employee.phone) callBtn.addEventListener('click', () => window.open(`tel:${employee.phone}`, '_self'));

  // Tabs behavior
  const tabBtns = modal.querySelectorAll('.tab-btn');
  const tabPanes = modal.querySelectorAll('.tab-pane');
  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.tab;
      tabBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      tabPanes.forEach(pane => {
        pane.style.display = pane.id === `${target}-tab` ? 'block' : 'none';
      });
    });
  });

  // Document links with fresh Storage URLs
  modal.querySelectorAll('.doc-action').forEach(a => {
    a.addEventListener('click', async (e) => {
      e.preventDefault();
      const kind = a.getAttribute('data-kind');
      const path = `${basePath}/${employee.id}/${kind}Pdf.pdf`;
      try {
        const refObj = storageRef(storage, path);
        const url = await getDownloadURL(refObj);
        window.open(url, '_blank');
      } catch (err) {
        // Fallback to stored URL
        const fallback = kind === 'qid' ? employee.qidPdfUrl : employee.passportPdfUrl;
        if (fallback) window.open(fallback, '_blank');
        else showToast('Document not available', 'warning');
      }
    });
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
  const tbody = document.getElementById('tempEmployeeTableBody');
  const emptyState = document.getElementById('tempEmptyState');
  if (!tbody || !emptyState) return;

  const filtered = temporaryEmployees.filter(emp => {
    const matchesDept = !currentDepartmentFilter || emp.department === currentDepartmentFilter;

    // Prioritize Qatar ID search when input is mostly digits
    const query = (currentSearch || '').trim();
    const queryDigits = query.replace(/\D/g, '');
    const empQidDigits = String(emp.qid || '').replace(/\D/g, '');
    let matchesSearch = true;
    if (query) {
      if (queryDigits.length >= 4) {
        // If user enters at least 4 digits, match against QID digits specifically
        matchesSearch = empQidDigits.includes(queryDigits);
      } else {
        // Fallback to generic text search across fields
        const text = `${emp.name} ${emp.email} ${emp.qid || ''} ${emp.phone || ''} ${emp.position} ${emp.department}`.toLowerCase();
        matchesSearch = text.includes(query);
      }
    }
    return matchesDept && matchesSearch;
  });

  const sorted = [...filtered];
  if (currentTempSortColumn) {
    sorted.sort((a, b) => {
      let va = a[currentTempSortColumn];
      let vb = b[currentTempSortColumn];
      if (currentTempSortColumn === 'salary') {
        va = Number(va);
        vb = Number(vb);
      } else {
        va = (va ?? '').toString().toLowerCase();
        vb = (vb ?? '').toString().toLowerCase();
      }
      if (va < vb) return currentTempSortOrder === 'asc' ? -1 : 1;
      if (va > vb) return currentTempSortOrder === 'asc' ? 1 : -1;
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

  tbody.innerHTML = sorted.map((employee) => `
    <tr class="hover:bg-gray-50">
      <td class="px-4 py-3 font-semibold text-indigo-600 hover:text-indigo-700 cursor-pointer" onclick="viewEmployee('${employee.id}', 'temporary')">${employee.name}</td>
      <td class="px-4 py-3">${employee.email}</td>
      <td class="px-4 py-3">${employee.phone || '-'}</td>
      <td class="px-4 py-3">${employee.qid || '-'}</td>
      <td class="px-4 py-3">${employee.position}</td>
      <td class="px-4 py-3">
        <span class="department-badge ${deptClass(employee.department)}">${employee.department}</span>
      </td>
      <td class="px-4 py-3 text-right tabular-nums">$${Number(employee.salary ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td class="px-4 py-3 whitespace-nowrap">${formatDate(employee.joinDate)}</td>
      <td class="px-4 py-3 text-center">
        <div class="action-buttons">
          <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}', 'temporary')"><i class="fas fa-edit"></i></button>
          <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}', 'temporary')"><i class="fas fa-trash"></i></button>
        </div>
      </td>
    </tr>
  `).join('');
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
