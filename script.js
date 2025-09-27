import { db, auth, storage } from './firebase-config.js?v=20250927-02';
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
  getDownloadURL
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
  if (qidPdf) qidPdf.addEventListener('change', () => markPending('qid'));
  if (passportPdf) passportPdf.addEventListener('change', () => markPending('passport'));
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
    phone: (document.getElementById('phone')?.value || '').trim()
  };
  const isNew = !employee.id;

  // Upload PDFs if selected
  const which = document.getElementById('employeeForm')?.getAttribute('data-which') || 'employees';
  const files = {
    qidPdf: document.getElementById('qidPdf')?.files?.[0] || null,
    passportPdf: document.getElementById('passportPdf')?.files?.[0] || null,
  };

  const basePath = which === 'temporary' ? 'temporaryEmployees' : 'employees';

  // If creating new document without ID, first create the doc to get ID, then upload
  if (!employee.id) {
    const { id, ...newData } = employee;
    const docRef = await addDoc(collection(db, basePath), newData);
    employee.id = docRef.id;
  }

  // Helper to upload using the actual employee.id path
  const uploadIfNeeded = async (key, file) => {
    if (!file) return null;
    if (file.type !== 'application/pdf') {
      showToast('Please upload PDF files only', 'warning');
      return null;
    }
    try {
      const path = `${basePath}/${employee.id}/${key}.pdf`;
      const ref = storageRef(storage, path);
      await uploadBytes(ref, file, { contentType: 'application/pdf' });
      return await getDownloadURL(ref);
    } catch (err) {
      console.error('Upload failed:', err);
      showToast('Failed to upload document. Saving other changes.', 'error');
      return null; // continue saving other fields
    }
  };

  const qidUrl = await uploadIfNeeded('qidPdf', files.qidPdf);
  const passportUrl = await uploadIfNeeded('passportPdf', files.passportPdf);
  if (qidUrl) employee.qidPdfUrl = qidUrl;
  if (passportUrl) employee.passportPdfUrl = passportUrl;

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

  tbody.innerHTML = sorted.map(emp => `
    <tr class="hover:bg-gray-50">
      <td class="px-4 py-3 font-semibold text-gray-900">${emp.name}</td>
      <td class="px-4 py-3">${emp._type}</td>
      <td class="px-4 py-3">${emp.department || '-'}</td>
      <td class="px-4 py-3 text-right tabular-nums">$${Number(emp.salary || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
      <td class="px-4 py-3 whitespace-nowrap">${formatDate(emp.joinDate)}</td>
      <td class="px-4 py-3">${emp.qid || '-'}</td>
    </tr>
  `).join('');
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

    // PDF view buttons state
    const qidBtn = document.getElementById('viewQidPdfBtn');
    const passBtn = document.getElementById('viewPassportPdfBtn');
    const qidStatus = document.getElementById('qidPdfStatus');
    const passStatus = document.getElementById('passportPdfStatus');
    if (qidBtn) {
      if (employee.qidPdfUrl) {
        qidBtn.style.display = '';
        qidBtn.onclick = () => window.open(employee.qidPdfUrl, '_blank');
        if (qidStatus) qidStatus.textContent = 'Uploaded';
      } else {
        qidBtn.style.display = 'none';
        if (qidStatus) qidStatus.textContent = '';
      }
    }
    if (passBtn) {
      if (employee.passportPdfUrl) {
        passBtn.style.display = '';
        passBtn.onclick = () => window.open(employee.passportPdfUrl, '_blank');
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
window.viewEmployee = function(id, which) {
  const list = which === 'temporary' ? temporaryEmployees : employees;
  const emp = list.find(e => e.id === id);
  if (!emp) return;
  const fmtCurrency = (n) => `$${Number(n || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const byId = (x) => document.getElementById(x);
  const deptText = emp.department || '';
  if (byId('viewName')) byId('viewName').textContent = emp.name || '';
  if (byId('viewNameSub')) byId('viewNameSub').textContent = emp.position ? `${emp.position} • ${deptText}` : deptText;
  if (byId('viewEmail')) byId('viewEmail').textContent = emp.email || '';
  if (byId('viewQid')) byId('viewQid').textContent = emp.qid || '-';
  if (byId('viewPhone')) byId('viewPhone').textContent = emp.phone || '-';
  if (byId('viewPosition')) byId('viewPosition').textContent = emp.position || '';
  if (byId('viewDepartment')) byId('viewDepartment').textContent = deptText;
  if (byId('viewSalary')) byId('viewSalary').textContent = fmtCurrency(emp.salary);
  if (byId('viewJoinDate')) byId('viewJoinDate').textContent = formatDate(emp.joinDate);
  // PDF preview + links in View modal
  const qidPreview = byId('qidPdfPreview');
  const qidLink = byId('qidPdfLink');
  const qidEmpty = byId('qidDocEmpty');
  if (qidPreview && qidLink && qidEmpty) {
    if (emp.qidPdfUrl && typeof emp.qidPdfUrl === 'string') {
      qidPreview.src = emp.qidPdfUrl;
      qidPreview.style.display = '';
      qidLink.href = emp.qidPdfUrl;
      qidLink.style.display = '';
      qidEmpty.style.display = 'none';
    } else {
      qidPreview.removeAttribute('src');
      qidPreview.style.display = 'none';
      qidLink.style.display = 'none';
      qidEmpty.style.display = '';
    }
  }

  const passPreview = byId('passportPdfPreview');
  const passLink = byId('passportPdfLink');
  const passEmpty = byId('passportDocEmpty');
  if (passPreview && passLink && passEmpty) {
    if (emp.passportPdfUrl && typeof emp.passportPdfUrl === 'string') {
      passPreview.src = emp.passportPdfUrl;
      passPreview.style.display = '';
      passLink.href = emp.passportPdfUrl;
      passLink.style.display = '';
      passEmpty.style.display = 'none';
    } else {
      passPreview.removeAttribute('src');
      passPreview.style.display = 'none';
      passLink.style.display = 'none';
      passEmpty.style.display = '';
    }
  }
  const vm = document.getElementById('viewModal');
  if (vm) vm.classList.add('show');
}

window.closeViewModal = function() {
  const vm = document.getElementById('viewModal');
  if (vm) vm.classList.remove('show');
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
    passport: 'passportPdfStatus'
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
