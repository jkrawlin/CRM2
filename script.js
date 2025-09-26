import { db, auth } from './firebase-config.js';
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

let employees = [];
let currentSortColumn = '';
let currentSortOrder = 'asc';
let deleteEmployeeId = null;
let currentSearch = '';
let currentDepartmentFilter = '';
let unsubscribeEmployees = null;
let authed = false;
let authInitialized = false;

// Initialize the app
document.addEventListener('DOMContentLoaded', () => {
  // IMPORTANT: Hide app and show login immediately on load
  const loginPage = document.getElementById('loginPage');
  const appRoot = document.getElementById('appRoot');
  if (loginPage) loginPage.style.display = '';
  if (appRoot) appRoot.style.display = 'none';

  initializeTheme();
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
      loadEmployeesRealtime();
    } else {
      // User is signed out
      if (loginPage) loginPage.style.display = '';
      if (appRoot) appRoot.style.display = 'none';

      // Clean up
      if (unsubscribeEmployees) {
        unsubscribeEmployees();
        unsubscribeEmployees = null;
      }
      employees = [];
      renderEmployeeTable();
      updateStats();
      updateDepartmentFilter();
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

// Add or update employee in Firestore
async function saveEmployee(employee) {
  try {
    if (employee.id) {
      const employeeRef = doc(db, "employees", employee.id);
      const { id, ...updateData } = employee;
      await updateDoc(employeeRef, updateData);
      showToast('Employee updated successfully', 'success');
    } else {
      const { id, ...newEmployee } = employee;
      await addDoc(collection(db, "employees"), newEmployee);
      showToast('Employee added successfully', 'success');
    }
    clearForm();
  } catch (error) {
    console.error("Error saving employee: ", error);
    showToast('Error saving employee', 'error');
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

// Setup event listeners
function setupEventListeners() {
  const formEl = document.getElementById('employeeForm');
  if (formEl) formEl.addEventListener('submit', handleFormSubmit);
  const searchEl = document.getElementById('searchInput');
  if (searchEl) searchEl.addEventListener('input', handleSearch);
  const filterEl = document.getElementById('filterDepartment');
  if (filterEl) filterEl.addEventListener('change', handleSearch);
  const confirmDeleteEl = document.getElementById('confirmDelete');
  if (confirmDeleteEl) confirmDeleteEl.addEventListener('click', handleConfirmDelete);

  const signOutBtn = document.getElementById('signOutBtn');
  if (signOutBtn) signOutBtn.addEventListener('click', handleSignOut);

  // Open employee modal button
  const openEmployeeModalBtn = document.getElementById('openEmployeeModalBtn');
  if (openEmployeeModalBtn) openEmployeeModalBtn.addEventListener('click', openEmployeeModal);

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
  if (e.target.id === 'filterDepartment') {
    currentDepartmentFilter = e.target.value;
  }
  renderEmployeeTable();
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
    joinDate: document.getElementById('joinDate').value
  };

  await saveEmployee(employee);
}

// Delete employee
window.openDeleteModal = function(id) {
  deleteEmployeeId = id;
  const modal = document.getElementById('deleteModal');
  if (modal) modal.classList.add('show');
}

// Confirm delete
async function handleConfirmDelete() {
  if (deleteEmployeeId) {
    await deleteEmployeeFromDB(deleteEmployeeId);
    closeModal();
    deleteEmployeeId = null;
  }
}

// Close modal
window.closeModal = function() {
  const modal = document.getElementById('deleteModal');
  if (modal) modal.classList.remove('show');
  deleteEmployeeId = null;
}

// Employee form modal controls
window.openEmployeeModal = function() {
  const modal = document.getElementById('employeeModal');
  if (modal) modal.classList.add('show');
  // Reset form to add mode
  clearForm();
}

window.closeEmployeeModal = function() {
  const modal = document.getElementById('employeeModal');
  if (modal) modal.classList.remove('show');
}

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
  const primaryBtn = document.querySelector('.btn-primary');
  if (primaryBtn) primaryBtn.innerHTML = '<i class="fas fa-save"></i> Save Employee';
}

// Sort handlers
window.sortTable = function(column) {
  if (currentSortColumn === column) {
    currentSortOrder = currentSortOrder === 'asc' ? 'desc' : 'asc';
  } else {
    currentSortColumn = column;
    currentSortOrder = 'asc';
  }
  renderEmployeeTable();
}

// Edit employee
window.editEmployee = function(id) {
  const employee = employees.find(emp => emp.id === id);
  if (employee) {
    document.getElementById('employeeId').value = employee.id;
    document.getElementById('name').value = employee.name;
    document.getElementById('email').value = employee.email;
    document.getElementById('position').value = employee.position;
    document.getElementById('department').value = employee.department;
    document.getElementById('salary').value = employee.salary;
    document.getElementById('joinDate').value = employee.joinDate;
    document.getElementById('formTitle').textContent = 'Edit Employee';
    document.querySelector('.btn-primary').innerHTML = '<i class="fas fa-save"></i> Update Employee';
    openEmployeeModal();
  }
}

// Render employee table
function renderEmployeeTable() {
  const tbody = document.getElementById('employeeTableBody');
  const emptyState = document.getElementById('emptyState');
  if (!tbody || !emptyState) return; // Guard against elements not existing

  // Filter
  const filtered = employees.filter(emp => {
    const matchesDept = !currentDepartmentFilter || emp.department === currentDepartmentFilter;
    const text = `${emp.name} ${emp.email} ${emp.position} ${emp.department}`.toLowerCase();
    const matchesSearch = !currentSearch || text.includes(currentSearch);
    return matchesDept && matchesSearch;
  });

  // Sort
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
    emptyState.style.display = 'block';
    return;
  }

  emptyState.style.display = 'none';
  tbody.innerHTML = sorted.map((employee, index) => `
        <tr style="animation: fadeIn 0.3s ease ${index * 0.05}s both">
            <td><strong>${employee.name}</strong></td>
            <td>${employee.email}</td>
            <td>${employee.position}</td>
            <td><span class="badge">${employee.department}</span></td>
            <td>$${parseInt(employee.salary).toLocaleString()}</td>
            <td>${formatDate(employee.joinDate)}</td>
            <td>
                <button class="action-btn edit-btn" onclick="editEmployee('${employee.id}')">
                    <i class="fas fa-edit"></i> Edit
                </button>
                <button class="action-btn delete-btn" onclick="openDeleteModal('${employee.id}')">
                    <i class="fas fa-trash"></i> Delete
                </button>
            </td>
        </tr>
    `).join('');
}

// Update statistics
function updateStats() {
  const totalEl = document.getElementById('totalEmployees');
  const deptEl = document.getElementById('totalDepartments');
  const avgEl = document.getElementById('avgSalary');
  if (!totalEl || !deptEl || !avgEl) return; // Guard against elements not existing

  totalEl.textContent = employees.length;

  const departments = [...new Set(employees.map(emp => emp.department))];
  deptEl.textContent = departments.length;

  const avgSalary = employees.length > 0
    ? employees.reduce((sum, emp) => sum + parseInt(emp.salary || 0, 10), 0) / employees.length
    : 0;
  avgEl.textContent = `$${Math.round(avgSalary).toLocaleString()}`;
}

// Update department filter
function updateDepartmentFilter() {
  const filter = document.getElementById('filterDepartment');
  if (!filter) return; // Guard against element not existing

  filter.innerHTML = '<option value="">All Departments</option>' +
    [...new Set(employees.map(emp => emp.department))]
      .map(dept => `<option value="${dept}">${dept}</option>`)
      .join('');
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

  return isValid;
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

// Theme toggle
function initializeTheme() {
  const savedTheme = localStorage.getItem('theme') || 'light';
  document.documentElement.setAttribute('data-theme', savedTheme);
  updateThemeIcon(savedTheme);
}

const themeToggle = document.getElementById('themeToggle');
if (themeToggle) {
  themeToggle.addEventListener('click', () => {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
  });
}

function updateThemeIcon(theme) {
  const icon = document.querySelector('#themeToggle i');
  if (icon) {
    icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
  }
}

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
        background: var(--primary-color);
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
  const signOutBtn = document.getElementById('signOutBtn');
  const userInfo = document.getElementById('userInfo');
  const userName = document.getElementById('userName');
  const userPhoto = document.getElementById('userPhoto');

  if (user) {
    if (signOutBtn) signOutBtn.style.display = 'inline-flex';
    if (userInfo) userInfo.style.display = 'inline-block';
    if (userName) userName.textContent = user.displayName || user.email || 'Signed in';
    if (userPhoto && user.photoURL) {
      userPhoto.src = user.photoURL;
      userPhoto.style.display = 'inline-block';
    } else if (userPhoto) {
      userPhoto.style.display = 'none';
    }
  } else {
    if (signOutBtn) signOutBtn.style.display = 'none';
    if (userInfo) userInfo.style.display = 'none';
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
