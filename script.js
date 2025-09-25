let employees = JSON.parse(localStorage.getItem('employees')) || [];
let editingId = null;
let deleteId = null;
let currentSort = { field: null, ascending: true };

// Initialize on page load
document.addEventListener('DOMContentLoaded', () => {
    displayEmployees();
    updateStats();
    populateDepartmentFilter();
    initializeTheme();
    
    // Set default join date to today
    document.getElementById('joinDate').valueAsDate = new Date();
    
    // Add event listeners
    document.getElementById('searchInput').addEventListener('input', filterEmployees);
    document.getElementById('filterDepartment').addEventListener('change', filterEmployees);
});

// Enhanced form submission with validation
document.getElementById('employeeForm').addEventListener('submit', (e) => {
    e.preventDefault();
    
    if (!validateForm()) {
        showToast('Please fill all required fields correctly', 'error');
        return;
    }
    
    const employee = {
        id: editingId || Date.now().toString(),
        name: document.getElementById('name').value.trim(),
        email: document.getElementById('email').value.trim(),
        position: document.getElementById('position').value.trim(),
        department: document.getElementById('department').value,
        salary: document.getElementById('salary').value,
        joinDate: document.getElementById('joinDate').value
    };
    
    if (editingId) {
        const index = employees.findIndex(emp => emp.id === editingId);
        employees[index] = employee;
        showToast('Employee updated successfully!', 'success');
        editingId = null;
        document.getElementById('formTitle').textContent = 'Add New Employee';
    } else {
        employees.push(employee);
        showToast('Employee added successfully!', 'success');
    }
    
    saveToLocalStorage();
    displayEmployees();
    updateStats();
    populateDepartmentFilter();
    clearForm();
    
    // Smooth scroll to table
    document.querySelector('.table-container').scrollIntoView({ behavior: 'smooth' });
});

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

// Enhanced display with animations
function displayEmployees(employeeList = employees) {
    const tbody = document.getElementById('employeeTableBody');
    const emptyState = document.getElementById('emptyState');
    
    if (employeeList.length === 0) {
        tbody.innerHTML = '';
        emptyState.style.display = 'block';
        return;
    }
    
    emptyState.style.display = 'none';
    tbody.innerHTML = employeeList.map((employee, index) => `
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
                <button class="action-btn delete-btn" onclick="confirmDelete('${employee.id}')">
                    <i class="fas fa-trash"></i> Delete
                </button>
            </td>
        </tr>
    `).join('');
}

// Filter employees
function filterEmployees() {
    const searchTerm = document.getElementById('searchInput').value.toLowerCase();
    const departmentFilter = document.getElementById('filterDepartment').value;
    
    let filtered = employees.filter(emp => {
        const matchesSearch = emp.name.toLowerCase().includes(searchTerm) ||
                             emp.email.toLowerCase().includes(searchTerm) ||
                             emp.position.toLowerCase().includes(searchTerm);
        const matchesDepartment = !departmentFilter || emp.department === departmentFilter;
        
        return matchesSearch && matchesDepartment;
    });
    
    displayEmployees(filtered);
}

// Sort table
function sortTable(field) {
    if (currentSort.field === field) {
        currentSort.ascending = !currentSort.ascending;
    } else {
        currentSort.field = field;
        currentSort.ascending = true;
    }
    
    employees.sort((a, b) => {
        let aVal = a[field];
        let bVal = b[field];
        
        if (field === 'salary') {
            aVal = parseInt(aVal);
            bVal = parseInt(bVal);
        } else {
            aVal = aVal.toLowerCase();
            bVal = bVal.toLowerCase();
        }
        
        if (aVal < bVal) return currentSort.ascending ? -1 : 1;
        if (aVal > bVal) return currentSort.ascending ? 1 : -1;
        return 0;
    });
    
    displayEmployees();
}

// Update statistics
function updateStats() {
    document.getElementById('totalEmployees').textContent = employees.length;
    
    const departments = [...new Set(employees.map(emp => emp.department))];
    document.getElementById('totalDepartments').textContent = departments.length;
    
    const avgSalary = employees.length > 0 
        ? employees.reduce((sum, emp) => sum + parseInt(emp.salary), 0) / employees.length
        : 0;
    document.getElementById('avgSalary').textContent = `$${Math.round(avgSalary).toLocaleString()}`;
}

// Populate department filter
function populateDepartmentFilter() {
    const departments = [...new Set(employees.map(emp => emp.department))];
    const filter = document.getElementById('filterDepartment');
    
    filter.innerHTML = '<option value="">All Departments</option>' + 
        departments.map(dept => `<option value="${dept}">${dept}</option>`).join('');
}

// Edit employee
function editEmployee(id) {
    const employee = employees.find(emp => emp.id === id);
    if (employee) {
        document.getElementById('name').value = employee.name;
        document.getElementById('email').value = employee.email;
        document.getElementById('position').value = employee.position;
        document.getElementById('department').value = employee.department;
        document.getElementById('salary').value = employee.salary;
        document.getElementById('joinDate').value = employee.joinDate;
        editingId = id;
        
        document.getElementById('formTitle').textContent = 'Edit Employee';
        document.querySelector('.form-container').scrollIntoView({ behavior: 'smooth' });
        
        // Add highlight effect
        document.querySelector('.form-container').style.animation = 'pulse 0.5s ease';
        setTimeout(() => {
            document.querySelector('.form-container').style.animation = '';
        }, 500);
    }
}

// Confirm delete with modal
function confirmDelete(id) {
    deleteId = id;
    document.getElementById('deleteModal').classList.add('show');
}

// Close modal
function closeModal() {
    document.getElementById('deleteModal').classList.remove('show');
    deleteId = null;
}

// Delete employee
document.getElementById('confirmDelete').addEventListener('click', () => {
    if (deleteId) {
        employees = employees.filter(emp => emp.id !== deleteId);
        saveToLocalStorage();
        displayEmployees();
        updateStats();
        populateDepartmentFilter();
        showToast('Employee deleted successfully!', 'success');
        closeModal();
    }
});

// Clear form
function clearForm() {
    document.getElementById('employeeForm').reset();
    document.getElementById('joinDate').valueAsDate = new Date();
    document.querySelectorAll('.error').forEach(el => el.classList.remove('error'));
    editingId = null;
    document.getElementById('formTitle').textContent = 'Add New Employee';
}

// Toast notifications
function showToast(message, type = 'success') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    
    const icon = type === 'success' ? 'fa-check-circle' : 
                 type === 'error' ? 'fa-exclamation-circle' : 'fa-info-circle';
    
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

document.getElementById('themeToggle').addEventListener('click', () => {
    const currentTheme = document.documentElement.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    
    document.documentElement.setAttribute('data-theme', newTheme);
    localStorage.setItem('theme', newTheme);
    updateThemeIcon(newTheme);
});

function updateThemeIcon(theme) {
    const icon = document.querySelector('#themeToggle i');
    icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
}

// Format date
function formatDate(dateString) {
    const options = { year: 'numeric', month: 'short', day: 'numeric' };
    return new Date(dateString).toLocaleDateString(undefined, options);
}

// Save to localStorage
function saveToLocalStorage() {
    localStorage.setItem('employees', JSON.stringify(employees));
}

// Add pulse animation
const style = document.createElement('style');
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
