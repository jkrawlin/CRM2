import { db, auth } from './firebase-config.js';
import { 
    collection, 
    addDoc, 
    getDocs, 
    query, 
    orderBy 
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { 
    signInAnonymously,
    onAuthStateChanged 
} from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";

// DOM Elements
const modal = document.getElementById('modalOverlay');
const addCustomerBtn = document.getElementById('addCustomerBtn');
const closeBtn = document.querySelector('.close-btn');
const cancelBtn = document.querySelector('.cancel-btn');
const customerForm = document.getElementById('customerForm');
const navItems = document.querySelectorAll('.nav-item');
const contentSections = document.querySelectorAll('.content-section');
const pageTitle = document.getElementById('pageTitle');
const userEmail = document.getElementById('userEmail');
const logoutBtn = document.getElementById('logoutBtn');

// Initialize app
async function initApp() {
    // Try to sign in anonymously; if disabled, continue as public user
    try {
        await signInAnonymously(auth);
    } catch (error) {
        console.warn('Anonymous auth unavailable, continuing without auth.');
    }

    // Auth state listener (non-blocking)
    onAuthStateChanged(auth, (user) => {
        if (user) {
            userEmail.textContent = 'Guest User';
            document.querySelector('.avatar').textContent = 'G';
        } else {
            userEmail.textContent = 'Public User';
            document.querySelector('.avatar').textContent = 'P';
        }
    });

    // Load initial data regardless of auth
    await loadCustomers();
    await updateStats();
}

// Navigation
navItems.forEach(item => {
    item.addEventListener('click', (e) => {
        e.preventDefault();
        
        // Update active nav
        navItems.forEach(nav => nav.classList.remove('active'));
        item.classList.add('active');
        
        // Show corresponding section
        const targetId = item.getAttribute('href').substring(1);
        contentSections.forEach(section => {
            section.style.display = section.id === targetId ? 'block' : 'none';
        });
        
        // Update page title
        pageTitle.textContent = item.textContent.trim();
        
        // Show/hide add customer button
        addCustomerBtn.style.display = targetId === 'customers' ? 'block' : 'none';
    });
});

// Modal controls
addCustomerBtn.addEventListener('click', () => {
    modal.classList.add('active');
});

closeBtn.addEventListener('click', () => {
    modal.classList.remove('active');
    customerForm.reset();
});

cancelBtn.addEventListener('click', () => {
    modal.classList.remove('active');
    customerForm.reset();
});

modal.addEventListener('click', (e) => {
    if (e.target === modal) {
        modal.classList.remove('active');
        customerForm.reset();
    }
});

// Form submission
customerForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    
    const formData = new FormData(customerForm);
    const customerData = {
        name: formData.get('name'),
        email: formData.get('email'),
        phone: formData.get('phone') || '',
        company: formData.get('company') || '',
        createdAt: new Date().toISOString()
    };
    
    try {
        await addDoc(collection(db, 'customers'), customerData);
        modal.classList.remove('active');
        customerForm.reset();
        await loadCustomers();
        await updateStats();
        showNotification('Customer added successfully!');
    } catch (error) {
        console.error('Error adding customer:', error);
        showNotification('Error adding customer', 'error');
    }
});

// Load customers
async function loadCustomers() {
    const customersList = document.getElementById('customersList');
    customersList.innerHTML = '<div style="text-align: center; padding: 2rem;">Loading...</div>';
    
    try {
        const q = query(collection(db, 'customers'), orderBy('createdAt', 'desc'));
        const querySnapshot = await getDocs(q);
        
        customersList.innerHTML = '';
        
        if (querySnapshot.empty) {
            customersList.innerHTML = `
                <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                    <p style="font-size: 1.125rem; margin-bottom: 1rem;">No customers yet</p>
                    <p>Click "Add Customer" to get started</p>
                </div>
            `;
            return;
        }
        
        querySnapshot.forEach((doc) => {
            const customer = doc.data();
            const card = createCustomerCard(customer);
            customersList.appendChild(card);
        });
    } catch (error) {
        console.error('Error loading customers:', error);
        customersList.innerHTML = '<div style="text-align: center; padding: 2rem; color: var(--danger);">Error loading customers</div>';
    }
}

// Create customer card
function createCustomerCard(customer) {
    const card = document.createElement('div');
    card.className = 'customer-card';
    card.innerHTML = `
        <div class="customer-name">${customer.name}</div>
        <div class="customer-info">📧 ${customer.email}</div>
        ${customer.phone ? `<div class="customer-info">📱 ${customer.phone}</div>` : ''}
        ${customer.company ? `<div class="customer-info">🏢 ${customer.company}</div>` : ''}
    `;
    return card;
}

// Update dashboard stats
async function updateStats() {
    try {
        const customersSnapshot = await getDocs(collection(db, 'customers'));
        const totalCustomers = customersSnapshot.size;
        
        // Update stat cards
        document.querySelectorAll('.stat-value')[0].textContent = totalCustomers;
        
        // Calculate growth (mock data for demo)
        const growth = totalCustomers > 0 ? '+12%' : '0%';
        document.querySelectorAll('.stat-value')[3].textContent = growth;
    } catch (error) {
        console.error('Error updating stats:', error);
    }
}

// Show notification
function showNotification(message, type = 'success') {
    const notification = document.createElement('div');
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 1rem 1.5rem;
        background: ${type === 'success' ? 'var(--success)' : 'var(--danger)'};
        color: white;
        border-radius: 0.5rem;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
        z-index: 2000;
        animation: slideIn 0.3s ease;
    `;
    notification.textContent = message;
    
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}

// Add animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from { transform: translateX(100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(100%); opacity: 0; }
    }
`;
document.head.appendChild(style);

// Logout functionality
logoutBtn.addEventListener('click', () => {
    try { auth.signOut(); } catch (_) {}
    location.reload();
});

// Initialize the app
initApp();
