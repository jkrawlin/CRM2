// Import Firebase SDKs from CDN for browser ESM usage
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAnalytics, isSupported as analyticsIsSupported } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-analytics.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getStorage } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyBNv8o3mjpnqrHSMqI-hIrKcbHOWK5V-0Y",
  authDomain: "crm1-7ed70.firebaseapp.com",
  projectId: "crm1-7ed70",
  storageBucket: "crm1-7ed70.firebasestorage.app",
  messagingSenderId: "577746769811",
  appId: "1:577746769811:web:27aabfba857b749266ba66",
  measurementId: "G-YRSFEXJ6VX"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

// Initialize Analytics non-blocking
analyticsIsSupported()
  .then((supported) => {
    if (supported) getAnalytics(app);
  })
  .catch(() => {
    console.log('Analytics not supported in this environment');
  });

// Production-ready configuration without App Check
// App Check is optional - authentication provides security
const db = getFirestore(app);
const auth = getAuth(app);
// Use the actual default bucket that exists in your project
const storage = getStorage(app, "gs://crm1-7ed70.firebasestorage.app");

export { db, auth, storage };