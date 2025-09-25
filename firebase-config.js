// Import Firebase SDKs from CDN for browser ESM usage
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAnalytics, isSupported as analyticsIsSupported } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-analytics.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyB_CAfPX-UC_668hVkCDPPrm8qn5qfBX4U",
  authDomain: "crm1-215ac.firebaseapp.com",
  projectId: "crm1-215ac",
  storageBucket: "crm1-215ac.firebasestorage.app",
  messagingSenderId: "166837046761",
  appId: "1:166837046761:web:7d118ebeed08f3b04b8f78",
  measurementId: "G-S35VYHJSKM"
};

// Initialize Firebase
const app = initializeApp(firebaseConfig);

// Initialize Analytics non-blocking (no top-level await for wider browser support)
analyticsIsSupported()
  .then((supported) => {
    if (supported) getAnalytics(app);
  })
  .catch(() => {
    // ignore analytics init errors
  });

const db = getFirestore(app);
const auth = getAuth(app);

export { db, auth };
