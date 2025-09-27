// Import Firebase SDKs from CDN for browser ESM usage
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAnalytics, isSupported as analyticsIsSupported } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-analytics.js";
import { getFirestore } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getStorage } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";
import { initializeAppCheck, ReCaptchaV3Provider } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app-check.js";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyBNv8o3mjpnqrHSMqI-hIrKcbHOWK5V-0Y",
  authDomain: "crm1-7ed70.firebaseapp.com",
  projectId: "crm1-7ed70",
  storageBucket: "crm1-7ed70.appspot.com",
  messagingSenderId: "577746769811",
  appId: "1:577746769811:web:27aabfba857b749266ba66",
  measurementId: "G-YRSFEXJ6VX"
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

// Initialize App Check with reCAPTCHA v3 to satisfy Storage/App Check enforcement
// Note: use the site key you configured in Firebase Console (provided by user)
initializeAppCheck(app, {
  provider: new ReCaptchaV3Provider("123nj"),
  isTokenAutoRefreshEnabled: true,
});

const db = getFirestore(app);
const auth = getAuth(app);
const storage = getStorage(app);

export { db, auth, storage };
