// Import Firebase SDKs from CDN for browser ESM usage
import { initializeApp } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js";
import { getAnalytics, isSupported as analyticsIsSupported } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-analytics.js";
import { initializeFirestore } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js";
import { getAuth } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js";
import { getStorage } from "https://www.gstatic.com/firebasejs/10.12.0/firebase-storage.js";

// Your web app's Firebase configuration
const firebaseConfig = {
  apiKey: "AIzaSyBNv8o3mjpnqrHSMqI-hIrKcbHOWK5V-0Y",
  authDomain: "crm1-7ed70.firebaseapp.com",
  projectId: "crm1-7ed70",
  // Use the default storage bucket domain (appspot.com)
  storageBucket: "crm1-7ed70.appspot.com",
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

// Firestore: initialize with long-polling auto-detection to avoid WebChannel 400s
// This helps when corporate proxies/firewalls break the WebChannel transport.
const db = initializeFirestore(app, {
  ignoreUndefinedProperties: true,
  experimentalAutoDetectLongPolling: true,
  useFetchStreams: false,
});
const auth = getAuth(app);
// Use the actual default bucket that exists in your project
const storage = getStorage(app, "gs://crm1-7ed70.appspot.com");

export { db, auth, storage };