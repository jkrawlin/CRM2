# CRM2

Employee Management web app deployed on Firebase Hosting.

- Live: https://crm1-215ac.web.app
- Firebase project: crm1-215ac

## Tech
- Vanilla JS with Firebase Web SDK (Auth + Firestore)
- Hosting on Firebase

## Local dev
Open `index.html` with a local web server (or use the Firebase emulator/serve):

```powershell
npm run serve
```

## Deploy
```powershell
npm run deploy
```

## Notes
- Email/Password sign-in is expected; enable in Firebase Console if needed.
- Firestore rules are in `firestore.rules`.
