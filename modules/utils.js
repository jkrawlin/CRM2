// Shared UI/data helpers

export function formatDate(dateString) {
  const options = { year: 'numeric', month: 'short', day: 'numeric' };
  return new Date(dateString).toLocaleDateString(undefined, options);
}

export function maskAccount(acc) {
  if (!acc) return '-';
  const s = String(acc).replace(/\s+/g, '');
  if (s.length <= 4) return s;
  return '•••• ' + s.slice(-4);
}

// Determine expiry status (Qatar ID or Passport) for an employee
// Returns an object with color ('red' | 'green') and a title tooltip string
export function getExpiryIndicator(employee) {
  if (!employee || typeof employee !== 'object') {
    return { color: 'green', title: 'No expiry information' };
  }

  // Collect possible expiry fields with labels for tooltip
  const candidates = [];
  const pushIfValid = (val, label) => {
    if (!val) return;
    // Expect YYYY-MM-DD, but be tolerant of Date-parsable values
    const d = new Date(val);
    if (!isNaN(d.getTime())) {
      candidates.push({ date: new Date(d.getFullYear(), d.getMonth(), d.getDate()), label });
    }
  };

  // Canonical
  pushIfValid(employee.qidExpiry, 'Qatar ID');
  pushIfValid(employee.passportExpiry, 'Passport');
  // Common legacy aliases (defensive)
  pushIfValid(employee.qid_expiry, 'Qatar ID');
  pushIfValid(employee.passport_expiry, 'Passport');
  pushIfValid(employee.QIDExpiry, 'Qatar ID');
  pushIfValid(employee.PassportExpiry, 'Passport');
  pushIfValid(employee.qidExpire, 'Qatar ID');
  pushIfValid(employee.passportExpire, 'Passport');
  pushIfValid(employee.qidExpireDate, 'Qatar ID');
  pushIfValid(employee.passportExpireDate, 'Passport');

  if (candidates.length === 0) {
    return { color: 'green', title: 'No expiry dates on file' };
  }

  // Choose the earliest upcoming/expired date
  candidates.sort((a, b) => a.date - b.date);
  const { date, label } = candidates[0];

  const today = new Date();
  const startOfToday = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const msPerDay = 24 * 60 * 60 * 1000;
  const diffMs = date.getTime() - startOfToday.getTime();
  const days = Math.ceil(diffMs / msPerDay);

  const isSoonOrExpired = days <= 30; // includes expired (days <= 0)
  const color = isSoonOrExpired ? 'red' : 'green';

  const yyyy = String(date.getFullYear());
  const mm = String(date.getMonth() + 1).padStart(2, '0');
  const dd = String(date.getDate()).padStart(2, '0');
  const onDate = `${yyyy}-${mm}-${dd}`;

  let title;
  if (days < 0) {
    title = `${label} expired ${Math.abs(days)} day${Math.abs(days) === 1 ? '' : 's'} ago (on ${onDate})`;
  } else if (days === 0) {
    title = `${label} expires today (${onDate})`;
  } else if (days === 1) {
    title = `${label} expires in 1 day (${onDate})`;
  } else {
    title = `${label} expires in ${days} days (${onDate})`;
  }

  return { color, title };
}
