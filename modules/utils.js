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
