/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './index.html',
    './script.js',
    './modules/**/*.js'
  ],
  safelist: [
    // Ensure dynamically generated classes are not purged
    'bg-amber-50', 'bg-amber-100', 'text-amber-800',
    'bg-emerald-50', 'bg-emerald-100', 'text-emerald-800'
  ],
  theme: {
    extend: {
      colors: {
        brand: {
          DEFAULT: '#6366F1',
          dark: '#4F46E5',
        },
      },
      boxShadow: {
        card: '0 10px 15px -3px rgba(0,0,0,0.1), 0 4px 6px -2px rgba(0,0,0,0.05)'
      },
      borderRadius: {
        xl: '1rem'
      }
    },
  },
  plugins: [],
};
