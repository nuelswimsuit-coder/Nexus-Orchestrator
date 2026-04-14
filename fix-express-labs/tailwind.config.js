/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,jsx,ts,tsx}'],
  theme: {
    extend: {
      colors: {
        electric: {
          DEFAULT: '#0070f3',
          light: '#3291ff',
          dark: '#0060df',
          glow: '#0070f340',
        },
        slate: {
          850: '#0f172a',
          950: '#020617',
        },
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'float': 'float 3s ease-in-out infinite',
        'glow': 'glow 2s ease-in-out infinite alternate',
      },
      keyframes: {
        float: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-8px)' },
        },
        glow: {
          '0%': { boxShadow: '0 0 5px #0070f3, 0 0 10px #0070f3' },
          '100%': { boxShadow: '0 0 20px #0070f3, 0 0 40px #0070f3' },
        },
      },
      backdropBlur: {
        xs: '2px',
      },
    },
  },
  plugins: [],
}
