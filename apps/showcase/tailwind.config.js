/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  // `class` so dark mode is controlled by toggling a class on <html>,
  // which we persist to localStorage. `media` would force OS-theme
  // behavior and we want an in-app toggle.
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // A single brand accent used across tabs, buttons, links.
        // Keeping it narrow makes the UI feel coherent without us
        // inventing a full design system.
        brand: {
          50: "#eef6ff",
          100: "#d9e8ff",
          500: "#3b82f6",
          600: "#2563eb",
          700: "#1d4ed8",
        },
      },
      fontFamily: {
        mono: ["ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
    },
  },
  plugins: [],
};
