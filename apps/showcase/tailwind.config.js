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
        // "Observatory" palette. Dark-first ops console; boldness is
        // spent only on the plasma/signal accents (the semantic spectrum).
        void: "#0B0E14", // deepest background
        panel: "#12161B", // raised surface
        hairline: "#1E2530", // borders / dividers
        ink: "#E7ECF5", // primary text
        muted: "#8B95A7", // secondary text
        // plasma (violet) = the "nebula" accent; signal (cyan) = the
        // semantic / vector accent. Together they form the score spectrum.
        plasma: {
          DEFAULT: "#7C5CFF",
          soft: "#A48BFF",
          dim: "#5B44C4",
        },
        signal: {
          DEFAULT: "#2DE1C2",
          soft: "#7CF0DC",
          dim: "#17A98F",
        },
      },
      fontFamily: {
        display: ['"Space Grotesk"', "system-ui", "sans-serif"],
        sans: ['"IBM Plex Sans"', "system-ui", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      boxShadow: {
        instrument: "0 1px 0 0 rgba(255,255,255,0.03) inset, 0 8px 30px -12px rgba(3,6,12,0.85)",
        glow: "0 0 0 1px rgba(124,92,255,0.35), 0 8px 30px -8px rgba(124,92,255,0.30)",
      },
      backgroundImage: {
        // The signature: violet -> cyan semantic-similarity spectrum.
        spectrum: "linear-gradient(90deg, #5B44C4 0%, #7C5CFF 45%, #2DE1C2 100%)",
      },
      keyframes: {
        rise: {
          "0%": { opacity: "0", transform: "translateY(6px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        drift: {
          "0%": { transform: "translate3d(0,0,0)" },
          "100%": { transform: "translate3d(-40px,-28px,0)" },
        },
      },
      animation: {
        rise: "rise 0.4s cubic-bezier(0.22,1,0.36,1) both",
        drift: "drift 60s linear infinite alternate",
      },
    },
  },
  plugins: [],
};
