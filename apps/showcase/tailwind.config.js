/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        // "Ring Promoter" palette: a near-black control plane. Almost
        // monochrome; colour is reserved for status (green/amber/red).
        carbon: {
          950: "#000000", // page
          900: "#0A0A0C", // card / raised surface
          800: "#101013", // hover / inset
        },
        edge: "#212227", // hairline borders
        ink: "#EDEDEE", // primary text
        muted: "#8B8D93", // secondary text
        faint: "#6B6E76", // timestamps / captions
        // Status colours (GitHub-dark family).
        ok: "#3FB950",
        warn: "#D29922",
        bad: "#F85149",
        idle: "#484F58",
        // A single cool accent for interactive focus / active nav.
        accent: "#4C8DFF",
      },
      fontFamily: {
        sans: ['"Inter"', "system-ui", "-apple-system", "sans-serif"],
        mono: ['"JetBrains Mono"', "ui-monospace", "SFMono-Regular", "Menlo", "monospace"],
      },
      borderRadius: {
        xl: "0.9rem",
        "2xl": "1.15rem",
      },
      keyframes: {
        rise: {
          "0%": { opacity: "0", transform: "translateY(6px)" },
          "100%": { opacity: "1", transform: "translateY(0)" },
        },
        pulseok: {
          "0%,100%": { opacity: "1" },
          "50%": { opacity: "0.35" },
        },
      },
      animation: {
        rise: "rise 0.35s cubic-bezier(0.22,1,0.36,1) both",
        pulseok: "pulseok 2s ease-in-out infinite",
      },
    },
  },
  plugins: [],
};
