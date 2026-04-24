/**
 * Tiny theme manager. The initial class is set by an inline script in
 * `index.html` (before React mounts), so this module only handles
 * runtime toggles.
 */
export type Theme = "light" | "dark";

export function getTheme(): Theme {
  return document.documentElement.classList.contains("dark") ? "dark" : "light";
}

export function setTheme(theme: Theme): void {
  const root = document.documentElement;
  if (theme === "dark") root.classList.add("dark");
  else root.classList.remove("dark");
  try {
    localStorage.setItem("nebula-theme", theme);
  } catch {
    /* private mode / storage blocked — toggle still works for the session */
  }
}

export function toggleTheme(): Theme {
  const next: Theme = getTheme() === "dark" ? "light" : "dark";
  setTheme(next);
  return next;
}
