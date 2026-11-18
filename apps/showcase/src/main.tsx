import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { SourceSite } from "./source/site/SourceSite";
import { currentSite } from "./tenant";
import "./index.css";

const root = document.getElementById("root");
if (!root) throw new Error("root element missing");
// One image, two sites: the combined showcase (internal) and the
// Source Supplies customer site, chosen by host (see tenant.ts).
createRoot(root).render(<StrictMode>{currentSite() === "sourcesupplies" ? <SourceSite /> : <App />}</StrictMode>);
