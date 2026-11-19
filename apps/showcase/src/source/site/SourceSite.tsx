/**
 * The Source Supplies customer site (sourcesupplies.nebuladb.net).
 *
 * Same image as the showcase; main.tsx renders this instead of <App/>
 * when the host is a Source Supplies one (see tenant.ts). It shows only
 * the Source Supplies demos and Labs (Source pack), in a customer-friendly
 * shell: big readable type, search, teaser cards with the win up front,
 * and every video one click away. The combined showcase stays our own
 * internal test bed.
 */
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { getTheme, toggleTheme } from "../../theme";
import { fetchShowcaseUser, showcaseSignOutUrl, type ShowcaseUser } from "../../auth";
import { startAutoCapture } from "../../analytics";
import { LabsTab } from "../../labs/LabsTab";
import { ServiceChannelTab } from "../ServiceChannelTab";
import { StockPulseTab } from "../StockPulseTab";
import { ReportDeskTab } from "../ReportDeskTab";
import { EmailOrderTab } from "../EmailOrderTab";
import { ReorderTab } from "../ReorderTab";
import { WashroomTab } from "../WashroomTab";
import { FinderTab } from "../FinderTab";
import { SafetyTab } from "../SafetyTab";
import { ProblemSolverTab } from "../ProblemSolverTab";
import { TenderTab } from "../TenderTab";
import { COMBINED, DEMOS, DEMO_BY_ID, GROUPS, searchDemos, type Demo, type DemoId, type Video } from "./catalog";

type Route = { page: "home" } | { page: "videos" } | { page: "demo"; id: DemoId };

function readRoute(): Route {
  const h = window.location.hash;
  if (h.startsWith("#/labs")) return { page: "demo", id: "labs" };
  if (h.startsWith("#/videos")) return { page: "videos" };
  const m = /^#\/tab\/([a-z]+)/.exec(h);
  if (m && m[1] in DEMO_BY_ID) return { page: "demo", id: m[1] as DemoId };
  return { page: "home" };
}

const go = (hash: string) => {
  window.location.hash = hash;
};
const demoHash = (id: DemoId) => (id === "labs" ? "#/labs?pack=source" : `#/tab/${id}`);

const PANELS: Record<Exclude<DemoId, "labs">, () => ReactNode> = {
  ssretail: () => <ServiceChannelTab />,
  ssstock: () => <StockPulseTab />,
  ssreports: () => <ReportDeskTab />,
  ssemail: () => <EmailOrderTab />,
  ssreorder: () => <ReorderTab />,
  sswashroom: () => <WashroomTab />,
  ssfinder: () => <FinderTab />,
  sssafety: () => <SafetyTab />,
  sssolver: () => <ProblemSolverTab />,
  sstender: () => <TenderTab />,
};

export function SourceSite() {
  const [route, setRoute] = useState<Route>(readRoute);
  const [theme, setThemeState] = useState(getTheme());
  const [playing, setPlaying] = useState<Video | null>(null);
  const [navOpen, setNavOpen] = useState(false);
  const mainRef = useRef<HTMLElement>(null);

  // Visitor analytics (customer sites only): pages, video progress, clicks, demo inputs.
  useEffect(() => {
    const titles = new Map<string, string>();
    for (const v of [COMBINED, ...DEMOS.map((d) => d.video)]) if (v) titles.set(v.src, v.title);
    return startAutoCapture((src) => titles.get(src));
  }, []);

  useEffect(() => {
    const on = () => {
      setRoute(readRoute());
      setNavOpen(false);
      mainRef.current?.scrollTo({ top: 0 });
    };
    window.addEventListener("hashchange", on);
    return () => window.removeEventListener("hashchange", on);
  }, []);

  useEffect(() => {
    const d = route.page === "demo" ? DEMO_BY_ID[route.id].name : route.page === "videos" ? "Video tours" : "Home";
    document.title = `${d} · Source Supplies × Jordan AI`;
  }, [route]);

  return (
    <div className="ss-site h-full flex flex-col bg-white text-gray-900 dark:bg-carbon-950 dark:text-ink">
      <Header theme={theme} onTheme={() => setThemeState(toggleTheme())} onMenu={() => setNavOpen((o) => !o)} onPlay={setPlaying} />
      <div className="flex-1 flex min-h-0">
        <Nav route={route} open={navOpen} />
        <main ref={mainRef} className="flex-1 min-w-0 overflow-y-auto">
          <div className="mx-auto max-w-[1400px] px-4 py-6 md:px-8 md:py-8">
            {route.page === "home" && <Home onPlay={setPlaying} />}
            {route.page === "videos" && <Videos onPlay={setPlaying} />}
            {route.page === "demo" && <DemoPage demo={DEMO_BY_ID[route.id]} onPlay={setPlaying} />}
            <Footer />
          </div>
        </main>
      </div>
      {playing && <VideoModal video={playing} onClose={() => setPlaying(null)} />}
    </div>
  );
}

/* ------------------------------ chrome ------------------------------ */

function Logo() {
  return (
    <a href="#/" className="flex items-center gap-3 min-w-0">
      <span className="grid h-10 w-10 shrink-0 place-items-center rounded-xl bg-gradient-to-br from-emerald-500 to-sky-600 font-display text-base font-bold text-white">SS</span>
      <span className="min-w-0 leading-tight">
        <span className="block font-display text-[17px] font-semibold truncate">Source Supplies × Jordan AI</span>
        <span className="block text-[13px] text-gray-500 dark:text-muted truncate">Live demos on dummy data</span>
      </span>
    </a>
  );
}

function SignedIn() {
  const [user, setUser] = useState<ShowcaseUser | null>(null);
  useEffect(() => {
    void fetchShowcaseUser().then(setUser);
  }, []);
  if (!user) return null;
  return (
    <div className="hidden lg:flex items-center gap-2 text-[13px] text-gray-500 dark:text-muted">
      <span className="max-w-[12rem] truncate" title={user.email}>
        {user.user === user.email ? user.email : `${user.user}`}
      </span>
      <a href={showcaseSignOutUrl()} className="rounded-lg border border-gray-200 dark:border-edge px-2.5 py-1.5 hover:border-accent">
        Sign out
      </a>
    </div>
  );
}

function Header({ theme, onTheme, onMenu, onPlay }: { theme: string; onTheme: () => void; onMenu: () => void; onPlay: (v: Video) => void }) {
  return (
    <header className="shrink-0 border-b border-gray-200 dark:border-edge bg-white/90 dark:bg-carbon-950/90 backdrop-blur">
      <div className="flex h-16 items-center gap-3 px-4 md:px-6">
        <button type="button" className="lg:hidden rounded-lg border border-gray-200 dark:border-edge px-2.5 py-1.5" onClick={onMenu} aria-label="Menu">
          ☰
        </button>
        <Logo />
        <div className="ml-auto flex items-center gap-2 min-w-0">
          <div className="hidden sm:block">
            <Search />
          </div>
          <button type="button" onClick={() => go("#/videos")} className="hidden sm:inline-flex items-center gap-1.5 rounded-lg border border-gray-200 dark:border-edge px-3 py-2 text-[15px] hover:border-accent">
            ▶ Videos
          </button>
          <button type="button" onClick={() => onPlay(COMBINED)} className="hidden md:inline-flex items-center rounded-lg bg-accent px-3.5 py-2 text-[15px] font-medium text-white hover:bg-accent/90">
            Watch the full tour
          </button>
          <SignedIn />
          <button type="button" onClick={onTheme} className="rounded-lg border border-gray-200 dark:border-edge px-2.5 py-2" aria-label="Toggle theme">
            {theme === "dark" ? "☀" : "☾"}
          </button>
        </div>
      </div>
    </header>
  );
}

function Search({ full = false }: { full?: boolean }) {
  const [q, setQ] = useState("");
  const [open, setOpen] = useState(false);
  const hits = useMemo(() => (q.trim() ? searchDemos(q).slice(0, 7) : []), [q]);
  const pick = (d: Demo) => {
    go(demoHash(d.id));
    setQ("");
    setOpen(false);
  };
  return (
    <div className={`relative ${full ? "w-full" : "w-64 md:w-80"}`}>
      <input
        value={q}
        onChange={(e) => {
          setQ(e.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && hits[0]) pick(hits[0]);
          if (e.key === "Escape") setOpen(false);
        }}
        placeholder="Search demos… e.g. stock, POD, report"
        aria-label="Search demos"
        data-tour="search"
        className="w-full rounded-lg border border-gray-200 dark:border-edge bg-gray-50 dark:bg-carbon-900 px-3.5 py-2 text-[15px] outline-none focus:border-accent"
      />
      {open && q.trim() && (
        <div className="absolute right-0 top-full z-40 mt-1 w-[min(28rem,90vw)] rounded-xl border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900 p-1.5 shadow-xl">
          {hits.length === 0 && <div className="px-3 py-2 text-[15px] text-gray-500 dark:text-muted">No demo matches “{q}”.</div>}
          {hits.map((d) => (
            <button key={d.id} type="button" onMouseDown={() => pick(d)} className="flex w-full items-start gap-3 rounded-lg px-3 py-2 text-left hover:bg-gray-100 dark:hover:bg-carbon-800">
              <span className="text-xl leading-none mt-0.5">{d.icon}</span>
              <span className="min-w-0">
                <span className="block text-[15px] font-medium">{d.name}</span>
                <span className="block text-[13px] text-gray-500 dark:text-muted line-clamp-1">{d.problem}</span>
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function Nav({ route, open }: { route: Route; open: boolean }) {
  const active = route.page === "demo" ? route.id : route.page;
  const item = (key: string, href: string, label: string, icon: string, extra?: ReactNode) => (
    <a
      key={key}
      href={href}
      className={`flex items-center gap-3 rounded-lg px-3 py-2 text-[15px] ${
        active === key ? "bg-accent/10 text-accent font-medium" : "text-gray-700 dark:text-muted hover:bg-gray-100 dark:hover:bg-carbon-800"
      }`}
    >
      <span className="w-6 text-center">{icon}</span>
      <span className="flex-1 truncate">{label}</span>
      {extra}
    </a>
  );
  return (
    <nav
      className={`${open ? "block" : "hidden"} lg:block absolute lg:static inset-x-0 top-16 bottom-0 z-30 w-full lg:w-72 shrink-0 overflow-y-auto border-r border-gray-200 dark:border-edge bg-white dark:bg-carbon-950 p-3`}
    >
      <div className="sm:hidden mb-3">
        <Search full />
      </div>
      {item("home", "#/", "Home", "⌂")}
      {item("videos", "#/videos", "All video tours", "▶")}
      {GROUPS.map((g) => (
        <div key={g.id} className="mt-4">
          <div className="px-3 pb-1 text-[12px] font-semibold uppercase tracking-wider text-gray-400 dark:text-faint">{g.label}</div>
          {DEMOS.filter((d) => d.group === g.id).map((d) =>
            item(d.id, demoHash(d.id), d.name, d.icon, d.video?.narrated ? <span className="text-[11px] text-gray-400 dark:text-faint">▶ {d.video.length}</span> : undefined)
          )}
        </div>
      ))}
    </nav>
  );
}

function Footer() {
  return (
    <footer className="mt-12 border-t border-gray-200 dark:border-edge pt-5 text-[13px] text-gray-500 dark:text-muted flex flex-wrap gap-x-6 gap-y-2">
      <span>All products, customers, orders and figures here are invented for this demo.</span>
      <span>Runs on NebulaDB · built for Source Supplies by Workstation</span>
    </footer>
  );
}

/* ------------------------------ pieces ------------------------------ */

function Poster({ video, onPlay, big }: { video: Video; onPlay: (v: Video) => void; big?: boolean }) {
  return (
    <button type="button" onClick={() => onPlay(video)} className="group relative block w-full overflow-hidden rounded-xl border border-gray-200 dark:border-edge bg-black aspect-video text-left" aria-label={`Play ${video.title}`}>
      <img src={video.poster} alt="" loading="lazy" className="h-full w-full object-cover object-top opacity-80 transition group-hover:opacity-95 group-hover:scale-[1.02]" />
      <span className="absolute inset-0 bg-gradient-to-t from-black/90 via-black/30 to-transparent" />
      <span className={`absolute left-4 top-4 grid place-items-center rounded-full bg-white/95 text-black shadow-xl transition group-hover:scale-110 ${big ? "h-16 w-16 text-2xl" : "h-12 w-12 text-lg"}`}>▶</span>
      <span className="absolute inset-x-4 bottom-3 flex items-end justify-between gap-3">
        <span className={`font-display font-semibold leading-tight text-white drop-shadow ${big ? "text-2xl" : "text-[17px]"}`}>{video.title.replace(/ — .*$/, "")}</span>
        <span className="shrink-0 rounded-md bg-black/70 px-2 py-0.5 text-[13px] text-white">{video.length}</span>
      </span>
    </button>
  );
}

function VideoModal({ video, onClose }: { video: Video; onClose: () => void }) {
  useEffect(() => {
    const k = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", k);
    return () => window.removeEventListener("keydown", k);
  }, [onClose]);
  return (
    <div className="fixed inset-0 z-50 grid place-items-center bg-black/80 p-4" onClick={onClose} role="dialog" aria-label={video.title}>
      <div className="w-full max-w-6xl" onClick={(e) => e.stopPropagation()}>
        <div className="mb-2 flex items-center justify-between text-white">
          <div className="font-display text-lg">{video.title}</div>
          <button type="button" onClick={onClose} className="rounded-lg border border-white/30 px-3 py-1 text-[15px] hover:bg-white/10">
            Close ✕
          </button>
        </div>
        <video src={video.src} poster={video.poster} controls autoPlay className="w-full rounded-xl bg-black aspect-video" />
      </div>
    </div>
  );
}

function DemoCard({ d, onPlay }: { d: Demo; onPlay: (v: Video) => void }) {
  return (
    <div className="flex flex-col rounded-2xl border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900 p-5 transition hover:border-accent/60 hover:shadow-lg">
      <div className="flex items-start gap-3">
        <span className="text-3xl leading-none">{d.icon}</span>
        <div className="min-w-0">
          <a href={demoHash(d.id)} className="font-display text-[19px] font-semibold hover:text-accent">
            {d.name}
          </a>
          <div className="text-[13px] text-gray-500 dark:text-muted">{d.who}</div>
        </div>
      </div>
      <p className="mt-3 text-[15px] text-gray-700 dark:text-muted">{d.problem}</p>
      <div className="mt-3 rounded-xl bg-ok/10 px-3 py-2">
        <span className="font-display text-xl font-semibold text-ok">{d.win}</span> <span className="text-[14px] text-gray-700 dark:text-muted">{d.winLabel}</span>
      </div>
      <div className="mt-auto flex flex-wrap gap-2 pt-4">
        <a href={demoHash(d.id)} className="rounded-lg bg-accent px-3.5 py-2 text-[15px] font-medium text-white hover:bg-accent/90">
          Try it live →
        </a>
        {d.video && (
          <button type="button" onClick={() => onPlay(d.video!)} className="rounded-lg border border-gray-200 dark:border-edge px-3.5 py-2 text-[15px] hover:border-accent">
            ▶ Watch {d.video.length}
          </button>
        )}
      </div>
    </div>
  );
}

/* ------------------------------ pages ------------------------------ */

function Home({ onPlay }: { onPlay: (v: Video) => void }) {
  const [q, setQ] = useState("");
  const [group, setGroup] = useState<Demo["group"] | "all">("all");
  const top = DEMOS.filter((d) => d.group === "top3");
  const shown = searchDemos(q).filter((d) => group === "all" || d.group === group);
  return (
    <div className="space-y-12">
      <section className="grid gap-8 lg:grid-cols-[1.1fr_1fr] items-center" data-tour="hero">
        <div>
          <div className="text-[13px] font-semibold uppercase tracking-[0.2em] text-accent">Prepared for Source Supplies · from our discovery call</div>
          <h1 className="mt-3 font-display text-4xl md:text-5xl font-bold leading-tight">Your three biggest headaches, handled by AI.</h1>
          <p className="mt-4 text-[18px] leading-relaxed text-gray-600 dark:text-muted">
            Retail PODs, stock that runs out, and customer reports stuck behind Merlin. Each one is a working demo you can click through right now, running
            on a real AI database with made-up Source Supplies data.
          </p>
          <div className="mt-6 flex flex-wrap gap-3">
            <button type="button" onClick={() => onPlay(COMBINED)} className="rounded-xl bg-accent px-5 py-3 text-[17px] font-medium text-white hover:bg-accent/90">
              ▶ Watch all three (5½ min)
            </button>
            <a href="#/tab/ssretail" className="rounded-xl border border-gray-300 dark:border-edge px-5 py-3 text-[17px] hover:border-accent">
              Try the live demos →
            </a>
          </div>
        </div>
        <Poster video={COMBINED} onPlay={onPlay} big />
      </section>

      <section className="grid grid-cols-2 lg:grid-cols-4 gap-3" data-tour="stats">
        {[
          ["≈ £22k", "a year back on retail orders and PODs"],
          ["87%", "of deliveries proven with no person"],
          ["−60%", "stock-outs on the lines that matter"],
          ["Weeks → secs", "for any customer report"],
        ].map(([v, l]) => (
          <div key={l} className="rounded-2xl border border-gray-200 dark:border-edge p-4">
            <div className="font-display text-2xl md:text-3xl font-bold text-ok">{v}</div>
            <div className="mt-1 text-[14px] text-gray-600 dark:text-muted">{l}</div>
          </div>
        ))}
      </section>

      <section>
        <h2 className="font-display text-2xl font-semibold">Your top 3, solved</h2>
        <p className="mt-1 text-[16px] text-gray-600 dark:text-muted">In the order you gave them. Each has a narrated tour and a live demo.</p>
        <div className="mt-5 grid gap-5 lg:grid-cols-3">
          {top.map((d, i) => (
            <div key={d.id} className="flex flex-col rounded-2xl border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900 p-4">
              <Poster video={d.video!} onPlay={onPlay} />
              <div className="mt-4 flex items-baseline gap-2">
                <span className="font-display text-xl text-accent">#{i + 1}</span>
                <a href={demoHash(d.id)} className="font-display text-xl font-semibold hover:text-accent">
                  {d.name}
                </a>
              </div>
              {d.quote && <p className="mt-2 text-[15px] italic text-gray-600 dark:text-muted">“{d.quote}”</p>}
              <p className="mt-2 text-[15px] text-gray-700 dark:text-muted">{d.does}</p>
              <div className="mt-3 rounded-xl bg-ok/10 px-3 py-2">
                <span className="font-display text-xl font-semibold text-ok">{d.win}</span> <span className="text-[14px] text-gray-700 dark:text-muted">{d.winLabel}</span>
              </div>
              <div className="mt-auto flex gap-2 pt-4">
                <a href={demoHash(d.id)} className="rounded-lg bg-accent px-3.5 py-2 text-[15px] font-medium text-white hover:bg-accent/90">
                  Try it live →
                </a>
                <button type="button" onClick={() => onPlay(d.video!)} className="rounded-lg border border-gray-200 dark:border-edge px-3.5 py-2 text-[15px] hover:border-accent">
                  ▶ {d.video!.length}
                </button>
              </div>
            </div>
          ))}
        </div>
      </section>

      <section className="rounded-2xl border border-gray-200 dark:border-edge bg-gray-50 dark:bg-carbon-900 p-6" data-tour="how">
        <h2 className="font-display text-2xl font-semibold">How it fits your business</h2>
        <div className="mt-5 grid gap-4 md:grid-cols-3">
          {[
            ["1 · Plugs into what you have", "Merlin, Sage, your website and the shared inbox. Nothing is replaced; Jordan reads and drafts, your team approves."],
            ["2 · Jordan AI, one assistant", "Specialist agents behind one chat: orders, stock, reports, safety, tenders. Ask in plain English, by typing or speaking."],
            ["3 · Your data stays with you", "Runs on a server in your office. No customer data sent to public AI services. Every correction your team makes teaches it."],
          ].map(([t, b]) => (
            <div key={t} className="rounded-xl bg-white dark:bg-carbon-950 border border-gray-200 dark:border-edge p-4">
              <div className="font-display text-[17px] font-semibold">{t}</div>
              <p className="mt-2 text-[15px] text-gray-600 dark:text-muted">{b}</p>
            </div>
          ))}
        </div>
      </section>

      <section data-tour="all-demos">
        <div className="flex flex-wrap items-end justify-between gap-3">
          <div>
            <h2 className="font-display text-2xl font-semibold">Every demo</h2>
            <p className="mt-1 text-[16px] text-gray-600 dark:text-muted">Beyond the top 3: more everyday jobs Jordan can take off your team.</p>
          </div>
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Filter… e.g. washroom, tender, eco"
            aria-label="Filter demos"
            className="w-full sm:w-80 rounded-lg border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900 px-3.5 py-2 text-[15px] outline-none focus:border-accent"
          />
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          {[{ id: "all" as const, label: "All" }, ...GROUPS].map((g) => (
            <button
              key={g.id}
              type="button"
              onClick={() => setGroup(g.id)}
              className={`rounded-full border px-3.5 py-1.5 text-[14px] ${group === g.id ? "border-accent bg-accent/10 text-accent" : "border-gray-200 dark:border-edge text-gray-600 dark:text-muted"}`}
            >
              {g.label}
            </button>
          ))}
        </div>
        <div className="mt-5 grid gap-5 sm:grid-cols-2 xl:grid-cols-3">
          {shown.map((d) => (
            <DemoCard key={d.id} d={d} onPlay={onPlay} />
          ))}
          {shown.length === 0 && <p className="text-[15px] text-gray-500 dark:text-muted">No demo matches “{q}”.</p>}
        </div>
      </section>
    </div>
  );
}

function Videos({ onPlay }: { onPlay: (v: Video) => void }) {
  const [q, setQ] = useState("");
  const list = [{ d: null as Demo | null, v: COMBINED }, ...DEMOS.filter((d) => d.video).map((d) => ({ d, v: d.video! }))].filter(
    ({ d, v }) => !q.trim() || (d ? searchDemos(q).includes(d) : v.title.toLowerCase().includes(q.toLowerCase()))
  );
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="font-display text-3xl font-bold">Video tours</h1>
          <p className="mt-1 text-[16px] text-gray-600 dark:text-muted">Short narrated tours of every demo. Click to play full screen.</p>
        </div>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search videos…"
          aria-label="Search videos"
          className="w-full sm:w-80 rounded-lg border border-gray-200 dark:border-edge bg-white dark:bg-carbon-900 px-3.5 py-2 text-[15px] outline-none focus:border-accent"
        />
      </div>
      <div className="grid gap-6 sm:grid-cols-2 xl:grid-cols-3">
        {list.map(({ d, v }) => (
          <div key={v.src}>
            <Poster video={v} onPlay={onPlay} />
            <div className="mt-2 font-display text-[17px] font-semibold">{v.title}</div>
            {d ? (
              <div className="text-[14px] text-gray-600 dark:text-muted">
                {d.win} {d.winLabel} ·{" "}
                <a href={demoHash(d.id)} className="text-accent hover:underline">
                  try it live →
                </a>
              </div>
            ) : (
              <div className="text-[14px] text-gray-600 dark:text-muted">Retail PODs, stock and reports, back to back</div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function DemoPage({ demo, onPlay }: { demo: Demo; onPlay: (v: Video) => void }) {
  return (
    <div className="space-y-6">
      <section className="rounded-2xl border border-gray-200 dark:border-edge bg-gray-50 dark:bg-carbon-900 p-5 md:p-6" data-tour="demo-head">
        <div className="flex flex-wrap items-start gap-4">
          <span className="text-4xl leading-none">{demo.icon}</span>
          <div className="min-w-0 flex-1">
            <div className="text-[13px] text-gray-500 dark:text-muted">
              <a href="#/" className="hover:text-accent">
                Home
              </a>{" "}
              / {GROUPS.find((g) => g.id === demo.group)?.label}
            </div>
            <h1 className="font-display text-3xl font-bold">{demo.name}</h1>
            <p className="mt-2 max-w-3xl text-[16px] text-gray-700 dark:text-muted">
              <b className="text-gray-900 dark:text-ink">The problem:</b> {demo.problem}
            </p>
            <p className="mt-1 max-w-3xl text-[16px] text-gray-700 dark:text-muted">
              <b className="text-gray-900 dark:text-ink">What Jordan does:</b> {demo.does}
            </p>
          </div>
          <div className="flex flex-col items-start gap-2">
            <div className="rounded-xl bg-ok/10 px-4 py-2">
              <div className="font-display text-2xl font-bold text-ok">{demo.win}</div>
              <div className="text-[13px] text-gray-700 dark:text-muted">{demo.winLabel}</div>
            </div>
            {demo.video && (
              <button type="button" onClick={() => onPlay(demo.video!)} className="rounded-lg bg-accent px-4 py-2 text-[15px] font-medium text-white hover:bg-accent/90">
                ▶ Watch the tour ({demo.video.length})
              </button>
            )}
          </div>
        </div>
      </section>
      {demo.id === "labs" ? <LabsTab lockPack="source" /> : PANELS[demo.id]()}
    </div>
  );
}
