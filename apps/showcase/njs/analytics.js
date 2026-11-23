/*
 * Visitor analytics endpoint (njs): POST /a/event.
 *
 * The browser (src/analytics.ts) says WHAT happened. This adds WHO and
 * WHERE from sources the browser can't fake, then writes one document to
 * the NebulaDB bucket `showcase_events` through the internal /_a/write
 * location (which injects the API bearer):
 *
 *   user     the signed-in identity: $auth_email (Google / OpsAPI) or
 *            $remote_user (password login). /a/event sits behind the same
 *            sign-in gate as the site, so anonymous events never arrive.
 *   ip       X-WSL-Client-IP, stamped by the wslproxy edge hook (Traefik
 *            rewrites X-Forwarded-For, so that header only holds a pod IP),
 *            else X-Real-IP / X-Forwarded-For / the socket address.
 *   country  X-WSL-Geo-Country (edge IP2Location lookup), else the
 *            browser's guess from its timezone / language.
 *
 * Browser-supplied strings are whitelisted and length-capped.
 *
 * Pixel (px.nebuladb.net, public, no sign-in): other websites embed
 * /px.js, which reports the same event types through GET /px.gif (an
 * <img>, also used by <noscript>) or POST /px (sendBeacon, text/plain).
 * Each site has a public key in SHOWCASE_PIXEL_SITES, e.g.
 *   {"ws_…":{"host":"www.example.com","label":"Example","origins":["https://www.example.com"]}}
 * An unknown key or an Origin/Referer not on the site's list is dropped
 * silently (the GIF is still returned). The stored host is the site's
 * configured host, never the request's Host header. Events go to the
 * bucket `web_events` through /_px/write; the API key stays in nginx.
 */

const TYPES = {
  page_view: "viewed",
  video_play: "started video",
  video_progress: "watched",
  video_complete: "finished video",
  video_stop: "stopped video",
  click: "clicked",
  input: "typed into",
  session_end: "left after",
};

function str(v, n) {
  if (v === undefined || v === null) return "";
  return String(v).replace(/[\u0000-\u001f]+/g, " ").trim().slice(0, n);
}
function num(v) {
  const x = Number(v);
  return isFinite(x) ? Math.round(x * 10) / 10 : null;
}
function cc(v) {
  const c = str(v, 8).toUpperCase();
  return /^[A-Z]{2}$/.test(c) ? (c === "UK" ? "GB" : c) : "";
}
function clientIp(r) {
  const edge = str(r.headersIn["X-WSL-Client-IP"], 64);
  if (edge) return edge;
  const real = str(r.headersIn["X-Real-IP"], 64);
  if (real) return real;
  const xff = str(r.headersIn["X-Forwarded-For"], 256).split(",")[0].trim();
  return xff || r.remoteAddress;
}
function hex(n) {
  const b = crypto.getRandomValues(new Uint8Array(n));
  return Array.from(b, (x) => ("0" + x.toString(16)).slice(-2)).join("");
}

/** Exported for tests: build the stored document from a request's parts. */
/*
 * Country from the device's time zone (IANA name, e.g. "Asia/Calcutta" -> IN).
 * From zone.tab plus the legacy names browsers still report; zones shared by
 * several countries or not tied to one (UTC, Etc/*) give "".
 */
const TZ_TABLE = "AD:Europe/Andorra AE:Asia/Dubai AF:Asia/Kabul AG:America/Antigua AI:America/Anguilla AL:Europe/Tirane AM:Asia/Yerevan AO:Africa/Luanda AQ:Antarctica/Casey,Antarctica/Davis,Antarctica/DumontDUrville,Antarctica/Mawson,Antarctica/McMurdo,Antarctica/Palmer,Antarctica/Rothera,Antarctica/Syowa,Antarctica/Troll,Antarctica/Vostok AR:America/Argentina/Buenos_Aires,America/Argentina/Catamarca,America/Argentina/Cordoba,America/Argentina/Jujuy,America/Argentina/La_Rioja,America/Argentina/Mendoza,America/Argentina/Rio_Gallegos,America/Argentina/Salta,America/Argentina/San_Juan,America/Argentina/San_Luis,America/Argentina/Tucuman,America/Argentina/Ushuaia,America/Buenos_Aires,America/Catamarca,America/Cordoba,America/Jujuy,America/Mendoza AS:Pacific/Pago_Pago,Pacific/Samoa,US/Samoa AT:Europe/Vienna AU:Antarctica/Macquarie,Australia/ACT,Australia/Adelaide,Australia/Brisbane,Australia/Broken_Hill,Australia/Canberra,Australia/Darwin,Australia/Eucla,Australia/Hobart,Australia/LHI,Australia/Lindeman,Australia/Lord_Howe,Australia/Melbourne,Australia/NSW,Australia/North,Australia/Perth,Australia/Queensland,Australia/South,Australia/Sydney,Australia/Tasmania,Australia/Victoria,Australia/West,Australia/Yancowinna AW:America/Aruba AX:Europe/Mariehamn AZ:Asia/Baku BA:Europe/Sarajevo BB:America/Barbados BD:Asia/Dacca,Asia/Dhaka BE:Europe/Brussels BF:Africa/Ouagadougou BG:Europe/Sofia BH:Asia/Bahrain BI:Africa/Bujumbura BJ:Africa/Porto-Novo BL:America/St_Barthelemy BM:Atlantic/Bermuda BN:Asia/Brunei BO:America/La_Paz BQ:America/Kralendijk BR:America/Araguaina,America/Bahia,America/Belem,America/Boa_Vista,America/Campo_Grande,America/Cuiaba,America/Eirunepe,America/Fortaleza,America/Maceio,America/Manaus,America/Noronha,America/Porto_Acre,America/Porto_Velho,America/Recife,America/Rio_Branco,America/Santarem,America/Sao_Paulo,Brazil/Acre,Brazil/DeNoronha,Brazil/East,Brazil/West BS:America/Nassau BT:Asia/Thimbu,Asia/Thimphu BW:Africa/Gaborone BY:Europe/Minsk BZ:America/Belize CA:America/Atikokan,America/Blanc-Sablon,America/Cambridge_Bay,America/Creston,America/Dawson,America/Dawson_Creek,America/Edmonton,America/Fort_Nelson,America/Glace_Bay,America/Goose_Bay,America/Halifax,America/Inuvik,America/Iqaluit,America/Moncton,America/Montreal,America/Rankin_Inlet,America/Regina,America/Resolute,America/St_Johns,America/Swift_Current,America/Toronto,America/Vancouver,America/Whitehorse,America/Winnipeg,Canada/Atlantic,Canada/Central,Canada/Eastern,Canada/Mountain,Canada/Newfoundland,Canada/Pacific,Canada/Saskatchewan,Canada/Yukon CC:Indian/Cocos CD:Africa/Kinshasa,Africa/Lubumbashi CF:Africa/Bangui CG:Africa/Brazzaville CH:Europe/Zurich CI:Africa/Abidjan CK:Pacific/Rarotonga CL:America/Coyhaique,America/Punta_Arenas,America/Santiago,Chile/Continental,Chile/EasterIsland,Pacific/Easter CM:Africa/Douala CN:Asia/Chongqing,Asia/Chungking,Asia/Harbin,Asia/Shanghai,Asia/Urumqi,PRC CO:America/Bogota CR:America/Costa_Rica CU:America/Havana,Cuba CV:Atlantic/Cape_Verde CW:America/Curacao CX:Indian/Christmas CY:Asia/Famagusta,Asia/Nicosia,Europe/Nicosia CZ:Europe/Prague DE:Europe/Berlin,Europe/Busingen DJ:Africa/Djibouti DK:Europe/Copenhagen DM:America/Dominica DO:America/Santo_Domingo DZ:Africa/Algiers EC:America/Guayaquil,Pacific/Galapagos EE:Europe/Tallinn EG:Africa/Cairo,Egypt EH:Africa/El_Aaiun ER:Africa/Asmara,Africa/Asmera ES:Africa/Ceuta,Atlantic/Canary,Europe/Madrid ET:Africa/Addis_Ababa FI:Europe/Helsinki FJ:Pacific/Fiji FK:Atlantic/Stanley FM:Pacific/Chuuk,Pacific/Kosrae,Pacific/Pohnpei,Pacific/Ponape,Pacific/Truk,Pacific/Yap FO:Atlantic/Faeroe,Atlantic/Faroe FR:Europe/Paris GA:Africa/Libreville GB:Europe/Belfast,Europe/London,GB,GB-Eire GD:America/Grenada GE:Asia/Tbilisi GF:America/Cayenne GG:Europe/Guernsey GH:Africa/Accra GI:Europe/Gibraltar GL:America/Danmarkshavn,America/Godthab,America/Nuuk,America/Scoresbysund,America/Thule GM:Africa/Banjul GN:Africa/Conakry GP:America/Guadeloupe GQ:Africa/Malabo GR:Europe/Athens GS:Atlantic/South_Georgia GT:America/Guatemala GU:Pacific/Guam GW:Africa/Bissau GY:America/Guyana HK:Asia/Hong_Kong,Hongkong HN:America/Tegucigalpa HR:Europe/Zagreb HT:America/Port-au-Prince HU:Europe/Budapest ID:Asia/Jakarta,Asia/Jayapura,Asia/Makassar,Asia/Pontianak,Asia/Ujung_Pandang IE:Eire,Europe/Dublin IL:Asia/Jerusalem,Asia/Tel_Aviv,Israel IM:Europe/Isle_of_Man IN:Asia/Calcutta,Asia/Kolkata IO:Indian/Chagos IQ:Asia/Baghdad IR:Asia/Tehran,Iran IS:Atlantic/Reykjavik,Iceland IT:Europe/Rome JE:Europe/Jersey JM:America/Jamaica,Jamaica JO:Asia/Amman JP:Asia/Tokyo,Japan KE:Africa/Nairobi KG:Asia/Bishkek KH:Asia/Phnom_Penh KI:Pacific/Enderbury,Pacific/Kanton,Pacific/Kiritimati,Pacific/Tarawa KM:Indian/Comoro KN:America/St_Kitts KP:Asia/Pyongyang KR:Asia/Seoul,ROK KW:Asia/Kuwait KY:America/Cayman KZ:Asia/Almaty,Asia/Aqtau,Asia/Aqtobe,Asia/Atyrau,Asia/Oral,Asia/Qostanay,Asia/Qyzylorda LA:Asia/Vientiane LB:Asia/Beirut LC:America/St_Lucia LI:Europe/Vaduz LK:Asia/Colombo LR:Africa/Monrovia LS:Africa/Maseru LT:Europe/Vilnius LU:Europe/Luxembourg LV:Europe/Riga LY:Africa/Tripoli,Libya MA:Africa/Casablanca MC:Europe/Monaco MD:Europe/Chisinau,Europe/Tiraspol ME:Europe/Podgorica MF:America/Marigot MG:Indian/Antananarivo MH:Kwajalein,Pacific/Kwajalein,Pacific/Majuro MK:Europe/Skopje ML:Africa/Bamako,Africa/Timbuktu MM:Asia/Rangoon,Asia/Yangon MN:Asia/Hovd,Asia/Ulaanbaatar,Asia/Ulan_Bator MO:Asia/Macao,Asia/Macau MP:Pacific/Saipan MQ:America/Martinique MR:Africa/Nouakchott MS:America/Montserrat MT:Europe/Malta MU:Indian/Mauritius MV:Indian/Maldives MW:Africa/Blantyre MX:America/Bahia_Banderas,America/Cancun,America/Chihuahua,America/Ciudad_Juarez,America/Ensenada,America/Hermosillo,America/Matamoros,America/Mazatlan,America/Merida,America/Mexico_City,America/Monterrey,America/Ojinaga,America/Tijuana,Mexico/BajaNorte,Mexico/BajaSur,Mexico/General MY:Asia/Kuala_Lumpur,Asia/Kuching MZ:Africa/Maputo NA:Africa/Windhoek NC:Pacific/Noumea NE:Africa/Niamey NF:Pacific/Norfolk NG:Africa/Lagos NI:America/Managua NL:Europe/Amsterdam NO:Europe/Oslo NP:Asia/Kathmandu,Asia/Katmandu NR:Pacific/Nauru NU:Pacific/Niue NZ:NZ,NZ-CHAT,Pacific/Auckland,Pacific/Chatham OM:Asia/Muscat PA:America/Panama PE:America/Lima PF:Pacific/Gambier,Pacific/Marquesas,Pacific/Tahiti PG:Pacific/Bougainville,Pacific/Port_Moresby PH:Asia/Manila PK:Asia/Karachi PL:Europe/Warsaw,Poland PM:America/Miquelon PN:Pacific/Pitcairn PR:America/Puerto_Rico PS:Asia/Gaza,Asia/Hebron PT:Atlantic/Azores,Atlantic/Madeira,Europe/Lisbon,Portugal PW:Pacific/Palau PY:America/Asuncion QA:Asia/Qatar RE:Indian/Reunion RO:Europe/Bucharest RS:Europe/Belgrade RU:Asia/Anadyr,Asia/Barnaul,Asia/Chita,Asia/Irkutsk,Asia/Kamchatka,Asia/Khandyga,Asia/Krasnoyarsk,Asia/Magadan,Asia/Novokuznetsk,Asia/Novosibirsk,Asia/Omsk,Asia/Sakhalin,Asia/Srednekolymsk,Asia/Tomsk,Asia/Ust-Nera,Asia/Vladivostok,Asia/Yakutsk,Asia/Yekaterinburg,Europe/Astrakhan,Europe/Kaliningrad,Europe/Kirov,Europe/Moscow,Europe/Samara,Europe/Saratov,Europe/Ulyanovsk,Europe/Volgograd RW:Africa/Kigali SA:Asia/Riyadh SB:Pacific/Guadalcanal SC:Indian/Mahe SD:Africa/Khartoum SE:Europe/Stockholm SG:Asia/Singapore,Singapore SH:Atlantic/St_Helena SI:Europe/Ljubljana SJ:Arctic/Longyearbyen,Atlantic/Jan_Mayen SK:Europe/Bratislava SL:Africa/Freetown SM:Europe/San_Marino SN:Africa/Dakar SO:Africa/Mogadishu SR:America/Paramaribo SS:Africa/Juba ST:Africa/Sao_Tome SV:America/El_Salvador SX:America/Lower_Princes SY:Asia/Damascus SZ:Africa/Mbabane TC:America/Grand_Turk TD:Africa/Ndjamena TF:Indian/Kerguelen TG:Africa/Lome TH:Asia/Bangkok TJ:Asia/Dushanbe TK:Pacific/Fakaofo TL:Asia/Dili TM:Asia/Ashgabat,Asia/Ashkhabad TN:Africa/Tunis TO:Pacific/Tongatapu TR:Asia/Istanbul,Europe/Istanbul,Turkey TT:America/Port_of_Spain TV:Pacific/Funafuti TW:Asia/Taipei,ROC TZ:Africa/Dar_es_Salaam UA:Europe/Kiev,Europe/Kyiv,Europe/Simferopol,Europe/Uzhgorod,Europe/Zaporozhye UG:Africa/Kampala UM:Pacific/Midway,Pacific/Wake US:America/Adak,America/Anchorage,America/Boise,America/Chicago,America/Denver,America/Detroit,America/Fort_Wayne,America/Indiana/Indianapolis,America/Indiana/Knox,America/Indiana/Marengo,America/Indiana/Petersburg,America/Indiana/Tell_City,America/Indiana/Vevay,America/Indiana/Vincennes,America/Indiana/Winamac,America/Indianapolis,America/Juneau,America/Kentucky/Louisville,America/Kentucky/Monticello,America/Knox_IN,America/Los_Angeles,America/Louisville,America/Menominee,America/Metlakatla,America/New_York,America/Nome,America/North_Dakota/Beulah,America/North_Dakota/Center,America/North_Dakota/New_Salem,America/Phoenix,America/Shiprock,America/Sitka,America/Yakutat,Navajo,Pacific/Honolulu,US/Alaska,US/Aleutian,US/Arizona,US/Central,US/East-Indiana,US/Eastern,US/Hawaii,US/Indiana-Starke,US/Michigan,US/Mountain,US/Pacific UY:America/Montevideo UZ:Asia/Samarkand,Asia/Tashkent VA:Europe/Vatican VC:America/St_Vincent VE:America/Caracas VG:America/Tortola VI:America/St_Thomas,America/Virgin VN:Asia/Ho_Chi_Minh,Asia/Saigon VU:Pacific/Efate WF:Pacific/Wallis WS:Pacific/Apia YE:Asia/Aden YT:Indian/Mayotte ZA:Africa/Johannesburg ZM:Africa/Lusaka ZW:Africa/Harare";
let tzIndex = null;
function tzCountry(tz) {
  if (!tzIndex) {
    tzIndex = {};
    TZ_TABLE.split(" ").forEach((group) => {
      const i = group.indexOf(":");
      group.slice(i + 1).split(",").forEach((z) => {
        tzIndex[z] = group.slice(0, i);
      });
    });
  }
  const z = str(tz, 64);
  return Object.prototype.hasOwnProperty.call(tzIndex, z) ? tzIndex[z] : "";
}

/**
 * Exported for tests: where the visitor is. The edge's IP lookup is used
 * unless the device's time zone names a different country: IP databases
 * often place leased or carrier ranges in the registrant's country (an
 * Indian visitor on a Cogent 38.x address shows as US), and VPNs move the
 * IP, while the time zone follows the person. The IP's answer is kept in
 * country_ip either way.
 */
function countryOf(ipCountry, tzCc, langCc) {
  if (ipCountry && tzCc && tzCc !== ipCountry) return { country: tzCc, source: "timezone" };
  if (ipCountry) return { country: ipCountry, source: "ip" };
  if (tzCc) return { country: tzCc, source: "timezone" };
  if (langCc) return { country: langCc, source: "browser" };
  return { country: "", source: "" };
}

function buildDoc(e, ctx) {
  // ctx.site (pixel: the configured label) wins over the browser's claim.
  const type = str(e.type, 32);
  if (!TYPES[type]) return null;
  const edgeCountry = cc(ctx.edgeCountry);
  const guess = cc(e.country_guess);
  const where = countryOf(edgeCountry, tzCountry(e.tz), guess);
  const now = ctx.now;
  const m = {
    type,
    site: str(ctx.site || e.site, 32),
    source: str(ctx.source, 16),
    host: str(ctx.host, 128),
    user: str(ctx.user, 200) || "anonymous",
    ip: str(ctx.ip, 64),
    country: where.country,
    country_source: where.source,
    country_ip: edgeCountry,
    country_browser: guess,
    lang: str(e.lang, 35),
    langs: str(e.langs, 200),
    tz: str(e.tz, 64),
    screen: str(e.screen, 16),
    viewport: str(e.viewport, 16),
    ua: str(ctx.ua, 300),
    path: str(e.path, 200),
    title: str(e.title, 160),
    demo: str(e.demo, 64),
    video: str(e.video, 160),
    pct: num(e.pct),
    seconds: num(e.seconds),
    label: str(e.label, 80),
    value: str(e.value, 300),
    referrer: str(e.referrer, 300),
    session: str(e.session, 32),
    visitor: str(e.visitor, 32),
    ts: Math.floor(now / 1000),
    day: new Date(now).toISOString().slice(0, 10),
  };
  Object.keys(m).forEach((k) => {
    if (m[k] === "" || m[k] === null) delete m[k];
  });
  const what =
    type === "page_view"
      ? m.title || m.path
      : type.indexOf("video") === 0
        ? `${m.video || "a video"}${m.pct != null ? ` (${m.pct}%)` : ""}`
        : type === "session_end"
          ? `${m.seconds || 0}s`
          : `${m.label || ""}${m.value ? `: ${m.value}` : ""}`;
  const text = `${m.user} ${TYPES[type]} ${what} on ${m.host || m.site || "the site"}${m.country ? ` from ${m.country}` : ""}`;
  return { id: `${m.ts}-${ctx.rand}`, text: text.slice(0, 600), metadata: m };
}

/* ------------------------------ Slack alerts ------------------------------ */

function envOf(name) {
  const v = process.env[name];
  return v === undefined || v === null ? "" : String(v).trim();
}
function slackEsc(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function flagOf(c) {
  return /^[A-Z]{2}$/.test(c) ? String.fromCodePoint(0x1f1a5 + c.charCodeAt(0), 0x1f1a5 + c.charCodeAt(1)) : "";
}
function deviceOf(ua) {
  ua = ua || "";
  const os = /iPhone|iPad/.test(ua) ? "iOS" : /Android/.test(ua) ? "Android" : /Mac OS X/.test(ua) ? "Mac" : /Windows/.test(ua) ? "Windows" : /Linux/.test(ua) ? "Linux" : "";
  const br = /Edg\//.test(ua) ? "Edge" : /Chrome\//.test(ua) ? "Chrome" : /Firefox\//.test(ua) ? "Firefox" : /Safari\//.test(ua) ? "Safari" : "";
  return [br, os].filter(Boolean).join(" on ");
}
function mmss(sec) {
  const t = Math.round(sec || 0);
  return `${Math.floor(t / 60)}:${("0" + (t % 60)).slice(-2)}`;
}
/** Exported for tests: is this user on the never-alert list? */
function ignoredUser(user, list) {
  const u = String(user || "").toLowerCase();
  return String(list || "")
    .toLowerCase()
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean)
    .some((x) => (x[0] === "@" ? u.endsWith(x) : u === x));
}

/**
 * Exported for tests: the Slack message for this event, or "" for none.
 * Alerts only for customer sites: a visit (first page of a browser
 * session) and a finished video.
 */
function sourceNote(m) {
  if (m.country_source === "browser") return " (browser settings)";
  if (m.country_source === "timezone") return m.country_ip ? ` (time zone; IP says ${m.country_ip})` : " (time zone)";
  return "";
}

function alertText(doc, first, link, pixelAlerts, videoStarts) {
  const m = doc.metadata;
  // Pixel sites (opt-in, showcase.pixel.slackAlerts): a visit only once the
  // visitor is identified, and a finished video. Anonymous traffic is silent.
  if (m.source === "pixel") {
    if (!pixelAlerts) return "";
    if (m.type === "page_view" && m.user === "anonymous") return "";
  } else if (m.site !== "sourcesupplies") return "";
  const who = `*${slackEsc(m.user)}*`;
  const more = link ? `  <${link}|Engagement>` : "";
  if (m.type === "page_view" && first) {
    const from = [
      m.country ? `${flagOf(m.country)} ${m.country}${sourceNote(m)}` : "",
      m.ip ? `IP ${m.ip}` : "",
      deviceOf(m.ua),
      m.lang || "",
    ]
      .filter(Boolean)
      .map(slackEsc)
      .join(" · ");
    return `:wave: ${who} opened ${slackEsc(m.host)}${from ? `\n${from}` : ""}${more}`;
  }
  // Pixel visitors are mostly anonymous, so say where they are and which site.
  const pixel = m.source === "pixel";
  const whoAt = pixel && m.country ? `${who} (${flagOf(m.country)} ${slackEsc(m.country)})` : who;
  const onSite = pixel ? ` on ${slackEsc(m.host)}` : "";
  const title = `*${slackEsc(m.video || "a video")}*`;
  if (m.type === "video_complete") {
    return `:clapper: ${whoAt} finished ${title} (${mmss(m.seconds)} watched)${onSite}${more}`;
  }
  if (m.type === "video_play" && pixel && videoStarts) {
    return `:arrow_forward: ${whoAt} started ${title}${onSite}${more}`;
  }
  return "";
}

async function slack(r, text) {
  const hook = envOf("SHOWCASE_SLACK_WEBHOOK");
  if (!hook || !text) return;
  try {
    const res = await ngx.fetch(hook, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ text }) });
    if (!res.ok) r.warn(`analytics: slack ${res.status}`);
  } catch (err) {
    r.warn(`analytics: slack failed: ${err}`);
  }
}

async function event(r) {
  if (r.method !== "POST") {
    r.return(405);
    return;
  }
  let e;
  try {
    e = JSON.parse(r.requestText || "");
  } catch (err) {
    r.return(400);
    return;
  }
  const doc = buildDoc(e || {}, {
    host: (r.headersIn.Host || "").toLowerCase().replace(/:\d+$/, ""),
    user: r.variables.auth_email || r.variables.remote_user || "",
    ip: clientIp(r),
    edgeCountry: r.headersIn["X-WSL-Geo-Country"],
    ua: r.headersIn["User-Agent"],
    now: Date.now(),
    rand: hex(6),
  });
  if (!doc) {
    r.return(400);
    return;
  }
  const write = r
    .subrequest("/_a/write", { method: "POST", body: JSON.stringify(doc) })
    .then((res) => {
      if (res.status >= 300) r.warn(`analytics: write ${res.status}`);
    })
    .catch((err) => r.warn(`analytics: write failed: ${err}`));
  const text = ignoredUser(doc.metadata.user, envOf("SHOWCASE_ALERT_IGNORE")) ? "" : alertText(doc, e.first === true, envOf("SHOWCASE_ENGAGEMENT_URL"));
  await Promise.all([write, slack(r, text)]);
  r.return(204);
}

/* --------------------------------- pixel ---------------------------------- */

const GIF = "R0lGODlhAQABAIAAAP///wAAACH5BAEAAAAALAAAAAABAAEAAAICRAEAOw=="; // 1x1 transparent, 43 bytes
const MAX_QUERY_PAYLOAD = 4096;

/** Exported for tests: the configured sites, keyed by their public key. */
function pixelSites(json) {
  let raw;
  try {
    raw = JSON.parse(json || "{}");
  } catch (err) {
    return {};
  }
  const out = {};
  Object.keys(raw || {}).forEach((k) => {
    const v = raw[k] || {};
    const host = str(v.host, 128).toLowerCase();
    if (!/^[A-Za-z0-9_-]{4,64}$/.test(k) || !host) return;
    const origins = (Array.isArray(v.origins) ? v.origins : []).map((o) => str(o, 200).toLowerCase().replace(/\/+$/, "")).filter(Boolean);
    out[k] = {
      host,
      label: str(v.label, 32) || host,
      origins: origins.length ? origins : [`https://${host}`],
      // Slack also when a video starts (not only when it is finished).
      videoStarts: v.videoStarts === true,
    };
  });
  return out;
}

function originOf(url) {
  const m = /^(https?:\/\/[^/?#]+)/i.exec(String(url || ""));
  return m ? m[1].toLowerCase() : "";
}

/**
 * Exported for tests: the site for this key, or null when the key is
 * unknown or the request doesn't come from one of its pages. Origin is
 * checked when the browser sends it (beacon, fetch); otherwise the
 * Referer's origin (<img>, <noscript>). Neither: rejected.
 */
function pixelSite(sites, key, origin, referer) {
  const site = Object.prototype.hasOwnProperty.call(sites, key) ? sites[key] : null;
  if (!site) return null;
  const from = originOf(origin) || originOf(referer);
  return from && site.origins.indexOf(from) !== -1 ? site : null;
}

/** Exported for tests: decode the GIF's `d` (base64url JSON, <= 4 KB); {} when absent or bad. */
function decodePayload(d) {
  if (!d) return {};
  if (d.length > MAX_QUERY_PAYLOAD) return null;
  try {
    const b64 = d.replace(/-/g, "+").replace(/_/g, "/");
    const json = Buffer.from(b64 + "===".slice((b64.length + 3) % 4), "base64").toString("utf8");
    const v = JSON.parse(json);
    return v && typeof v === "object" && !Array.isArray(v) ? v : null;
  } catch (err) {
    return null;
  }
}

/** Exported for tests: the stored document for a pixel event (host and label from config). */
function pixelDoc(e, site, ctx) {
  const ev = {};
  Object.keys(e || {}).forEach((k) => {
    if (k !== "user" && k !== "site" && k !== "host") ev[k] = e[k];
  });
  return buildDoc(ev, {
    site: site.label,
    source: "pixel",
    host: site.host,
    user: str(e && e.uid, 200) || "anonymous",
    ip: ctx.ip,
    edgeCountry: ctx.edgeCountry,
    ua: ctx.ua,
    now: ctx.now,
    rand: ctx.rand,
  });
}

function pixelCors(r, site) {
  const origin = originOf(r.headersIn.Origin);
  if (site && origin) {
    r.headersOut["Access-Control-Allow-Origin"] = origin;
    r.headersOut["Access-Control-Allow-Credentials"] = "true";
  }
  r.headersOut.Vary = "Origin";
}

async function pixelRecord(r, e) {
  const sites = pixelSites(envOf("SHOWCASE_PIXEL_SITES"));
  const site = e ? pixelSite(sites, str(e.k, 64), r.headersIn.Origin, r.headersIn.Referer) : null;
  pixelCors(r, site);
  if (!site) return;
  const doc = pixelDoc(e, site, {
    ip: clientIp(r),
    edgeCountry: r.headersIn["X-WSL-Geo-Country"],
    ua: r.headersIn["User-Agent"],
    now: Date.now(),
    rand: hex(6),
  });
  if (!doc) return;
  const write = r
    .subrequest("/_px/write", { method: "POST", body: JSON.stringify(doc) })
    .then((res) => {
      if (res.status >= 300) r.warn(`pixel: write ${res.status}`);
    })
    .catch((err) => r.warn(`pixel: write failed: ${err}`));
  const on = /^(1|true|yes|on)$/i.test(envOf("SHOWCASE_PIXEL_ALERTS"));
  const text = !on || ignoredUser(doc.metadata.user, envOf("SHOWCASE_ALERT_IGNORE")) ? "" : alertText(doc, e.first === true || e.first === 1 || e.first === "1", envOf("SHOWCASE_ENGAGEMENT_URL"), on, site.videoStarts);
  await Promise.all([write, slack(r, text)]);
}

/** Exported for tests: the event from a GIF query (k, e, d), or null to drop it. */
function gifEvent(args, referer) {
  const payload = decodePayload(args.d || "");
  if (!payload) return null;
  const e = Object.assign({}, payload, { k: args.k, type: args.e || payload.type || "page_view" });
  // <noscript>: no payload, so the page comes from the Referer.
  if (!e.path) {
    const m = /^https?:\/\/[^/?#]+([^#]*)/i.exec(String(referer || ""));
    if (m) e.path = m[1] || "/";
  }
  return e;
}

/** GET /px.gif?k=&e=&d= : always the GIF, 200, no-store. */
async function pxGif(r) {
  try {
    await pixelRecord(r, gifEvent(r.args || {}, r.headersIn.Referer));
  } catch (err) {
    r.warn(`pixel: ${err}`);
  }
  r.headersOut["Content-Type"] = "image/gif";
  r.headersOut["Cache-Control"] = "no-store, max-age=0";
  r.return(200, Buffer.from(GIF, "base64"));
}

/** POST /px (text/plain JSON from sendBeacon) -> 204; OPTIONS -> preflight. */
async function pxPost(r) {
  r.headersOut["Cache-Control"] = "no-store";
  if (r.method === "OPTIONS") {
    const sites = pixelSites(envOf("SHOWCASE_PIXEL_SITES"));
    const origin = originOf(r.headersIn.Origin);
    const ok = Object.keys(sites).some((k) => sites[k].origins.indexOf(origin) !== -1);
    pixelCors(r, ok ? {} : null);
    r.headersOut["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS";
    r.headersOut["Access-Control-Allow-Headers"] = "Content-Type";
    r.headersOut["Access-Control-Max-Age"] = "86400";
    r.return(204);
    return;
  }
  if (r.method !== "POST") {
    r.return(405);
    return;
  }
  let e = null;
  try {
    e = JSON.parse(r.requestText || "");
    if (!e || typeof e !== "object" || Array.isArray(e)) e = null;
  } catch (err) {
    e = null;
  }
  try {
    await pixelRecord(r, e);
  } catch (err) {
    r.warn(`pixel: ${err}`);
  }
  r.return(204);
}

export default { event, buildDoc, tzCountry, countryOf, alertText, ignoredUser, pxGif, pxPost, pixelSites, pixelSite, decodePayload, pixelDoc, gifEvent };
