# Prompt 06 — Kisaan: Multilingual Farmer Advisory on NebulaDB

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `kisaan`. Bucket prefix: `kis_`.

## Goal

Build **Kisaan**, an advisory assistant for smallholder farmers in India. A farmer asks in
Hindi, Punjabi or English (typed, or voice-transcribed), with spelling as people really write
it, including Hinglish ("gehu me peela rog aa gaya"). Kisaan answers from an agronomy knowledge
base, local mandi (market) prices, weather and the farmer's own plot history, all stored in
NebulaDB. This demo stresses **multilingual and code-mixed retrieval**, **geo + time filters**
and a low-bandwidth UX.

All data is invented. Advisory text is written fresh for the demo and carries "Demo advice:
confirm with your local KVK / agriculture officer". Prices and weather are synthetic.

## Fictional setting (today is 6 Nov 2026, rabi sowing season)

Three districts in Punjab and two in Uttar Pradesh, 40 villages, 5,000 farmer profiles at demo
scale, 20 crops (wheat, paddy, mustard, sugarcane, potato, cotton...), 60 mandis.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `kis_advisory` | chunk of a crop guide: sowing, seed rate, irrigation, nutrients, pest/disease ID and management | `crop`, `stage`, `topic`, `state`, `season`, `lang` (hi/pa/en), `pest` |
| `kis_pest_cases` | a past farmer query + symptoms + confirmed diagnosis + outcome (synthetic) | `crop`, `district`, `symptoms[]`, `diagnosis`, `month` |
| `kis_mandi_prices` | daily price for a crop at a mandi | `mandi`, `district`, `state`, `crop`, `variety`, `date`, `modal_price_inr` |
| `kis_weather` | daily forecast/observation per village (synthetic) | `village`, `district`, `date`, `rain_mm`, `tmax`, `tmin`, `alert` |
| `kis_farmers` | farmer profile (no real names/phones): village, land, crops, language, plot history | `farmer_id`, `village`, `district`, `lang`, `crops[]` |
| `kis_schemes` | plain-language summaries of **fictional or generalised** government scheme rules written for the demo (eligibility, documents needed) | `state`, `scheme`, `category` |
| `kis_queries` | each farmer question + answer + feedback (thumbs up/down) | `farmer_id`, `lang`, `crop`, `at`, `helpful` |

`bench` scale: mandi prices + weather make up most of it (daily × mandis/villages × years).
Queries arrive in a morning and evening peak.

## The apps (sidebar group "Kisaan")

### A. Ask Kisaan (chat, multilingual)
The query goes through language detection and transliteration normalisation (keep the original
too), then hybrid retrieval over `kis_advisory` filtered by the farmer's `crop`, `state` and
`season`, and an answer **in the farmer's language** with cited guide sections. The answer is
short, step-by-step and readable on a basic phone. It shows the retrieved chunks' languages
(cross-lingual retrieval: a Hindi question can hit an English chunk).

### B. Crop Doctor
The farmer describes symptoms (photo upload optional: caption via a local vision model if
available, otherwise text only). The app finds similar `kis_pest_cases` and advisory chunks for
that crop and stage. It gives the top likely issues with confidence wording ("it may be..."),
management steps (cultural → biological → chemical, in that order), and **never** gives a
pesticide dose not present in the cited chunk.

### C. Mandi Bhav (market prices)
"Sarson ka bhav Khanna mandi me" parses to crop + mandi, then a SQL query for the last 14 days,
nearby mandis within the district, and a trend. It is structured data only, with no model
numbers. The model may explain the table but must quote it.

### D. Weather-Aware Advisory
It combines `kis_weather` alerts with the crop stage: "Rain expected Thursday, delay
irrigation / pesticide spray". It shows which rule fired and why.

### E. Scheme Helper
"Am I eligible for ...?" runs RAG over `kis_schemes` for the state, gives a checklist of
documents, and says "confirm at your CSC / agriculture office".

### F. Extension Officer Dashboard
SQL + semantic clustering over `kis_queries`: what farmers in each district asked about this
week (for example, a spike in "yellow leaves wheat" in one district is an early pest-outbreak
signal), answers marked unhelpful, and unanswered questions (content gaps).

## Safety rules (pure TS module + tests)

- **Poisoning / exposure words** (pesticide swallowed, chakkar after spray, fainting) → a
  fixed card in the farmer's language: "Call 108 / go to the nearest hospital now. Take the
  pesticide container with you." No model text.
- Pesticide doses only verbatim from a cited chunk. Otherwise: "Ask your local agriculture
  officer." Add tests.
- Prices and weather are quoted from data, never generated. Add a test that every number in a
  price answer appears in the retrieved rows.
- Distress or self-harm language → a fixed helpline card (demo text marked for verification)
  and no advisory content.

## Benchmark workloads (`kisaan.*`)

| Workload | Kind | Mix | Notes |
|---|---|---|---|
| `kisaan.advice_rag` | rag | 0.25 | crop + state + season filtered |
| `kisaan.crosslingual_search` | hybrid | 0.15 | hi/pa/Hinglish query → mixed-language corpus |
| `kisaan.crop_doctor` | vector | 0.10 | symptoms → cases |
| `kisaan.mandi_lookup` | filtered_scan | 0.25 | crop + mandi + date range |
| `kisaan.weather_lookup` | point_read | 0.10 | village + date |
| `kisaan.price_ingest` | write | 0.10 | daily batch of all mandis |
| `kisaan.query_log_write` | write | 0.05 | |

Specific to this demo: report **recall@10 split by query language and script** (English,
Devanagari Hindi, Romanised Hinglish, Gurmukhi Punjabi) against a labelled set, for at least
two embedders (for example `nomic-embed-text` against a multilingual model such as `bge-m3`
via Ollama). Also report response payload size, since farmers are on 2G/3G.

## Acceptance

A Hinglish question gets a Hindi answer with citations. Mandi numbers match the rows (tested).
The poisoning card is tested. `make bench-kisaan` writes results with per-language recall.
`docs/kisaan-demo.md` exists.
