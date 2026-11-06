# Prompt 01 — NebulaDB RAG for School & University Management ("Campus")

> Prepend `00-shared-benchmark-contract.md`. Demo slug: `campus`. Bucket prefix: `edu_`.

## Goal

Build **Campus**, a NebulaDB + RAG demo for running a multi-academy school trust **and** a
university from one database. It must show that one engine holds structured records (pupils,
timetables, grades, attendance), documents (policies, handbooks, course specs, lecture notes)
and vectors, and that RAG over that data answers real staff, student and parent questions with
citations and with **role-based access** enforced before retrieval.

## Fictional institutions (fixed demo calendar: today is 6 Nov 2026, autumn term / semester 1)

- **Northfield Learning Trust**: 4 schools (2 primary, 2 secondary incl. a sixth form), about
  3,200 pupils at demo scale. One school is "St Anne's C of E Primary" to exercise `''` escaping.
- **University of Wexley**: 3 faculties (Engineering, Health Sciences, Business), 24 programmes,
  about 400 modules and about 9,000 students at demo scale.

## Buckets

| Bucket | One doc is… | Filtered on |
|---|---|---|
| `edu_people` | a pupil, student, parent/guardian or staff member (no real names; generated) | `person_id`, `role`, `institution`, `year_group`, `programme`, `sen_flag`, `guardian_of[]` |
| `edu_policies` | a chunk of a policy or handbook (safeguarding, behaviour, attendance, exams, academic misconduct, extenuating circumstances, SEND, admissions, fees, accessibility) | `institution`, `policy`, `section`, `audience[]`, `effective_from`, `version` |
| `edu_curriculum` | a scheme-of-work unit (school) or a module spec / learning outcome (university) | `institution`, `subject`, `key_stage` or `level`, `module_code`, `credits`, `term` |
| `edu_materials` | a chunk of lesson notes, lecture transcript, slide text or reading-list item | `module_code`, `week`, `lecturer_id`, `kind`, `visibility` |
| `edu_timetable` | one timetabled event (lesson, lecture, lab, exam sitting) | `institution`, `room`, `staff_id`, `group_id`, `date`, `period`/`start`, `kind` |
| `edu_attendance` | one register mark | `person_id`, `date`, `session`, `mark`, `reason_code` |
| `edu_assessment` | a grade, coursework submission or exam result with feedback text | `person_id`, `module_code`/`subject`, `assessment_id`, `grade`, `submitted_at`, `late` |
| `edu_tickets` | a helpdesk / student-services / parent enquiry and its resolution | `institution`, `category`, `status`, `opened_at`, `channel` |
| `edu_drafts` | letters, reports and emails the apps draft (never sent automatically) | `kind`, `created_by`, `status = 'draft'` |

`bench` scale generator: 1M–10M docs, dominated by `edu_attendance` (about 60%) and
`edu_materials` (about 20%), matching how a real MIS/VLE grows.

## The apps (one showcase tab each, sidebar group "Campus")

### A. Policy & Handbook Assistant (staff, parents, students)
RAG over `edu_policies`, filtered by `institution` and by `audience` from the signed-in demo
persona. Parents never retrieve staff-only sections. Every answer cites policy, section and
version. "Effective from" handling: questions about last year get the old version.
Example questions: "Can my child take a term-time holiday?", "What counts as extenuating
circumstances for a late dissertation?", "Who is the DSL at Northfield High?"

### B. Student Study Copilot (university)
RAG over `edu_materials` + `edu_curriculum` limited to modules the student is enrolled on and
materials whose `visibility` allows it. It explains, quizzes and points to the lecture/week.
It **refuses to write assessed work** and cites the academic-misconduct policy chunk when asked.

### C. Teacher / Tutor Workbench
- "Draft end-of-term report comments for 9X2 Science" uses `edu_assessment` + `edu_attendance`
  (SQL aggregates) and the scheme of work. The output goes to `edu_drafts` for a teacher to edit.
- "Find lesson resources for photosynthesis, KS3, SEN-friendly" runs a hybrid search over
  `edu_materials` with `key_stage` and tags.

### D. Attendance & Early-Warning Dashboard
SQL over `edu_attendance` + `edu_assessment`: persistent absence (<90%), falling grades and late
submissions. A semantic search over pastoral notes and tickets finds similar past cases and
what helped. It produces a ranked "students to check in with" list with reasons. The model
never labels a student. It cites the numbers.

### E. Timetable & Rooms
Point and filtered reads: "Where is Dr Okafor at 2pm Thursday?", "Free labs with 30 seats in
week 7", "Exam clashes for students on two programmes". Mostly structured. It shows NebulaDB as
an MIS store and not only a vector DB.

### F. Admissions & Enquiries Desk
RAG over admissions/fees policies + past `edu_tickets`. It drafts replies to prospective
families and applicants with citations and suggests similar resolved tickets.

## Safety rules (pure TS module + tests)

- **Safeguarding trigger words** (abuse, self-harm, "unsafe at home", and so on) → a fixed card:
  "If a child is in immediate danger call 999. Otherwise contact the Designated Safeguarding
  Lead: <DSL from policy>." No model text, and the conversation is logged to a `safeguarding`
  draft for the DSL.
- No grades, attendance or SEN data for a person outside the persona's scope (parent → own
  children; tutor → own groups; student → self). Enforce this with filters in the SQL, not in
  the prompt, and add a test that a parent persona cannot retrieve another pupil's doc.
- Academic integrity: refuses "write my essay", and cites the policy.

## Benchmark workloads (`campus.*`)

| Workload | Kind | Mix | Example |
|---|---|---|---|
| `campus.timetable_lookup` | filtered_scan | 0.25 | staff_id + date range |
| `campus.attendance_aggregate` | filtered_scan | 0.15 | 30-day % per tutor group |
| `campus.policy_rag` | rag | 0.15 | audience-filtered policy Q&A |
| `campus.study_copilot_rag` | rag | 0.10 | module-scoped materials RAG |
| `campus.resource_hybrid` | hybrid | 0.15 | key_stage + semantic |
| `campus.register_write` | write | 0.15 | burst of 30 marks/class at 08:45 and 13:15 (model the burst) |
| `campus.report_draft` | mixed | 0.05 | aggregates + RAG + draft write |

Specific to this demo: model the **morning-registration write spike** (thousands of classes
marking within 10 minutes) while policy RAG reads continue, and report read p99 during the
spike against p99 at rest.

## Acceptance

All six apps work with the mock embedder. Role filtering has tests. The safeguarding card has
tests. `make bench-campus` writes results. `docs/campus-demo.md` exists.
