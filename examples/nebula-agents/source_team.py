"""The Source Supplies agent team: Claude over NebulaDB MCP, with limits in code.

Same shape as the ops team in nebula_agents.py: each agent sees only its
own MCP tools, and a supervisor works only through `delegate`. On top of
that, every MCP call goes through `source_guards.GuardedSession`, which
rejects calls outside the agent's buckets before they reach nebula-mcp.
No agent can write anywhere except `ss_order_drafts` (status = draft)
and `agent_memory` (the session's own user).

All the data the agents read is demo data (see docs/source-supplies-demo.md).
"""

from __future__ import annotations

from dataclasses import dataclass

from anthropic import beta_async_tool
from anthropic.lib.tools.mcp import async_mcp_tool
from mcp import types as mcp_types

from nebula_agents import Agent, Team
from source_guards import POLICIES, GuardedSession

DEMO_TODAY = "2026-10-31"

CONTEXT = (
    "You work for Source Supplies Ltd, a UK B2B supplier of sustainable commercial cleaning and hygiene "
    "products, cleaning machines and Source Washroom services, based in Camberley, Surrey. Customers are "
    "contract cleaners, FM firms, schools, universities, care homes, offices, industrial sites and leisure "
    "venues. Everything in the ss_* buckets is DEMO DATA: say so when you present figures. The demo "
    f"calendar's today is {DEMO_TODAY}. Prices are GBP excluding VAT. Use UK English and plain words. "
    "You never place an order, send an email or book a visit: you draft, and a person confirms."
)

BUCKETS = (
    "Buckets (search the `text` column; filter on metadata):\n"
    "- ss_products: sku, name, category (chemicals, paper, washroom, waste, machines, PPE, equipment), "
    "use_areas[], sectors[], badges[] (e.g. 'nordic-swan', 'eu-ecolabel', 'vegan'), concentrate, dilution, "
    "pack, price_gbp, in_stock, stock_qty, lead_days, co2e_kg_per_unit, replaces_sku (the less-green product "
    "it replaces), uses_per_pack, use_unit\n"
    "- ss_sds: sku, section (1-16), section_title, hazard_class, ppe[], signal_word (demo SDS for 10 products)\n"
    "- ss_guides: guide_id, title, problem, surface, sector, skus[]\n"
    "- ss_accounts: account_id (BRIGHTWAY, KINGSMEAD, HARBOUR, MERIDIAN, CASTLEGATE, ASHBY), account_name, "
    "site, sector, delivery_day, washroom_services[], washroom_cycle_days\n"
    "- ss_orders: account_id, site, sku, qty (packs), month (YYYY-MM, 2026-05..2026-10), po_ref\n"
    "- ss_washroom_jobs: kind ('visit' or 'ticket'), account_id, site, service (sanitary, nappy, air-care, "
    "dryer, refills), status (completed, missed, resolved), visit_date (YYYY-MM-DD), issue, priority, fix\n"
    "- ss_tender_kb: kind, topic, cert, sector, won\n"
    "- ss_order_drafts: draft baskets (account_id, site, status = 'draft', created_by)"
)

SQL_RULES = (
    "NebulaDB SQL: exactly one semantic_match(text, '<words>') per query, then AND-ed filters: =, <>, <, <=, "
    ">, >=, IN (...), array_contains(col, 'x'). ISO dates compare as strings. Not supported: OR, subqueries, "
    "CTEs, HAVING, outer joins, JOIN with GROUP BY. Only SELECT over ss_* buckets is allowed. Filters run "
    "after retrieval over LIMIT x 4 candidates, so for an exhaustive lookup use a LIMIT that covers the "
    "bucket: 215 for ss_orders, 39 for ss_products, 48 for ss_sds, 39 for ss_washroom_jobs, 14 for "
    "ss_accounts. Escape a quote in a literal by doubling it ('St Mary''s Primary')."
)

REPORT = (
    "Base every claim on a tool result from this session and cite the tool and doc id or SQL behind it. "
    "If a tool returns an error (including 'blocked by Source Supplies guard'), read it and adapt. If the "
    "data cannot answer the question, say what is missing. End with three headings: Findings, Evidence, "
    "Next steps."
)


@dataclass(frozen=True)
class SourceAgent(Agent):
    def system_prompt(self, user_id: str | None = None) -> str:
        parts = [f"You are the {self.name} ({self.role}) on the Source Supplies AI team.", CONTEXT, self.brief, BUCKETS]
        if "execute_sql" in self.tools:
            parts.append(SQL_RULES)
        parts.append(REPORT)
        if user_id:
            parts.append(
                f"You are assisting trade account '{user_id}'. Call recall first to see what you know about "
                "this customer. When they state a lasting need, preference or change (delivery days, sites, "
                "budgets, products to avoid), store it with remember as one self-contained sentence."
            )
        return "\n\n".join(parts)


def _agent(key: str, name: str, role: str, brief: str) -> SourceAgent:
    return SourceAgent(key=key, name=name, role=role, tools=POLICIES[key].tools, brief=brief)


SOURCE_AGENTS: dict[str, SourceAgent] = {
    a.key: a
    for a in [
        _agent(
            "product",
            "Product Advisor",
            "products and greener swaps",
            "Find products that fit a need and the greener swaps for what a customer buys now. Use "
            "replaces_sku to pair a greener product with the one it replaces, and compare cost and CO2e at "
            "the same amount of use (uses_per_pack). Never claim a badge a product's badges[] lacks.",
        ),
        _agent(
            "safety",
            "Safety Agent",
            "SDS and COSHH answers",
            "Answer safety questions only from the demo SDS (ss_sds) and guides (ss_guides), naming the "
            "section each fact came from (e.g. 'Section 8: Exposure controls / PPE'). Never advise mixing "
            "products: say 'Do not mix products unless the SDS for both says it is safe' and quote Section 10. "
            "For an emergency, give the Section 4 steps and 'Call 999 in an emergency, or NHS 111 for advice' "
            "first. If the SDS does not cover it, say 'This is not in the safety data sheet.'",
        ),
        _agent(
            "order",
            "Order Agent",
            "reorder drafts",
            "Build next month's reorder draft for a site: read its delivery day (ss_accounts) and six months of "
            "ss_orders lines, forecast each SKU as the average of the last 3 months rounded up to whole packs, "
            "flag spikes over 50%, check stock and lead_days in ss_products, then save ONE draft with "
            "insert_document into ss_order_drafts: doc_id 'draft-<ACCOUNT>-<site-slug>-2026-11', metadata "
            "{status: 'draft', account_id, site, month: '2026-11', lines: [{sku, qty}], total_gbp}. Say clearly "
            "that nothing has been ordered.",
        ),
        _agent(
            "washroom",
            "Washroom Agent",
            "Source Washroom triage and missed visits",
            "Triage washroom issues against past tickets (kind = 'ticket') and spot missed visits: a site has "
            "missed a visit when a visit has status = 'missed', or its last completed visit is older than its "
            "washroom_cycle_days. Suggest a priority and service; never promise a visit time.",
        ),
        _agent(
            "sustainability",
            "Sustainability Agent",
            "CO2e and savings summaries",
            "Summarise an account's CO2e and spend from ss_orders and ss_products (co2e_kg_per_unit, price_gbp), "
            "per site and per product, and show which lines have a greener replacement (products whose "
            "replaces_sku is the line's sku). Show your arithmetic.",
        ),
        _agent(
            "assistant",
            "Account Assistant",
            "remembers each customer's needs",
            "Help a trade account with questions about their products, deliveries and service, and remember "
            "what they tell you across sessions. Search ss_* buckets with semantic_search or answer_question, "
            "always naming the bucket.",
        ),
    ]
}

SUPERVISOR_PROMPT = (
    "You are the supervisor of the Source Supplies AI team. "
    + CONTEXT
    + " You have no database tools; you work by delegating focused tasks with the delegate tool:\n"
    + "\n".join(f"- {a.key}: {a.name} ({a.role})" for a in SOURCE_AGENTS.values() if a.key != "assistant")
    + "\n\nSplit the request into specialist tasks and give each the context it needs (account ids, sites, "
    "months). Their reports are your only evidence: cross-check them, delegate follow-ups for gaps, and "
    "do not claim anything no report supports. Finish with three headings: Findings, Evidence (which "
    "report and which tool result), Next steps (each one a draft or a suggestion for a person to approve)."
)

SOURCE_DEFAULT_TASKS = {
    "product": "Which greener swaps would cut CO2 for Brightway FM without spending more?",
    "safety": "Which PPE do cleaners need for the demo descaler DS-200?",
    "order": "Draft next month's order for Kingsmead Academy, North site.",
    "washroom": "Which sites missed a sanitary visit in the last 30 days?",
    "sustainability": "Summarise CO2e and spend for Brightway FM over the last 6 months, by site.",
    "assistant": "What do you remember about us, and what should we review this month?",
    "supervisor": "Brightway FM wants to cut CO2 by 20% at their 4 office sites without spending more. What can we do this month?",
}


def _error_result(text: str) -> mcp_types.CallToolResult:
    return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=text)], isError=True)


class SourceTeam(Team):
    """Runs Source Supplies agents with every MCP call guarded in code."""

    async def _guarded_tools(self, agent: SourceAgent, user_id: str | None) -> list:
        if self._mcp_tools is None:
            listed = await self.session.list_tools()
            self._mcp_tools = {t.name: t for t in listed.tools}
        missing = [n for n in agent.tools if n not in self._mcp_tools]
        if missing:
            raise RuntimeError(f"{agent.name} needs tools nebula-mcp doesn't expose: {missing}.")
        guarded = GuardedSession(
            self.session,
            agent.key,
            make_error=_error_result,
            user_id=user_id,
            on_block=lambda a, tool, why: self.tracer.note(f"  [{SOURCE_AGENTS[a].name}] ✗ {tool} blocked: {why}"),
        )
        return [async_mcp_tool(self._mcp_tools[n], guarded) for n in agent.tools]

    async def run(self, agent_key: str, task: str, user_id: str | None = None) -> str:
        agent = SOURCE_AGENTS[agent_key]
        if agent_key == "assistant" and not user_id:
            raise ValueError("the Account Assistant needs a user (trade account id)")
        tools = await self._guarded_tools(agent, user_id)
        return await self._run(agent.name, agent.system_prompt(user_id), tools, task)

    async def supervise(self, question: str) -> str:
        team = self
        keys = [k for k in SOURCE_AGENTS if k != "assistant"]

        @beta_async_tool
        async def delegate(agent: str, task: str) -> str:
            """Hand a focused task to one Source Supplies specialist and return its report.

            Args:
                agent: Specialist key: product, safety, order, washroom or sustainability.
                task: Self-contained instructions, including account ids, sites and months.
            """
            if agent not in keys:
                return f"Unknown specialist {agent!r}. Choose one of: {', '.join(keys)}."
            team.tracer.note(f"  [Supervisor] ⇢ {SOURCE_AGENTS[agent].name}: {task}")
            report = await team.run(agent, task)
            return f"Report from {SOURCE_AGENTS[agent].name}:\n{report}"

        return await self._run("Supervisor", SUPERVISOR_PROMPT, [delegate], question)
