"""Tests for the Source Supplies agent guards (standard library only).

    python -m unittest -v test_source_guards
"""

from __future__ import annotations

import asyncio
import unittest

from source_guards import (
    DRAFTS_BUCKET,
    MEMORY_BUCKET,
    POLICIES,
    WRITE_TOOLS,
    GuardedSession,
    GuardError,
    check,
    check_sql,
)

DRAFT = {
    "bucket": DRAFTS_BUCKET,
    "doc_id": "draft-KINGSMEAD-north-site-2026-11",
    "text": "Draft basket",
    "metadata": {"status": "draft", "account_id": "KINGSMEAD", "site": "North site"},
}


class ToolLists(unittest.TestCase):
    def test_tool_lists_match_the_spec(self):
        self.assertEqual(POLICIES["product"].tools, ("execute_sql", "semantic_search", "get_document"))
        self.assertEqual(POLICIES["safety"].tools, ("semantic_search", "get_document"))
        self.assertEqual(POLICIES["order"].tools, ("execute_sql", "semantic_search", "insert_document"))
        self.assertEqual(POLICIES["washroom"].tools, ("execute_sql", "semantic_search", "get_document"))
        self.assertEqual(POLICIES["sustainability"].tools, ("execute_sql", "get_document"))
        self.assertEqual(POLICIES["assistant"].tools, ("recall", "remember", "semantic_search", "answer_question"))

    def test_only_order_and_assistant_have_write_tools(self):
        writers = {k for k, p in POLICIES.items() if set(p.tools) & WRITE_TOOLS}
        self.assertEqual(writers, {"order", "assistant"})
        self.assertEqual(set(POLICIES["order"].tools) & WRITE_TOOLS, {"insert_document"})
        self.assertEqual(set(POLICIES["assistant"].tools) & WRITE_TOOLS, {"remember"})

    def test_tools_outside_the_policy_are_rejected(self):
        with self.assertRaises(GuardError):
            check("product", "insert_document", DRAFT)
        with self.assertRaises(GuardError):
            check("sustainability", "semantic_search", {"query": "x", "bucket": "ss_products"})
        with self.assertRaises(GuardError):
            check("washroom", "delete_document", {"bucket": "ss_washroom_jobs", "doc_id": "x"})
        with self.assertRaises(GuardError):
            check("nobody", "execute_sql", {"sql": "SELECT * FROM ss_products"})


class Sql(unittest.TestCase):
    def test_read_only_select_over_ss_buckets(self):
        check_sql("SELECT * FROM ss_orders WHERE semantic_match(text, 'order lines') AND account_id = 'KINGSMEAD' LIMIT 215")
        check_sql("select * from ss_products where semantic_match(text, 'x') limit 5;")

    def test_keywords_inside_string_literals_are_fine(self):
        check_sql("SELECT * FROM ss_guides WHERE semantic_match(text, 'how to delete and drop gum') LIMIT 5")
        check_sql("SELECT * FROM ss_accounts WHERE semantic_match(text, 'x') AND site = 'St Mary''s Primary' LIMIT 14")

    def test_rejects_other_buckets_writes_and_multiple_statements(self):
        for bad in [
            "SELECT * FROM agent_memory WHERE semantic_match(text, 'x') LIMIT 5",
            "SELECT * FROM docs WHERE semantic_match(text, 'x') LIMIT 5",
            "SELECT * FROM ss_orders o JOIN runbooks r ON o.sku = r.sku WHERE semantic_match(text, 'x')",
            "DELETE FROM ss_orders",
            "INSERT INTO ss_order_drafts VALUES (1)",
            "SELECT * FROM ss_orders WHERE semantic_match(text, 'x'); DROP TABLE ss_orders",
            "UPDATE ss_products SET price_gbp = 0",
            "",
            "SELECT 1",
        ]:
            with self.subTest(sql=bad), self.assertRaises(GuardError):
                check("order", "execute_sql", {"sql": bad})


class Buckets(unittest.TestCase):
    def test_safety_agent_reads_only_sds_and_guides(self):
        check("safety", "semantic_search", {"query": "PPE for DS-200", "bucket": "ss_sds"})
        check("safety", "semantic_search", {"query": "descaling", "bucket": "ss_guides"})
        check("safety", "get_document", {"bucket": "ss_sds", "doc_id": "ss-sds-DS-200-s08"})
        for bucket in ["ss_products", "ss_orders", "agent_memory", "runbooks", ""]:
            with self.subTest(bucket=bucket), self.assertRaises(GuardError):
                check("safety", "semantic_search", {"query": "x", "bucket": bucket})
        with self.assertRaises(GuardError):
            check("safety", "semantic_search", {"query": "x"})  # no bucket = every bucket
        with self.assertRaises(GuardError):
            check("safety", "get_document", {"bucket": "ss_products", "doc_id": "ss-prod-DS-200"})

    def test_other_readers_stay_inside_ss_buckets(self):
        check("product", "semantic_search", {"query": "x", "bucket": "ss_products"})
        check("washroom", "get_document", {"bucket": "ss_washroom_jobs", "doc_id": "ss-job-T-4101"})
        check("assistant", "answer_question", {"query": "x", "bucket": "ss_tender_kb"})
        for agent, tool in [("product", "semantic_search"), ("washroom", "get_document"), ("assistant", "answer_question")]:
            with self.subTest(agent=agent), self.assertRaises(GuardError):
                check(agent, tool, {"query": "x", "bucket": "runbooks", "doc_id": "x"})
            with self.subTest(agent=agent, bucket=None), self.assertRaises(GuardError):
                check(agent, tool, {"query": "x", "doc_id": "x"})


class Drafts(unittest.TestCase):
    def test_order_agent_writes_drafts(self):
        out = check("order", "insert_document", DRAFT)
        self.assertEqual(out["metadata"]["status"], "draft")
        self.assertEqual(out["metadata"]["created_by"], "order-agent")

    def test_order_agent_cannot_write_elsewhere_or_place_orders(self):
        cases = [
            {**DRAFT, "bucket": "ss_orders"},
            {**DRAFT, "bucket": "ss_products"},
            {**DRAFT, "bucket": MEMORY_BUCKET},
            {**DRAFT, "metadata": {**DRAFT["metadata"], "status": "placed"}},
            {**DRAFT, "metadata": {**DRAFT["metadata"], "status": "confirmed"}},
            {**DRAFT, "metadata": {"account_id": "KINGSMEAD", "site": "North site"}},
            {**DRAFT, "metadata": {"status": "draft", "site": "North site"}},
            {**DRAFT, "metadata": None},
            {**DRAFT, "doc_id": "ss-ord-KINGSMEAD-north-site-2026-10-JR-410"},
        ]
        for args in cases:
            with self.subTest(args=args), self.assertRaises(GuardError):
                check("order", "insert_document", args)


class Memory(unittest.TestCase):
    def test_assistant_memory_is_scoped_to_the_session_user(self):
        out = check("assistant", "remember", {"text": "Moving to 3 deliveries a week", "user_id": "BRIGHTWAY"}, user_id="BRIGHTWAY")
        self.assertEqual(out["bucket"], MEMORY_BUCKET)
        out = check("assistant", "recall", {"query": "deliveries"}, user_id="BRIGHTWAY")
        self.assertEqual(out["user_id"], "BRIGHTWAY")

    def test_assistant_cannot_touch_other_users_or_buckets(self):
        with self.assertRaises(GuardError):
            check("assistant", "remember", {"text": "x", "user_id": "KINGSMEAD"}, user_id="BRIGHTWAY")
        with self.assertRaises(GuardError):
            check("assistant", "remember", {"text": "x", "user_id": "BRIGHTWAY", "bucket": "ss_order_drafts"}, user_id="BRIGHTWAY")
        with self.assertRaises(GuardError):
            check("assistant", "recall", {"query": "x"}, user_id=None)
        with self.assertRaises(GuardError):
            check("order", "remember", {"text": "x", "user_id": "BRIGHTWAY"}, user_id="BRIGHTWAY")


class Session(unittest.TestCase):
    def test_blocked_calls_never_reach_mcp(self):
        calls: list[tuple[str, dict]] = []
        blocked: list[tuple[str, str, str]] = []

        class FakeSession:
            async def call_tool(self, name, arguments=None, **kw):
                calls.append((name, arguments))
                return {"ok": True}

        s = GuardedSession(FakeSession(), "order", make_error=lambda t: {"error": t}, on_block=lambda *a: blocked.append(a))

        async def go():
            bad = await s.call_tool("insert_document", {**DRAFT, "bucket": "ss_orders"})
            good = await s.call_tool("insert_document", DRAFT)
            sql = await s.call_tool("execute_sql", {"sql": "DELETE FROM ss_orders"})
            return bad, good, sql

        bad, good, sql = asyncio.run(go())
        self.assertIn("blocked", bad["error"])
        self.assertIn("blocked", sql["error"])
        self.assertEqual(good, {"ok": True})
        self.assertEqual([c[0] for c in calls], ["insert_document"])
        self.assertEqual(calls[0][1]["metadata"]["created_by"], "order-agent")
        self.assertEqual(len(blocked), 2)


if __name__ == "__main__":
    unittest.main()
