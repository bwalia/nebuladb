/**
 * Every seeded Source Supplies bucket in one list, so each tab can ask
 * for just the buckets it reads and the home tab can seed them all.
 * (`ss_order_drafts` is written by the apps and agents, never seeded.)
 */
import type { SeedDoc } from "./common.ts";
import { BUCKET as PRODUCTS_BUCKET, PRODUCT_DOCS, SEED_VERSION as PRODUCTS_V } from "./products.ts";
import { BUCKET as SDS_BUCKET, SDS_DOCS, SEED_VERSION as SDS_V } from "./sds.ts";
import { BUCKET as GUIDES_BUCKET, GUIDE_DOCS, SEED_VERSION as GUIDES_V } from "./guides.ts";
import { ACCOUNTS_BUCKET, ACCOUNT_DOCS, ORDERS_BUCKET, ORDER_DOCS, SEED_VERSION as ACCOUNTS_V } from "./accounts.ts";
import { BUCKET as WASHROOM_BUCKET, JOB_DOCS, SEED_VERSION as WASHROOM_V } from "./washroom.ts";
import { BUCKET as TENDER_BUCKET, TENDER_DOCS, SEED_VERSION as TENDER_V } from "./tender.ts";

export interface SeedSet {
  bucket: string;
  docs: SeedDoc[];
  version: string;
  noun: string;
}

export const SEEDS = {
  products: { bucket: PRODUCTS_BUCKET, docs: PRODUCT_DOCS, version: PRODUCTS_V, noun: "products" },
  sds: { bucket: SDS_BUCKET, docs: SDS_DOCS, version: SDS_V, noun: "SDS sections" },
  guides: { bucket: GUIDES_BUCKET, docs: GUIDE_DOCS, version: GUIDES_V, noun: "guide chunks" },
  accounts: { bucket: ACCOUNTS_BUCKET, docs: ACCOUNT_DOCS, version: ACCOUNTS_V, noun: "sites" },
  orders: { bucket: ORDERS_BUCKET, docs: ORDER_DOCS, version: ACCOUNTS_V, noun: "order lines" },
  washroom: { bucket: WASHROOM_BUCKET, docs: JOB_DOCS, version: WASHROOM_V, noun: "washroom jobs" },
  tender: { bucket: TENDER_BUCKET, docs: TENDER_DOCS, version: TENDER_V, noun: "tender KB entries" },
} satisfies Record<string, SeedSet>;

export type SeedKey = keyof typeof SEEDS;
