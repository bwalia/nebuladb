import type { Pack } from "../types.ts";
import { PACK as campus } from "./campus.ts";
import { PACK as carehome } from "./carehome.ts";
import { PACK as chat } from "./chat.ts";
import { PACK as cms } from "./cms.ts";
import { PACK as joyai } from "./joyai.ts";
import { PACK as kisaan } from "./kisaan.ts";
import { PACK as source } from "./source.ts";
import { PACK as visa } from "./visa.ts";
import { currentSite, type Site } from "../../tenant.ts";

/** Every business, in the order shown in the pack picker. */
export const ALL_PACKS: Pack[] = [campus, carehome, visa, chat, cms, joyai, kisaan, source];

/** A customer site only ever sees its own business's data. */
const SITE_PACK: Partial<Record<Site, string>> = { sourcesupplies: "source" };

/** The packs this site may show (all of them on the combined showcase). */
export const PACKS: Pack[] = (() => {
  if (typeof window === "undefined") return ALL_PACKS; // node --test
  const only = SITE_PACK[currentSite()];
  return only ? ALL_PACKS.filter((p) => p.id === only) : ALL_PACKS;
})();

export function getPack(id: string | undefined): Pack {
  return PACKS.find((p) => p.id === id) ?? PACKS[0];
}
