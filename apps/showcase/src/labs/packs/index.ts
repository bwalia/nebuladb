import type { Pack } from "../types.ts";
import { PACK as campus } from "./campus.ts";
import { PACK as carehome } from "./carehome.ts";
import { PACK as chat } from "./chat.ts";
import { PACK as cms } from "./cms.ts";
import { PACK as joyai } from "./joyai.ts";
import { PACK as kisaan } from "./kisaan.ts";
import { PACK as source } from "./source.ts";

/** Order shown in the pack picker. */
export const PACKS: Pack[] = [campus, carehome, chat, cms, joyai, kisaan, source];

export function getPack(id: string | undefined): Pack {
  return PACKS.find((p) => p.id === id) ?? PACKS[0];
}
