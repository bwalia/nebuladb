import type { Pack } from "../types";
import type { LabState } from "../engine/core";
import type { LabEnv } from "../shell";

export interface LabProps {
  pack: Pack;
  state: LabState;
  update: (p: Partial<LabState>) => void;
  env: LabEnv | null;
}
