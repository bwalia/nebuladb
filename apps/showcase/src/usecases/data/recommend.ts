/**
 * Product catalogue for recommendations (filters + semantic neighbours).
 * Fictional workstation / AI hardware boutique.
 */
import type { SeedDoc } from "../../rag/seed";

export const SEED_VERSION = "reco-v1";
export const PRODUCT_BUCKET = "reco_products";

export const CATEGORIES = ["gpu", "cpu", "storage", "chassis", "network"] as const;
export type Category = (typeof CATEGORIES)[number];

function prod(
  id: string,
  name: string,
  category: Category,
  price_gbp: number,
  stock: number,
  tags: string[],
  blurb: string
): SeedDoc {
  return {
    id,
    text: `${name} · ${category} · £${price_gbp} · stock ${stock}\nTags: ${tags.join(", ")}\n${blurb}`,
    metadata: {
      sku: id,
      name,
      category,
      price_gbp,
      stock,
      tags,
    },
  };
}

export const PRODUCTS: SeedDoc[] = [
  prod("SKU-GPU-60", "RTX PRO 6000 Blackwell", "gpu", 7200, 4, ["blackwell", "pro", "vram-96"],
    "Workstation GPU for local LLM and CAD. 96GB VCH. Needs 600W PSU headroom."),
  prod("SKU-GPU-50", "RTX 5090 Founders", "gpu", 2100, 12, ["blackwell", "gaming", "vram-32"],
    "High-end gaming / light inference. Not ECC. Pairs with ATX 3.1 PSU."),
  prod("SKU-GPU-40", "RTX 4080 Super", "gpu", 1100, 20, ["ada", "gaming", "vram-16"],
    "Previous-gen sweet spot for creators on a budget."),
  prod("SKU-CPU-99", "Threadripper PRO 7995WX", "cpu", 9500, 3, ["tr-pro", "96c", "wrx90"],
    "96-core WRX90 CPU for simulation and multi-GPU hosts."),
  prod("SKU-CPU-85", "Ryzen 9 9950X", "cpu", 580, 25, ["am5", "16c", "creator"],
    "16-core AM5 for single-GPU workstations."),
  prod("SKU-CPU-70", "Core Ultra 9 285K", "cpu", 620, 18, ["lga1851", "npu", "creator"],
    "Arrow Lake with NPU for hybrid AI PCs."),
  prod("SKU-SSD-8", "NeonGen 8TB Gen5 NVMe", "storage", 890, 30, ["gen5", "8tb", "heatsink"],
    "8000 MB/s class — pair with chassis that has airflow over M.2."),
  prod("SKU-SSD-2", "NeonGen 2TB Gen4 NVMe", "storage", 140, 80, ["gen4", "2tb"],
    "Boot / scratch drive."),
  prod("SKU-CASE-TR", "TR PRO Tower Quiet", "chassis", 420, 9, ["tr-pro", "4x-gpu", "quiet"],
    "Supports 4× double-slot GPUs and WRX90 boards. Reinforced PCIe spine."),
  prod("SKU-CASE-ATX", "Creator ATX Mesh", "chassis", 160, 40, ["atx", "mesh", "2x-gpu"],
    "ATX mid-tower, dual GPU, strong intake."),
  prod("SKU-NIC-100", "Dual 100GbE QSFP28", "network", 780, 6, ["100g", "qsfp", "rdma"],
    "For model checkpoint sync across nodes."),
  prod("SKU-NIC-25", "Dual 25GbE SFP28", "network", 210, 22, ["25g", "sfp"],
    "Lab / edge inference racks."),
];

export const BASKETS: Array<{ id: string; label: string; skus: string[] }> = [
  { id: "b1", label: "Local LLM workstation", skus: ["SKU-GPU-60", "SKU-CPU-99"] },
  { id: "b2", label: "Creator PC upgrade", skus: ["SKU-GPU-50", "SKU-CPU-85"] },
  { id: "b3", label: "Storage refresh", skus: ["SKU-SSD-8"] },
];
