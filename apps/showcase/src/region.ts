/**
 * Display name for a region. The cluster's real region ids stay as they are
 * (buckets store `home_region` and the server rejects writes whose home does
 * not match its own region), so the showcase only renames them on screen:
 * us-west-N is shown as uk-west-N. Use the raw id for keys and API calls.
 */
export function regionLabel(region: string): string;
export function regionLabel(region: string | null | undefined): string | undefined;
export function regionLabel(region: string | null | undefined): string | undefined {
  if (region == null) return undefined;
  return region.replace(/\bus-west-/g, "uk-west-");
}
