import { AppError, type PolicyCase } from "../types.js";

export interface AllocatableOffer {
  offer_id: number;
  is_pinned: boolean;
  share: number;
}

export interface OfferShare {
  offer_id: number;
  is_pinned: boolean;
  share: number;
}

export interface AllocationResult {
  shares: OfferShare[];
  pinned_total: number;
  unallocated: number;
  policy_cases: PolicyCase[];
}

function allocationError(message: string): AppError {
  return new AppError(422, "DOMAIN_POLICY_REJECTION", message, {
    reason: message,
  });
}

function validateOffers(items: AllocatableOffer[]): number {
  const seen = new Set<number>();
  for (const offer of items) {
    if (!Number.isInteger(offer.offer_id)) {
      throw allocationError(
        `offer_id must be an int, got ${typeof offer.offer_id}`,
      );
    }
    if (typeof offer.is_pinned !== "boolean") {
      throw allocationError(
        `offer ${offer.offer_id}: is_pinned must be a bool`,
      );
    }
    if (!Number.isInteger(offer.share) || offer.share < 0 || offer.share > 100) {
      throw allocationError(
        `offer ${offer.offer_id}: share must be an int in 0..100, got ${offer.share}`,
      );
    }
    if (seen.has(offer.offer_id)) {
      throw allocationError(`duplicate offer_id ${offer.offer_id}`);
    }
    seen.add(offer.offer_id);
  }

  const pinnedTotal = items
    .filter((offer) => offer.is_pinned)
    .reduce((sum, offer) => sum + offer.share, 0);

  if (pinnedTotal > 100) {
    throw allocationError(
      `pinned total ${pinnedTotal} exceeds 100 percent`,
    );
  }
  return pinnedTotal;
}

function unpinnedShares(
  unpinned: AllocatableOffer[],
  remaining: number,
): Map<number, number> {
  const result = new Map<number, number>();
  if (unpinned.length === 0) {
    return result;
  }
  const base = Math.floor(remaining / unpinned.length);
  const extra = remaining % unpinned.length;
  const sorted = [...unpinned].sort((a, b) => a.offer_id - b.offer_id);
  sorted.forEach((offer, index) => {
    result.set(offer.offer_id, base + (index < extra ? 1 : 0));
  });
  return result;
}

function detectPolicyCases(
  items: AllocatableOffer[],
  unallocated: number,
): PolicyCase[] {
  const cases: PolicyCase[] = [];
  if (items.length === 0) {
    cases.push("no_active_offers");
  }
  if (items.length === 1) {
    cases.push("single_active_offer");
  }
  if (unallocated > 0) {
    cases.push("unallocated_remainder");
  }
  return cases;
}

export function allocate(offers: Iterable<AllocatableOffer>): AllocationResult {
  const items = Array.from(offers);
  const pinnedTotal = validateOffers(items);

  const pinned = items.filter((offer) => offer.is_pinned);
  const unpinned = items.filter((offer) => !offer.is_pinned);
  const allocated = new Map<number, number>();

  for (const offer of pinned) {
    allocated.set(offer.offer_id, offer.share);
  }
  for (const [offerId, share] of unpinnedShares(unpinned, 100 - pinnedTotal)) {
    allocated.set(offerId, share);
  }

  const sorted = [...items].sort((a, b) => a.offer_id - b.offer_id);
  const shares: OfferShare[] = sorted.map((offer) => ({
    offer_id: offer.offer_id,
    is_pinned: offer.is_pinned,
    share: allocated.get(offer.offer_id) ?? 0,
  }));

  const unallocated =
    100 - shares.reduce((sum, item) => sum + item.share, 0);

  return {
    shares,
    pinned_total: pinnedTotal,
    unallocated,
    policy_cases: detectPolicyCases(items, unallocated),
  };
}
