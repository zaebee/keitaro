import {
  AppError,
  type OfferEntry,
  type PolicyCase,
  type PublicationStatus,
  type StagedOffer,
} from "../types.js";

export type EligibilityReason =
  | "empty_offers"
  | "pinned_sum_exceeded"
  | "unallocated_remainder"
  | "invalid_total";

export interface EligibilityResult {
  eligible: boolean;
  active_total: number;
  pinned_total: number;
  policy_cases: PolicyCase[];
  reason: EligibilityReason | null;
}

const ALLOWED_TRANSITIONS: Record<PublicationStatus, ReadonlySet<PublicationStatus>> = {
  pending: new Set(["in_progress", "abandoned"]),
  in_progress: new Set(["succeeded", "failed", "ambiguous"]),
  succeeded: new Set(),
  failed: new Set(["abandoned"]),
  ambiguous: new Set(["succeeded", "abandoned"]),
  abandoned: new Set(),
};

const WRITABLE_OFFER_STATES = new Set(["active", "disabled"]);

export function canTransition(
  current: PublicationStatus,
  target: PublicationStatus,
): boolean {
  return ALLOWED_TRANSITIONS[current].has(target);
}

export function evaluateEligibility(
  offers: Iterable<StagedOffer>,
  policyCases: PolicyCase[] = [],
  unallocated = 0,
): EligibilityResult {
  const active = Array.from(offers).filter(
    (offer) => offer.lifecycle === "ACTIVE",
  );
  if (active.length === 0) {
    return {
      eligible: false,
      active_total: 0,
      pinned_total: 0,
      policy_cases: policyCases,
      reason: "empty_offers",
    };
  }

  const pinnedTotal = active
    .filter((offer) => offer.is_pinned)
    .reduce((sum, offer) => sum + offer.share, 0);
  const total = active.reduce((sum, offer) => sum + offer.share, 0);

  if (pinnedTotal > 100) {
    return {
      eligible: false,
      active_total: total,
      pinned_total: pinnedTotal,
      policy_cases: policyCases,
      reason: "pinned_sum_exceeded",
    };
  }

  if (total !== 100) {
    const reason: EligibilityReason =
      unallocated > 0 || total < 100 ? "unallocated_remainder" : "invalid_total";
    return {
      eligible: false,
      active_total: total,
      pinned_total: pinnedTotal,
      policy_cases: policyCases,
      reason,
    };
  }

  return {
    eligible: true,
    active_total: total,
    pinned_total: pinnedTotal,
    policy_cases: policyCases,
    reason: null,
  };
}

export function buildOfferEntries(offers: Iterable<StagedOffer>): OfferEntry[] {
  const entries: OfferEntry[] = [];
  const seen = new Set<number>();
  for (const offer of offers) {
    if (offer.lifecycle !== "ACTIVE") {
      continue;
    }
    if (!Number.isInteger(offer.offer_id) || offer.offer_id <= 0) {
      throw new AppError(422, "DOMAIN_POLICY_REJECTION", "offer_id must be a positive int");
    }
    if (!Number.isInteger(offer.share) || offer.share < 0 || offer.share > 100) {
      throw new AppError(
        422,
        "DOMAIN_POLICY_REJECTION",
        `offer ${offer.offer_id}: share must be an int in 0..100`,
      );
    }
    if (seen.has(offer.offer_id)) {
      throw new AppError(
        422,
        "DOMAIN_POLICY_REJECTION",
        `duplicate offer_id ${offer.offer_id}`,
      );
    }
    seen.add(offer.offer_id);
    const state =
      offer.state && WRITABLE_OFFER_STATES.has(offer.state) ? offer.state : null;
    entries.push({ offer_id: offer.offer_id, share: offer.share, state });
  }
  return entries;
}

export function entriesMatch(
  observed: Iterable<OfferEntry>,
  expected: Iterable<OfferEntry>,
): boolean {
  const wanted = new Map(Array.from(expected).map((e) => [e.offer_id, e]));
  const found = new Map(Array.from(observed).map((e) => [e.offer_id, e]));
  if (wanted.size !== found.size) {
    return false;
  }
  for (const [id, entry] of wanted) {
    const match = found.get(id);
    if (!match || match.share !== entry.share) {
      return false;
    }
    const stateMatches =
      entry.state === null
        ? match.state === null || match.state === "active"
        : match.state === entry.state;
    if (!stateMatches) {
      return false;
    }
  }
  return true;
}
