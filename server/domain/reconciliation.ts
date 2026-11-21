import crypto from "node:crypto";
import {
  AppError,
  type BindingState,
  type BindingTransition,
  type ConflictDetail,
  type RemoteOffer,
} from "../types.js";

export interface DraftReconciliationState {
  baseline_offers: RemoteOffer[];
  staged_changes: Record<string, unknown>;
  is_stale: boolean;
  conflicts: ConflictDetail[];
}

export interface ReconciliationPlan {
  stream_ref: number;
  remote_offers: RemoteOffer[];
  transitions: BindingTransition[];
  conflicts: ConflictDetail[];
}

function validationError(message: string): AppError {
  return new AppError(422, "DOMAIN_POLICY_REJECTION", message, {
    reason: message,
  });
}

export function validateRemoteOffers(
  offers: Iterable<RemoteOffer>,
): RemoteOffer[] {
  const items = Array.from(offers);
  const seen = new Set<number>();
  for (const offer of items) {
    if (!Number.isInteger(offer.offer_id) || offer.offer_id <= 0) {
      throw validationError("offer_id must be a positive int");
    }
    if (!Number.isInteger(offer.share) || offer.share < 0 || offer.share > 100) {
      throw validationError(
        `offer ${offer.offer_id}: share must be an int in 0..100`,
      );
    }
    if (offer.state !== null && typeof offer.state !== "string") {
      throw validationError(
        `offer ${offer.offer_id}: state must be a string or None`,
      );
    }
    if (!Number.isInteger(offer.position) || offer.position < 0) {
      throw validationError(
        `offer ${offer.offer_id}: position must be a non-negative int`,
      );
    }
    if (seen.has(offer.offer_id)) {
      throw validationError(`duplicate offer_id ${offer.offer_id}`);
    }
    seen.add(offer.offer_id);
  }
  return items;
}

function stagedIds(stagedChanges: Record<string, unknown>, key: string): Set<number> {
  const raw = stagedChanges[key];
  if (!Array.isArray(raw)) {
    return new Set();
  }
  return new Set(raw.filter((item): item is number => Number.isInteger(item)));
}

function stagedMappingIds(
  stagedChanges: Record<string, unknown>,
  key: string,
): Set<number> {
  const raw = stagedChanges[key];
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return new Set();
  }
  const ids = new Set<number>();
  for (const k of Object.keys(raw)) {
    if (/^\d+$/.test(k)) {
      ids.add(Number(k));
    }
  }
  return ids;
}

function allocationInputIds(
  stagedChanges: Record<string, unknown>,
  candidates: Set<number>,
): Set<number> {
  const archives = stagedIds(stagedChanges, "archives");
  const restores = stagedIds(stagedChanges, "restores");
  const shareEdits = stagedMappingIds(stagedChanges, "share_edits");
  const active = new Set<number>();
  for (const id of candidates) {
    if (!archives.has(id)) {
      active.add(id);
    }
  }
  for (const id of restores) {
    if (candidates.has(id)) {
      active.add(id);
    }
  }
  for (const id of shareEdits) {
    active.delete(id);
  }
  return active;
}

function compareConflicts(a: ConflictDetail, b: ConflictDetail): number {
  if (a.stream_ref !== b.stream_ref) return a.stream_ref - b.stream_ref;
  if (a.offer_id !== b.offer_id) return a.offer_id - b.offer_id;
  if (a.field !== b.field) return a.field < b.field ? -1 : 1;
  const ea = String(a.expected);
  const eb = String(b.expected);
  if (ea !== eb) return ea < eb ? -1 : 1;
  const oa = String(a.observed);
  const ob = String(b.observed);
  if (oa !== ob) return oa < ob ? -1 : 1;
  return a.reason < b.reason ? -1 : a.reason > b.reason ? 1 : 0;
}

export function canonicalConflictsJson(conflicts: Iterable<ConflictDetail>): string {
  const ordered = Array.from(conflicts).sort(compareConflicts);
  return JSON.stringify(
    ordered.map((item) => ({
      stream_ref: item.stream_ref,
      offer_id: item.offer_id,
      field: item.field,
      expected: item.expected,
      observed: item.observed,
      reason: item.reason,
    })),
  );
}

export function conflictFingerprint(conflicts: Iterable<ConflictDetail>): string {
  const hash = crypto
    .createHash("sha256")
    .update(canonicalConflictsJson(conflicts), "utf-8")
    .digest("hex");
  return `sha256:${hash}`;
}

function conflictKey(c: ConflictDetail): string {
  return JSON.stringify([
    c.stream_ref,
    c.offer_id,
    c.field,
    c.expected,
    c.observed,
    c.reason,
  ]);
}

function mergeConflicts(
  existing: ConflictDetail[],
  found: ConflictDetail[],
): ConflictDetail[] {
  const map = new Map<string, ConflictDetail>();
  for (const item of [...existing, ...found]) {
    const key = conflictKey(item);
    if (!map.has(key)) {
      map.set(key, item);
    }
  }
  return Array.from(map.values()).sort(compareConflicts);
}

function draftConflicts(
  streamRef: number,
  remote: RemoteOffer[],
  transitions: BindingTransition[],
  draft: DraftReconciliationState,
): ConflictDetail[] {
  const baselineOffers = validateRemoteOffers(draft.baseline_offers);
  const baseline = new Map(baselineOffers.map((o) => [o.offer_id, o]));
  const observed = new Map(remote.map((o) => [o.offer_id, o]));

  const reappeared = new Set(
    transitions
      .filter((t) => t.kind === "REACTIVATED")
      .map((t) => t.offer_id),
  );

  const found: ConflictDetail[] = [];
  const addedIds = remote
    .map((o) => o.offer_id)
    .filter((id) => !baseline.has(id) && !reappeared.has(id))
    .sort((a, b) => a - b);
  for (const offerId of addedIds) {
    found.push({
      stream_ref: streamRef,
      offer_id: offerId,
      field: "offer",
      expected: null,
      observed: offerId,
      reason: "remote_offer_added",
    });
  }

  const removedIds = baselineOffers
    .map((o) => o.offer_id)
    .filter((id) => !observed.has(id))
    .sort((a, b) => a - b);
  for (const offerId of removedIds) {
    found.push({
      stream_ref: streamRef,
      offer_id: offerId,
      field: "offer",
      expected: offerId,
      observed: null,
      reason: "remote_offer_removed",
    });
  }

  const reappearedIds = Array.from(reappeared)
    .filter((id) => observed.has(id))
    .sort((a, b) => a - b);
  for (const offerId of reappearedIds) {
    found.push({
      stream_ref: streamRef,
      offer_id: offerId,
      field: "offer",
      expected: null,
      observed: offerId,
      reason: "remote_offer_reappeared",
    });
  }

  const hasChanges = Object.keys(draft.staged_changes).length > 0;
  if (hasChanges) {
    const intersection = new Set(
      baselineOffers
        .map((o) => o.offer_id)
        .filter((id) => observed.has(id)),
    );
    const inputIds = Array.from(
      allocationInputIds(draft.staged_changes, intersection),
    ).sort((a, b) => a - b);
    for (const offerId of inputIds) {
      const expected = baseline.get(offerId)!.share;
      const actual = observed.get(offerId)!.share;
      if (expected !== actual) {
        found.push({
          stream_ref: streamRef,
          offer_id: offerId,
          field: "share",
          expected,
          observed: actual,
          reason: "remote_share_changed",
        });
      }
    }
  }

  return mergeConflicts(draft.conflicts, found);
}

export function planReconciliation(params: {
  stream_ref: number;
  remote_offers: Iterable<RemoteOffer>;
  previous_bindings: Iterable<BindingState>;
  draft?: DraftReconciliationState | null;
}): ReconciliationPlan {
  const remote = validateRemoteOffers(params.remote_offers);
  const remoteIds = new Set(remote.map((o) => o.offer_id));
  const bindings = new Map(
    Array.from(params.previous_bindings).map((b) => [b.offer_id, b]),
  );

  const transitions: BindingTransition[] = [];
  for (const offer of remote) {
    const binding = bindings.get(offer.offer_id);
    if (!binding) {
      transitions.push({
        offer_id: offer.offer_id,
        kind: "CREATED",
        share: offer.share,
      });
    } else if (binding.lifecycle === "ARCHIVED") {
      transitions.push({
        offer_id: offer.offer_id,
        kind: "REACTIVATED",
        share: offer.share,
      });
    } else if (binding.last_remote_share !== offer.share) {
      transitions.push({
        offer_id: offer.offer_id,
        kind: "SHARE_CHANGED",
        share: offer.share,
      });
    }
  }

  for (const [offerId, binding] of bindings) {
    if (!remoteIds.has(offerId) && binding.lifecycle === "ACTIVE") {
      transitions.push({
        offer_id: offerId,
        kind: "ARCHIVED",
        share: null,
      });
    }
  }

  transitions.sort((a, b) => {
    if (a.offer_id !== b.offer_id) return a.offer_id - b.offer_id;
    return a.kind < b.kind ? -1 : a.kind > b.kind ? 1 : 0;
  });

  const conflicts = params.draft
    ? draftConflicts(params.stream_ref, remote, transitions, params.draft)
    : [];

  return {
    stream_ref: params.stream_ref,
    remote_offers: remote,
    transitions,
    conflicts,
  };
}
