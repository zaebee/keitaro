import {
  AppError,
  type BaselineBinding,
  type BindingLifecycle,
  type DraftIntent,
  type PolicyCase,
  type RemoteOffer,
  type StagedOffer,
} from "../types.js";
import { allocate, type AllocationResult } from "./allocation.js";
import { validateRemoteOffers } from "./reconciliation.js";

export type DraftCommandKind =
  | "add"
  | "remove"
  | "restore"
  | "set_pin"
  | "set_share"
  | "cancel";

export interface DraftCommand {
  kind: DraftCommandKind;
  offer_id?: number;
  value?: number | boolean;
}

export interface DraftViewData {
  offers: StagedOffer[];
  policy_cases: PolicyCase[];
  unallocated: number;
}

export interface DraftContext {
  stream_ref: number;
  baseline_offers: RemoteOffer[];
  baseline_bindings: BaselineBinding[];
  intent: DraftIntent;
}

export interface DraftPlan {
  intent: DraftIntent;
  view: DraftViewData;
  noop: boolean;
}

function draftInputError(message: string): AppError {
  return new AppError(422, "DOMAIN_POLICY_REJECTION", message, {
    reason: message,
  });
}

export function emptyIntent(): DraftIntent {
  return {
    additions: [],
    archives: [],
    restores: [],
    pin_toggles: {},
    share_edits: {},
  };
}

export function hasIntentChanges(intent: DraftIntent): boolean {
  return (
    intent.additions.length > 0 ||
    intent.archives.length > 0 ||
    intent.restores.length > 0 ||
    Object.keys(intent.pin_toggles).length > 0 ||
    Object.keys(intent.share_edits).length > 0
  );
}

export function intentToJson(intent: DraftIntent): Record<string, unknown> {
  const data: Record<string, unknown> = {};
  if (intent.additions.length > 0) {
    data.additions = [...intent.additions];
  }
  if (intent.archives.length > 0) {
    data.archives = [...intent.archives];
  }
  if (intent.restores.length > 0) {
    data.restores = [...intent.restores];
  }
  const pinKeys = Object.keys(intent.pin_toggles)
    .map(Number)
    .sort((a, b) => a - b);
  if (pinKeys.length > 0) {
    const pinToggles: Record<string, boolean> = {};
    for (const k of pinKeys) {
      pinToggles[String(k)] = intent.pin_toggles[k];
    }
    data.pin_toggles = pinToggles;
  }
  const shareKeys = Object.keys(intent.share_edits)
    .map(Number)
    .sort((a, b) => a - b);
  if (shareKeys.length > 0) {
    const shareEdits: Record<string, number> = {};
    for (const k of shareKeys) {
      shareEdits[String(k)] = intent.share_edits[k];
    }
    data.share_edits = shareEdits;
  }
  return data;
}

export function validateShare(value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > 100) {
    throw draftInputError(`share must be an int in 0..100, got ${String(value)}`);
  }
  return value;
}

function orderedIds(context: DraftContext): number[] {
  const ids: number[] = [];
  const seen = new Set<number>();
  for (const offer of context.baseline_offers) {
    if (!seen.has(offer.offer_id)) {
      seen.add(offer.offer_id);
      ids.push(offer.offer_id);
    }
  }
  for (const offerId of [...context.intent.additions].sort((a, b) => a - b)) {
    if (!seen.has(offerId)) {
      seen.add(offerId);
      ids.push(offerId);
    }
  }
  for (const binding of [...context.baseline_bindings].sort(
    (a, b) => a.offer_id - b.offer_id,
  )) {
    if (!seen.has(binding.offer_id)) {
      seen.add(binding.offer_id);
      ids.push(binding.offer_id);
    }
  }
  return ids;
}

function viewFromAllocations(
  items: StagedOffer[],
  result: AllocationResult | null,
): DraftViewData {
  if (!result) {
    return { offers: items, policy_cases: [], unallocated: 0 };
  }
  const shares = new Map(result.shares.map((item) => [item.offer_id, item.share]));
  const offers = items.map((item) =>
    item.lifecycle === "ACTIVE"
      ? { ...item, share: shares.get(item.offer_id) ?? 0 }
      : item,
  );
  return {
    offers,
    policy_cases: result.policy_cases,
    unallocated: result.unallocated,
  };
}

function effectiveLifecycle(
  offerId: number,
  binding: BaselineBinding | undefined,
  archives: Set<number>,
  restores: Set<number>,
  applyIntent: boolean,
): BindingLifecycle {
  if (applyIntent) {
    if (archives.has(offerId) && !restores.has(offerId)) {
      return "ARCHIVED";
    }
    if (restores.has(offerId)) {
      return "ACTIVE";
    }
  }
  return binding ? binding.lifecycle : "ACTIVE";
}

function effectivePin(
  offerId: number,
  binding: BaselineBinding | undefined,
  pinToggles: Record<number, boolean>,
  applyIntent: boolean,
): boolean {
  if (applyIntent && Object.prototype.hasOwnProperty.call(pinToggles, offerId)) {
    return pinToggles[offerId];
  }
  return binding ? binding.is_pinned : false;
}

function effectiveShare(
  offerId: number,
  baselineOffer: RemoteOffer | undefined,
  shareEdits: Record<number, number>,
  lifecycle: BindingLifecycle,
  applyIntent: boolean,
): number {
  if (lifecycle === "ARCHIVED") {
    return 0;
  }
  if (applyIntent && Object.prototype.hasOwnProperty.call(shareEdits, offerId)) {
    return shareEdits[offerId];
  }
  return baselineOffer ? baselineOffer.share : 0;
}

function itemsFromContext(
  context: DraftContext,
  applyIntent: boolean,
): StagedOffer[] {
  const offers = validateRemoteOffers(context.baseline_offers);
  const baseline = new Map(offers.map((o) => [o.offer_id, o]));
  const bindings = new Map(
    context.baseline_bindings.map((b) => [b.offer_id, b]),
  );
  const intent = applyIntent ? context.intent : emptyIntent();
  const archives = new Set(intent.archives);
  const restores = new Set(intent.restores);

  return orderedIds(context).map((offerId, position) => {
    const binding = bindings.get(offerId);
    const lifecycle = effectiveLifecycle(
      offerId,
      binding,
      archives,
      restores,
      applyIntent,
    );
    const isPinned = effectivePin(
      offerId,
      binding,
      intent.pin_toggles,
      applyIntent,
    );
    const baselineOffer = baseline.get(offerId);
    const share = effectiveShare(
      offerId,
      baselineOffer,
      intent.share_edits,
      lifecycle,
      applyIntent,
    );
    return {
      offer_id: offerId,
      lifecycle,
      is_pinned: isPinned,
      share,
      state: baselineOffer ? baselineOffer.state : null,
      position,
    };
  });
}

function deriveView(context: DraftContext, applyIntent: boolean): DraftViewData {
  const items = itemsFromContext(context, applyIntent);
  if (!applyIntent) {
    return viewFromAllocations(items, null);
  }
  const active = items
    .filter((item) => item.lifecycle === "ACTIVE")
    .map((item) => ({
      offer_id: item.offer_id,
      is_pinned: item.is_pinned,
      share: item.share,
    }));
  return viewFromAllocations(items, allocate(active));
}

export function baselineView(context: DraftContext): DraftViewData {
  const raw = deriveView(context, false);
  const active = raw.offers
    .filter((item) => item.lifecycle === "ACTIVE")
    .map((item) => ({
      offer_id: item.offer_id,
      is_pinned: item.is_pinned,
      share: item.share,
    }));
  return viewFromAllocations(raw.offers, allocate(active));
}

export function stagedView(
  context: DraftContext,
  intent: DraftIntent,
): DraftViewData {
  return deriveView({ ...context, intent }, true);
}

function sameView(left: DraftViewData, right: DraftViewData): boolean {
  if (left.offers.length !== right.offers.length) {
    return false;
  }
  for (let i = 0; i < left.offers.length; i += 1) {
    const a = left.offers[i];
    const b = right.offers[i];
    if (
      a.offer_id !== b.offer_id ||
      a.lifecycle !== b.lifecycle ||
      a.is_pinned !== b.is_pinned ||
      a.share !== b.share ||
      a.state !== b.state ||
      a.position !== b.position
    ) {
      return false;
    }
  }
  return true;
}

export function loadBaselineBindings(
  values: Iterable<BaselineBinding>,
): BaselineBinding[] {
  return Array.from(values).sort((a, b) => a.offer_id - b.offer_id);
}

export function planEdit(
  context: DraftContext,
  command: DraftCommand,
): DraftPlan {
  const baseView = baselineView(context);
  if (command.kind === "cancel") {
    const noop = !hasIntentChanges(context.intent);
    return {
      intent: noop ? context.intent : emptyIntent(),
      view: baseView,
      noop,
    };
  }

  const offerId = command.offer_id;
  if (typeof offerId !== "number" || !Number.isInteger(offerId) || offerId <= 0) {
    throw draftInputError("offer_id must be a positive int");
  }

  const current = stagedView(context, context.intent);
  const additions = new Set(context.intent.additions);
  const archives = new Set(context.intent.archives);
  const restores = new Set(context.intent.restores);
  const pinToggles: Record<number, boolean> = { ...context.intent.pin_toggles };
  const shareEdits: Record<number, number> = { ...context.intent.share_edits };

  const target = current.offers.find((item) => item.offer_id === offerId) ?? null;
  const baselineIds = new Set(baseView.offers.map((item) => item.offer_id));
  const rawOffers = new Map(
    context.baseline_offers.map((item) => [item.offer_id, item]),
  );
  const rawBindings = new Map(
    context.baseline_bindings.map((item) => [item.offer_id, item]),
  );

  switch (command.kind) {
    case "add": {
      if (target && target.lifecycle === "ACTIVE") {
        return { intent: context.intent, view: current, noop: true };
      }
      if (baselineIds.has(offerId) && target !== null) {
        throw draftInputError(`offer ${offerId} is archived; use restore`);
      }
      additions.add(offerId);
      archives.delete(offerId);
      restores.delete(offerId);
      break;
    }
    case "remove": {
      if (!target || target.lifecycle === "ARCHIVED") {
        return { intent: context.intent, view: current, noop: true };
      }
      if (additions.has(offerId)) {
        additions.delete(offerId);
        delete pinToggles[offerId];
      } else {
        archives.add(offerId);
        restores.delete(offerId);
      }
      delete shareEdits[offerId];
      break;
    }
    case "restore": {
      if (target && target.lifecycle === "ACTIVE") {
        return { intent: context.intent, view: current, noop: true };
      }
      if (!baselineIds.has(offerId)) {
        throw draftInputError(`offer ${offerId} has no archived baseline`);
      }
      restores.add(offerId);
      archives.delete(offerId);
      break;
    }
    case "set_pin": {
      if (typeof command.value !== "boolean") {
        throw draftInputError("pin value must be a bool");
      }
      if (!target) {
        throw draftInputError(`offer ${offerId} is not in the draft baseline`);
      }
      if (target.is_pinned === command.value) {
        return { intent: context.intent, view: current, noop: true };
      }
      const binding = rawBindings.get(offerId);
      const baselinePinned = binding ? binding.is_pinned : false;
      if (baselinePinned === command.value) {
        delete pinToggles[offerId];
      } else {
        pinToggles[offerId] = command.value;
      }
      if (!command.value) {
        delete shareEdits[offerId];
      }
      break;
    }
    case "set_share": {
      const share = validateShare(command.value);
      if (!target || target.lifecycle !== "ACTIVE") {
        throw draftInputError(`offer ${offerId} is not active`);
      }
      const rawOffer = rawOffers.get(offerId);
      if (rawOffer && share === rawOffer.share) {
        delete shareEdits[offerId];
        const binding = rawBindings.get(offerId);
        if (!binding || !binding.is_pinned) {
          delete pinToggles[offerId];
        }
      } else {
        shareEdits[offerId] = share;
        pinToggles[offerId] = true;
      }
      break;
    }
    default:
      throw draftInputError(`unsupported command ${String(command.kind)}`);
  }

  const nextIntent: DraftIntent = {
    additions: Array.from(additions).sort((a, b) => a - b),
    archives: Array.from(archives).sort((a, b) => a - b),
    restores: Array.from(restores).sort((a, b) => a - b),
    pin_toggles: pinToggles,
    share_edits: shareEdits,
  };
  const staged = stagedView(context, nextIntent);
  if (sameView(staged, baseView)) {
    return { intent: emptyIntent(), view: baseView, noop: true };
  }
  return { intent: nextIntent, view: staged, noop: false };
}
