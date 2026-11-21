import crypto from "node:crypto";
import {
  AppError,
  UNRESOLVED_PUBLICATION_STATUSES,
  type BaselineBinding,
  type BindingLifecycle,
  type BindingState,
  type BindingTransition,
  type CampaignCreationOperation,
  type ConflictDetail,
  type CreationStage,
  type DraftCommandResult,
  type DraftIntent,
  type DraftView,
  type FetchOutcome,
  type OfferEntry,
  type OperationStatus,
  type PublicationStatus,
  type PublicPublicationView,
  type RemoteOffer,
  type RemoteSnapshotView,
  type StagedOffer,
  type StepState,
  type StreamRefView,
  type AcceptRemoteResult,
  type CommittedStateView,
} from "./types.js";
import {
  baselineView,
  emptyIntent,
  hasIntentChanges,
  intentToJson,
  loadBaselineBindings,
  planEdit,
  stagedView,
  type DraftCommand,
  type DraftContext,
  type DraftViewData,
} from "./domain/draft.js";
import {
  conflictFingerprint,
  planReconciliation,
  validateRemoteOffers,
} from "./domain/reconciliation.js";
import { canTransition } from "./domain/publication.js";

export interface FetchedStream {
  stream_ref: number;
  campaign_id: number;
  type: string;
  name: string;
  schema: string;
  offers: RemoteOffer[];
  comments: string | null;
}

export interface FetchSnapshot {
  campaign_id: number;
  streams: FetchedStream[];
  captured_at: string;
}

export interface DirtyDraftView {
  stream_ref: number;
  version: number;
  intent: Record<string, unknown>;
  is_dirty: boolean;
  is_stale: boolean;
}

export interface PublicationPlan {
  campaign_id: number;
  stream_ref: number;
  draft_version: number;
  intent: Record<string, unknown>;
  staged_offers: StagedOffer[];
  payload: Record<string, unknown>;
  expected_entries: OfferEntry[];
  preimage: Record<string, unknown>;
  correlation_marker: string;
}

export interface PublicationOperationRecord {
  id: number;
  operation_uuid: string;
  campaign_id: number;
  stream_ref: number;
  stream_ref_id: number;
  status: PublicationStatus;
  draft_version: number;
  intent: Record<string, unknown>;
  request_payload: Record<string, unknown>;
  remote_baseline_before: Record<string, unknown>;
  remote_baseline_after: RemoteOffer[] | null;
  effective_entries: OfferEntry[];
  staged_offers: StagedOffer[];
  attempt_count: number;
  worker_token: string | null;
  error: string | null;
  evidence: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
}

interface StreamRefRecord extends StreamRefView {
  id: number;
  draft_version_counter: number;
}

interface DraftRecord {
  stream_ref_id: number;
  baseline_offers: RemoteOffer[];
  baseline_bindings: BaselineBinding[];
  baseline_captured_at: string | null;
  intent: DraftIntent;
  version: number;
  is_dirty: boolean;
  is_stale: boolean;
  conflicts: ConflictDetail[];
}

export class InMemoryStore {
  private nextOpId = 1;
  private nextStreamInternalId = 1;
  private nextPubId = 1;

  private readonly operationsByUuid = new Map<string, CampaignCreationOperation>();
  private readonly operationsByAlias = new Map<string, CampaignCreationOperation>();

  private readonly streamRefsByRemoteId = new Map<number, StreamRefRecord>();
  private readonly streamRefsByInternalId = new Map<number, StreamRefRecord>();
  private readonly bindingsByStreamId = new Map<number, Map<number, BindingState>>();
  private readonly snapshotsByStreamId = new Map<number, RemoteSnapshotView>();
  private readonly draftsByStreamId = new Map<number, DraftRecord>();
  private readonly publicationsById = new Map<number, PublicationOperationRecord>();

  // ---- Campaign Creation Operations ----

  getOperationByUuid(uuid: string): CampaignCreationOperation | null {
    const op = this.operationsByUuid.get(uuid);
    return op ? { ...op } : null;
  }

  getOrCreateOperation(
    operation: Omit<CampaignCreationOperation, "id" | "created_at" | "updated_at">,
  ): { operation: CampaignCreationOperation; created: boolean } {
    const existing = this.operationsByUuid.get(operation.operation_uuid);
    if (existing) {
      return { operation: { ...existing }, created: false };
    }
    if (this.operationsByAlias.has(operation.alias)) {
      throw new AppError(
        409,
        "IDEMPOTENCY_KEY_CONFLICT",
        `operation alias ${operation.alias} is already reserved`,
      );
    }
    const now = new Date().toISOString();
    const record: CampaignCreationOperation = {
      ...operation,
      id: this.nextOpId++,
      created_at: now,
      updated_at: now,
    };
    this.operationsByUuid.set(record.operation_uuid, record);
    this.operationsByAlias.set(record.alias, record);
    return { operation: { ...record }, created: true };
  }

  saveOperation(operation: CampaignCreationOperation): CampaignCreationOperation {
    const existing = this.operationsByUuid.get(operation.operation_uuid);
    if (!existing) {
      throw new Error(`unknown operation ${operation.operation_uuid}`);
    }
    const terminal = new Set<OperationStatus>(["created", "failed", "abandoned"]);
    if (terminal.has(existing.status) && existing.status !== operation.status) {
      return { ...existing };
    }
    const updated: CampaignCreationOperation = {
      ...existing,
      status: operation.status,
      keitaro_campaign_id: operation.keitaro_campaign_id,
      flow_country_id: operation.flow_country_id,
      flow_offer_id: operation.flow_offer_id,
      error_message: operation.error_message,
      updated_at: new Date().toISOString(),
    };
    this.operationsByUuid.set(updated.operation_uuid, updated);
    this.operationsByAlias.set(updated.alias, updated);
    return { ...updated };
  }

  claimStep(operationUuid: string, stage: CreationStage): CampaignCreationOperation | null {
    const op = this.operationsByUuid.get(operationUuid);
    if (!op) return null;
    const field = stepField(stage);
    if (op[field] !== "not_started") {
      return null;
    }
    op[field] = "in_flight";
    op.updated_at = new Date().toISOString();
    return { ...op };
  }

  confirmStep(
    operationUuid: string,
    stage: CreationStage,
    remoteId: number | null,
    status: OperationStatus | null,
    error: string | null,
  ): CampaignCreationOperation | null {
    const op = this.operationsByUuid.get(operationUuid);
    if (!op) return null;
    const stepCol = stepField(stage);
    const idCol = stepIdField(stage);
    if (op[stepCol] === "confirmed") {
      return op[idCol] === remoteId ? { ...op } : null;
    }
    if (op[stepCol] !== "in_flight" && op[stepCol] !== "ambiguous") {
      return null;
    }
    op[stepCol] = "confirmed";
    op[idCol] = remoteId;
    if (status !== null) op.status = status;
    if (error !== null) op.error_message = error;
    op.updated_at = new Date().toISOString();
    return { ...op };
  }

  markStepAmbiguous(
    operationUuid: string,
    stage: CreationStage,
    status: OperationStatus | null,
    error: string | null,
  ): CampaignCreationOperation | null {
    const op = this.operationsByUuid.get(operationUuid);
    if (!op) return null;
    const stepCol = stepField(stage);
    if (op[stepCol] !== "in_flight" && op[stepCol] !== "ambiguous") {
      return null;
    }
    op[stepCol] = "ambiguous";
    if (status !== null) op.status = status;
    if (error !== null) op.error_message = error;
    op.updated_at = new Date().toISOString();
    return { ...op };
  }

  // ---- Streams & Fetch Reconciliation ----

  reconcile(snapshot: FetchSnapshot): FetchOutcome {
    const counts = {
      created_bindings: 0,
      archived_bindings: 0,
      reactivated_bindings: 0,
      share_updates: 0,
      conflicted_drafts: 0,
    };
    let reconciled = 0;

    for (const stream of snapshot.streams) {
      const ref = this.getOrCreateStreamRef(snapshot.campaign_id, stream);
      const bindingMap = this.getBindingMap(ref.id);
      const cached = this.snapshotsByStreamId.get(ref.id) ?? null;
      const draft = this.draftsByStreamId.get(ref.id) ?? null;

      if (
        stream.offers.length === 0 &&
        bindingMap.size === 0 &&
        cached === null &&
        draft === null
      ) {
        continue;
      }

      const remote = validateRemoteOffers(stream.offers);
      const plan = planReconciliation({
        stream_ref: stream.stream_ref,
        remote_offers: remote,
        previous_bindings: bindingMap.values(),
        draft: draft
          ? {
              baseline_offers: draft.baseline_offers,
              staged_changes: intentToJson(draft.intent),
              is_stale: draft.is_stale,
              conflicts: draft.conflicts,
            }
          : null,
      });

      this.applyTransitions(ref.id, plan.transitions, counts);
      this.snapshotsByStreamId.set(ref.id, {
        offers: plan.remote_offers,
        captured_at: snapshot.captured_at,
      });

      if (draft && plan.conflicts.length > 0) {
        const prevSerialized = JSON.stringify(draft.conflicts);
        const nextSerialized = JSON.stringify(plan.conflicts);
        const isNewMarker = !draft.is_stale || prevSerialized !== nextSerialized;
        draft.is_stale = true;
        draft.conflicts = plan.conflicts;
        if (isNewMarker) {
          counts.conflicted_drafts += 1;
        }
      }
      reconciled += 1;
    }

    return {
      campaign_id: snapshot.campaign_id,
      stream_count: snapshot.streams.length,
      reconciled_stream_count: reconciled,
      ...counts,
    };
  }

  listStreamsByCampaign(campaignId: number): StreamRefView[] {
    return Array.from(this.streamRefsByRemoteId.values())
      .filter((ref) => ref.campaign_id === campaignId)
      .sort((a, b) => a.stream_ref - b.stream_ref)
      .map((ref) => ({
        stream_ref: ref.stream_ref,
        campaign_id: ref.campaign_id,
        type: ref.type,
        name: ref.name,
        schema: ref.schema,
        comments: ref.comments,
      }));
  }

  getStreamRef(streamRef: number): StreamRefView | null {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref) return null;
    return {
      stream_ref: ref.stream_ref,
      campaign_id: ref.campaign_id,
      type: ref.type,
      name: ref.name,
      schema: ref.schema,
      comments: ref.comments,
    };
  }

  getStreamCampaign(streamRef: number): number | null {
    return this.streamRefsByRemoteId.get(streamRef)?.campaign_id ?? null;
  }

  getCommittedState(campaignId: number, streamRef: number): CommittedStateView {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref || ref.campaign_id !== campaignId) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${streamRef} not found`, {
        campaign_id: campaignId,
        stream_ref: streamRef,
      });
    }
    const bindings = Array.from(this.getBindingMap(ref.id).values())
      .sort((a, b) => a.offer_id - b.offer_id)
      .map((b) => ({ ...b }));
    const snapshot = this.snapshotsByStreamId.get(ref.id) ?? null;
    return {
      stream_ref: streamRef,
      bindings,
      snapshot: snapshot
        ? {
            offers: snapshot.offers.map((o) => ({ ...o })),
            captured_at: snapshot.captured_at,
          }
        : null,
    };
  }

  getRemoteSnapshot(streamRef: number): RemoteSnapshotView | null {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref) return null;
    const snapshot = this.snapshotsByStreamId.get(ref.id);
    if (!snapshot) return null;
    return {
      offers: snapshot.offers.map((o) => ({ ...o })),
      captured_at: snapshot.captured_at,
    };
  }

  // ---- Drafts ----

  getDraftView(streamRef: number): DraftView | null {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref) return null;
    const draft = this.draftsByStreamId.get(ref.id);
    if (!draft) return null;

    const context: DraftContext = {
      stream_ref: streamRef,
      baseline_offers: draft.baseline_offers,
      baseline_bindings: draft.baseline_bindings,
      intent: draft.intent,
    };
    const data = hasIntentChanges(draft.intent)
      ? stagedView(context, draft.intent)
      : baselineView(context);
    return buildDraftView(
      streamRef,
      context,
      data,
      draft.intent,
      draft.version,
      draft.is_stale,
      draft.conflicts,
    );
  }

  applyDraftCommand(
    campaignId: number,
    streamRef: number,
    expectedVersion: number,
    command: DraftCommand,
  ): DraftCommandResult {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref || ref.campaign_id !== campaignId) {
      throw new AppError(
        422,
        "DOMAIN_POLICY_REJECTION",
        "stream does not belong to the requested campaign",
        { reason: "stream does not belong to the requested campaign" },
      );
    }
    if (this.hasUnresolvedPublication(ref.id)) {
      throw new AppError(
        409,
        "PUBLICATION_UNRESOLVED",
        `stream ${streamRef} has an unresolved publication`,
      );
    }

    const cached = this.snapshotsByStreamId.get(ref.id) ?? null;
    const draft = this.draftsByStreamId.get(ref.id) ?? null;

    if (draft && draft.is_stale && command.kind !== "cancel") {
      throw new AppError(
        409,
        "DRAFT_STALE_CONFLICT",
        `draft for stream ${streamRef} is stale; resolve conflict first`,
        { stream_ref: streamRef, conflicts: draft.conflicts },
      );
    }

    let baselineOffers: RemoteOffer[];
    let baselineBindings: BaselineBinding[];
    let baselineCapturedAt: string | null;
    let currentIntent: DraftIntent;
    let targetVersion: number;

    if (!draft) {
      if (!cached || cached.offers.length === 0) {
        throw new AppError(
          422,
          "DOMAIN_POLICY_REJECTION",
          "stream has no remote offer snapshot to baseline",
          { reason: "stream has no remote offer snapshot to baseline" },
        );
      }
      if (expectedVersion !== 0) {
        throw new AppError(
          409,
          "DRAFT_VERSION_CONFLICT",
          "a new draft requires expected_version 0",
          {
            stream_ref: streamRef,
            expected_version: expectedVersion,
            current_version: 0,
          },
        );
      }
      baselineOffers = cached.offers.map((o) => ({ ...o }));
      baselineBindings = loadBaselineBindings(
        Array.from(this.getBindingMap(ref.id).values()).map((b) => ({
          offer_id: b.offer_id,
          lifecycle: b.lifecycle,
          is_pinned: b.is_pinned,
        })),
      );
      baselineCapturedAt = cached.captured_at;
      currentIntent = emptyIntent();
      targetVersion = ref.draft_version_counter + 1;
    } else {
      if (expectedVersion !== draft.version) {
        throw new AppError(409, "DRAFT_VERSION_CONFLICT", "draft version changed", {
          stream_ref: streamRef,
          expected_version: expectedVersion,
          current_version: draft.version,
        });
      }
      baselineOffers = draft.baseline_offers;
      baselineBindings = draft.baseline_bindings;
      baselineCapturedAt = draft.baseline_captured_at;
      currentIntent = draft.intent;
      targetVersion = draft.version;
    }

    const planContext: DraftContext = {
      stream_ref: streamRef,
      baseline_offers: baselineOffers,
      baseline_bindings: baselineBindings,
      intent: currentIntent,
    };

    const plan = planEdit(planContext, command);
    const conflicts = draft ? draft.conflicts : [];
    const isStale = draft ? draft.is_stale : false;

    const view = buildDraftView(
      streamRef,
      planContext,
      plan.view,
      plan.intent,
      targetVersion,
      isStale,
      conflicts,
    );

    if (plan.noop) {
      let deleted = false;
      if (
        draft &&
        JSON.stringify(intentToJson(plan.intent)) !==
          JSON.stringify(intentToJson(currentIntent))
      ) {
        if (!hasIntentChanges(plan.intent) && !isStale && conflicts.length === 0) {
          this.draftsByStreamId.delete(ref.id);
          deleted = true;
        } else {
          draft.intent = plan.intent;
          draft.is_dirty = false;
        }
      }
      return {
        view: deleted ? { ...view, version: 0 } : view,
        created: false,
        noop: true,
      };
    }

    const isClean = !isStale && conflicts.length === 0;
    if (draft && !hasIntentChanges(plan.intent) && isClean) {
      ref.draft_version_counter = Math.max(
        ref.draft_version_counter,
        expectedVersion,
      );
      this.draftsByStreamId.delete(ref.id);
      return {
        view: { ...view, version: 0 },
        created: false,
        noop: false,
      };
    }

    if (!draft) {
      this.draftsByStreamId.set(ref.id, {
        stream_ref_id: ref.id,
        baseline_offers: baselineOffers,
        baseline_bindings: baselineBindings,
        baseline_captured_at: baselineCapturedAt,
        intent: plan.intent,
        version: targetVersion,
        is_dirty: hasIntentChanges(plan.intent),
        is_stale: false,
        conflicts: [],
      });
      ref.draft_version_counter = targetVersion;
      return { view: { ...view, version: targetVersion }, created: true, noop: false };
    }

    const nextVersion = expectedVersion + 1;
    draft.intent = plan.intent;
    draft.version = nextVersion;
    draft.is_dirty = hasIntentChanges(plan.intent);
    ref.draft_version_counter = Math.max(ref.draft_version_counter, nextVersion);
    return { view: { ...view, version: nextVersion }, created: false, noop: false };
  }

  acceptRemote(
    campaignId: number,
    streamRef: number,
    expectedVersion: number,
    expectedFingerprint: string,
    observation: FetchedStream,
    capturedAt: string,
  ): AcceptRemoteResult {
    const ref = this.streamRefsByRemoteId.get(streamRef);
    if (!ref || ref.campaign_id !== campaignId) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${streamRef} not found`, {
        campaign_id: campaignId,
        stream_ref: streamRef,
      });
    }
    const draft = this.draftsByStreamId.get(ref.id);
    if (!draft) {
      throw new AppError(
        422,
        "DOMAIN_POLICY_REJECTION",
        "no conflicted draft exists for this stream",
        { reason: "no conflicted draft exists for this stream" },
      );
    }
    const currentFingerprint = conflictFingerprint(draft.conflicts);
    if (currentFingerprint !== expectedFingerprint) {
      throw new AppError(
        409,
        "DRAFT_CONFLICT_CHANGED",
        "conflict details changed",
        {
          stream_ref: streamRef,
          expected: expectedFingerprint,
          observed: currentFingerprint,
        },
      );
    }
    if (draft.version !== expectedVersion) {
      throw new AppError(409, "DRAFT_VERSION_CONFLICT", "draft version changed", {
        stream_ref: streamRef,
        expected_version: expectedVersion,
        current_version: draft.version,
      });
    }
    if (this.hasUnresolvedPublication(ref.id)) {
      throw new AppError(
        409,
        "PUBLICATION_UNRESOLVED",
        `stream ${streamRef} has an unresolved publication`,
      );
    }
    const cached = this.snapshotsByStreamId.get(ref.id);
    if (cached && cached.captured_at > capturedAt) {
      throw new AppError(
        409,
        "DRAFT_CONFLICT_CHANGED",
        "remote snapshot advanced",
        {
          stream_ref: streamRef,
          expected: expectedFingerprint,
          observed: currentFingerprint,
        },
      );
    }

    const bindingMap = this.getBindingMap(ref.id);
    const plan = planReconciliation({
      stream_ref: streamRef,
      remote_offers: observation.offers,
      previous_bindings: bindingMap.values(),
    });

    const counts = {
      created_bindings: 0,
      archived_bindings: 0,
      reactivated_bindings: 0,
      share_updates: 0,
    };
    this.applyTransitions(ref.id, plan.transitions, counts);
    this.snapshotsByStreamId.set(ref.id, {
      offers: plan.remote_offers,
      captured_at: capturedAt,
    });

    const baselineBindings = loadBaselineBindings(
      Array.from(this.getBindingMap(ref.id).values()).map((b) => ({
        offer_id: b.offer_id,
        lifecycle: b.lifecycle,
        is_pinned: b.is_pinned,
      })),
    );
    const newVersion = expectedVersion + 1;
    draft.baseline_offers = observation.offers.map((o) => ({ ...o }));
    draft.baseline_bindings = baselineBindings;
    draft.baseline_captured_at = capturedAt;
    draft.intent = emptyIntent();
    draft.is_dirty = false;
    draft.is_stale = false;
    draft.conflicts = [];
    draft.version = newVersion;
    ref.draft_version_counter = Math.max(ref.draft_version_counter, newVersion);

    const context: DraftContext = {
      stream_ref: streamRef,
      baseline_offers: draft.baseline_offers,
      baseline_bindings: baselineBindings,
      intent: emptyIntent(),
    };
    const view = buildDraftView(
      streamRef,
      context,
      baselineView(context),
      emptyIntent(),
      newVersion,
      false,
      [],
    );
    return { stream_ref: streamRef, draft: view, noop: false, version: newVersion };
  }

  listDirtyDrafts(campaignId: number): DirtyDraftView[] {
    const result: DirtyDraftView[] = [];
    for (const draft of this.draftsByStreamId.values()) {
      if (!draft.is_dirty) continue;
      const ref = this.streamRefsByInternalId.get(draft.stream_ref_id);
      if (!ref || ref.campaign_id !== campaignId) continue;
      result.push({
        stream_ref: ref.stream_ref,
        version: draft.version,
        intent: intentToJson(draft.intent),
        is_dirty: draft.is_dirty,
        is_stale: draft.is_stale,
      });
    }
    return result.sort((a, b) => a.stream_ref - b.stream_ref);
  }

  // ---- Publications ----

  getLatestPublication(
    campaignId: number,
    streamRef: number,
  ): PublicationOperationRecord | null {
    const matches = Array.from(this.publicationsById.values())
      .filter((p) => p.campaign_id === campaignId && p.stream_ref === streamRef)
      .sort((a, b) => b.id - a.id);
    return matches[0] ? { ...matches[0] } : null;
  }

  getUnresolvedPublication(
    campaignId: number,
    streamRef: number,
  ): PublicationOperationRecord | null {
    const matches = Array.from(this.publicationsById.values())
      .filter(
        (p) =>
          p.campaign_id === campaignId &&
          p.stream_ref === streamRef &&
          UNRESOLVED_PUBLICATION_STATUSES.has(p.status),
      )
      .sort((a, b) => b.id - a.id);
    return matches[0] ? { ...matches[0] } : null;
  }

  listRecoverablePublications(limit = 100): PublicationOperationRecord[] {
    return Array.from(this.publicationsById.values())
      .filter((p) => UNRESOLVED_PUBLICATION_STATUSES.has(p.status))
      .sort((a, b) => a.id - b.id)
      .slice(0, limit)
      .map((p) => ({ ...p }));
  }

  createPendingPublications(plans: PublicationPlan[]): PublicationOperationRecord[] {
    const created: PublicationOperationRecord[] = [];
    const now = new Date().toISOString();

    for (const plan of plans) {
      const ref = this.streamRefsByRemoteId.get(plan.stream_ref);
      if (!ref || ref.campaign_id !== plan.campaign_id) {
        throw new AppError(404, "STREAM_NOT_FOUND", `stream ${plan.stream_ref} not found`);
      }
      if (this.hasUnresolvedPublication(ref.id)) {
        throw new AppError(
          409,
          "PUBLICATION_UNRESOLVED",
          `stream ${plan.stream_ref} has an unresolved publication`,
        );
      }
      const draft = this.draftsByStreamId.get(ref.id);
      if (
        !draft ||
        draft.version !== plan.draft_version ||
        draft.is_stale ||
        !draft.is_dirty
      ) {
        throw new AppError(
          409,
          "PUBLICATION_CONCURRENT_CHANGE",
          "draft changed before publication creation",
        );
      }
    }

    for (const plan of plans) {
      const ref = this.streamRefsByRemoteId.get(plan.stream_ref)!;
      const op: PublicationOperationRecord = {
        id: this.nextPubId++,
        operation_uuid: crypto.randomUUID(),
        campaign_id: plan.campaign_id,
        stream_ref: plan.stream_ref,
        stream_ref_id: ref.id,
        status: "pending",
        draft_version: plan.draft_version,
        intent: plan.intent,
        request_payload: plan.payload,
        remote_baseline_before: plan.preimage,
        remote_baseline_after: null,
        effective_entries: plan.expected_entries,
        staged_offers: plan.staged_offers,
        attempt_count: 0,
        worker_token: null,
        error: null,
        evidence: null,
        created_at: now,
        updated_at: now,
      };
      this.publicationsById.set(op.id, op);
      created.push({ ...op });
    }
    return created;
  }

  claimPublication(
    operationId: number,
    workerToken: string,
  ): PublicationOperationRecord | null {
    const op = this.publicationsById.get(operationId);
    if (!op || op.status !== "pending") return null;
    if (!canTransition(op.status, "in_progress")) return null;
    op.status = "in_progress";
    op.attempt_count += 1;
    op.worker_token = workerToken;
    op.updated_at = new Date().toISOString();
    return { ...op };
  }

  markPublicationAmbiguous(operationId: number, error: string): void {
    const op = this.publicationsById.get(operationId);
    if (!op) return;
    if (op.status === "in_progress" || op.status === "ambiguous") {
      op.status = "ambiguous";
      op.error = error;
      op.updated_at = new Date().toISOString();
    }
  }

  finalizePublicationSuccess(
    operationId: number,
    observed: RemoteOffer[],
  ): PublicationOperationRecord | null {
    const op = this.publicationsById.get(operationId);
    if (!op || (op.status !== "in_progress" && op.status !== "ambiguous")) {
      return null;
    }
    const ref = this.streamRefsByInternalId.get(op.stream_ref_id);
    if (!ref) return null;
    const draft = this.draftsByStreamId.get(ref.id);
    if (!draft || draft.version !== op.draft_version) {
      return null;
    }

    op.status = "succeeded";
    op.remote_baseline_after = observed;
    op.error = null;
    op.updated_at = new Date().toISOString();

    const bindingMap = this.getBindingMap(ref.id);
    for (const staged of op.staged_offers) {
      const existing = bindingMap.get(staged.offer_id);
      if (staged.lifecycle === "ACTIVE") {
        bindingMap.set(staged.offer_id, {
          offer_id: staged.offer_id,
          lifecycle: "ACTIVE",
          is_pinned: staged.is_pinned,
          last_remote_share: staged.share,
        });
      } else if (existing) {
        bindingMap.set(staged.offer_id, {
          ...existing,
          lifecycle: "ARCHIVED",
          is_pinned: staged.is_pinned,
          last_remote_share: null,
        });
      }
    }

    this.snapshotsByStreamId.set(ref.id, {
      offers: observed.map((o) => ({ ...o })),
      captured_at: new Date().toISOString(),
    });
    this.draftsByStreamId.delete(ref.id);
    return { ...op };
  }

  cancelPublication(
    campaignId: number,
    streamRef: number,
    reason: string,
  ): { operation: PublicationOperationRecord; draft_discarded: boolean } | null {
    const latest = this.getLatestPublication(campaignId, streamRef);
    if (!latest) return null;
    const op = this.publicationsById.get(latest.id);
    if (!op) return null;
    if (op.status !== "pending" && op.status !== "failed") {
      return null;
    }
    if (!canTransition(op.status, "abandoned")) {
      return null;
    }
    op.status = "abandoned";
    op.error = reason;
    op.updated_at = new Date().toISOString();

    const ref = this.streamRefsByRemoteId.get(streamRef);
    let draftDiscarded = false;
    if (ref) {
      const draft = this.draftsByStreamId.get(ref.id);
      if (draft && !draft.is_stale && draft.conflicts.length === 0) {
        this.draftsByStreamId.delete(ref.id);
        draftDiscarded = true;
      }
    }
    return { operation: { ...op }, draft_discarded: draftDiscarded };
  }

  // ---- Internal helpers ----

  private hasUnresolvedPublication(streamRefInternalId: number): boolean {
    for (const pub of this.publicationsById.values()) {
      if (
        pub.stream_ref_id === streamRefInternalId &&
        UNRESOLVED_PUBLICATION_STATUSES.has(pub.status)
      ) {
        return true;
      }
    }
    return false;
  }

  private getOrCreateStreamRef(
    campaignId: number,
    stream: FetchedStream,
  ): StreamRefRecord {
    const existing = this.streamRefsByRemoteId.get(stream.stream_ref);
    if (existing) {
      existing.campaign_id = campaignId;
      existing.type = stream.type;
      existing.name = stream.name;
      existing.schema = stream.schema;
      existing.comments = stream.comments;
      return existing;
    }
    const ref: StreamRefRecord = {
      id: this.nextStreamInternalId++,
      stream_ref: stream.stream_ref,
      campaign_id: campaignId,
      type: stream.type,
      name: stream.name,
      schema: stream.schema,
      comments: stream.comments,
      draft_version_counter: 0,
    };
    this.streamRefsByRemoteId.set(ref.stream_ref, ref);
    this.streamRefsByInternalId.set(ref.id, ref);
    return ref;
  }

  private getBindingMap(streamRefInternalId: number): Map<number, BindingState> {
    let map = this.bindingsByStreamId.get(streamRefInternalId);
    if (!map) {
      map = new Map<number, BindingState>();
      this.bindingsByStreamId.set(streamRefInternalId, map);
    }
    return map;
  }

  private applyTransitions(
    streamRefInternalId: number,
    transitions: BindingTransition[],
    counts: {
      created_bindings: number;
      archived_bindings: number;
      reactivated_bindings: number;
      share_updates: number;
    },
  ): void {
    const bindingMap = this.getBindingMap(streamRefInternalId);
    for (const transition of transitions) {
      if (transition.kind === "CREATED") {
        bindingMap.set(transition.offer_id, {
          offer_id: transition.offer_id,
          lifecycle: "ACTIVE",
          is_pinned: false,
          last_remote_share: transition.share,
        });
        counts.created_bindings += 1;
        continue;
      }
      const existing = bindingMap.get(transition.offer_id);
      if (!existing) {
        throw new Error(
          `missing binding for offer ${transition.offer_id} in stream ${streamRefInternalId}`,
        );
      }
      if (transition.kind === "ARCHIVED") {
        bindingMap.set(transition.offer_id, {
          ...existing,
          lifecycle: "ARCHIVED",
          last_remote_share: null,
        });
        counts.archived_bindings += 1;
      } else if (transition.kind === "REACTIVATED") {
        bindingMap.set(transition.offer_id, {
          ...existing,
          lifecycle: "ACTIVE",
          last_remote_share: transition.share,
        });
        counts.reactivated_bindings += 1;
      } else if (transition.kind === "SHARE_CHANGED") {
        bindingMap.set(transition.offer_id, {
          ...existing,
          last_remote_share: transition.share,
        });
        counts.share_updates += 1;
      }
    }
  }
}

function buildDraftView(
  streamRef: number,
  context: DraftContext,
  data: DraftViewData,
  staged: DraftIntent,
  version: number,
  isStale: boolean,
  conflicts: ConflictDetail[],
): DraftView {
  return {
    stream_ref: streamRef,
    offers: data.offers,
    baseline_offers: context.baseline_offers,
    staged_changes: intentToJson(staged),
    version,
    is_dirty: hasIntentChanges(staged),
    is_stale: isStale,
    conflicts,
    conflicts_fingerprint: conflictFingerprint(conflicts),
    policy_cases: data.policy_cases,
    unallocated: data.unallocated,
  };
}

function stepField(
  stage: CreationStage,
): "campaign_step" | "country_flow_step" | "offer_flow_step" {
  switch (stage) {
    case "campaign":
      return "campaign_step";
    case "country_flow":
      return "country_flow_step";
    case "offer_flow":
      return "offer_flow_step";
  }
}

function stepIdField(
  stage: CreationStage,
): "keitaro_campaign_id" | "flow_country_id" | "flow_offer_id" {
  switch (stage) {
    case "campaign":
      return "keitaro_campaign_id";
    case "country_flow":
      return "flow_country_id";
    case "offer_flow":
      return "flow_offer_id";
  }
}

export function toPublicPublicationView(
  op: PublicationOperationRecord,
): PublicPublicationView {
  return {
    operation_uuid: op.operation_uuid,
    campaign_id: op.campaign_id,
    stream_ref: op.stream_ref,
    status: op.status,
    draft_version: op.draft_version,
    effective_entries: op.effective_entries,
    attempt_count: op.attempt_count,
  };
}

export function normalizeLifecycle(value: string): BindingLifecycle {
  return value === "ARCHIVED" ? "ARCHIVED" : "ACTIVE";
}

export function normalizeStepState(value: string): StepState {
  return value as StepState;
}
