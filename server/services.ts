import crypto from "node:crypto";
import {
  AppError,
  UpstreamError,
  type AcceptRemoteResult,
  type CampaignCreationOperation,
  type CampaignCreationResult,
  type CommittedStateView,
  type CreationStage,
  type DraftCommandResult,
  type DraftView,
  type FetchOutcome,
  type OfferEntry,
  type OperationStatus,
  type PublicPublicationView,
  type PushResult,
  type PushStreamOutcome,
  type RecoveryResult,
  type RemoteOffer,
  type StreamRefView,
  type CancelResult,
} from "./types.js";
import type { KeitaroClientPort, KeitaroStream } from "./keitaroClient.js";
import {
  InMemoryStore,
  toPublicPublicationView,
  type DirtyDraftView,
  type FetchedStream,
  type PublicationOperationRecord,
  type PublicationPlan,
} from "./store.js";
import { validateRemoteOffers } from "./domain/reconciliation.js";
import {
  buildOfferEntries,
  entriesMatch,
  evaluateEligibility,
} from "./domain/publication.js";

function makeAlias(operationUuid: string): string {
  return `slug-${operationUuid.replace(/-/g, "").slice(0, 8)}`;
}

function isTerminalOpStatus(status: OperationStatus): boolean {
  return status === "created" || status === "failed" || status === "abandoned";
}

function isDefinitiveRejection(status: number | null): boolean {
  return status !== null && status >= 400 && status < 500 && status !== 408 && status !== 429;
}

function upstreamStatus(err: unknown): number | null {
  if (err instanceof UpstreamError) return err.upstreamStatus;
  return null;
}

function parseOptionalInt(raw: string | undefined): number | null {
  if (!raw || raw.trim() === "") return null;
  const n = Number(raw);
  return Number.isInteger(n) ? n : null;
}

export class CampaignCreationService {
  constructor(
    private readonly client: KeitaroClientPort,
    private readonly store: InMemoryStore,
  ) {}

  private getConfig(): {
    domain_id: number | null;
    group_id: number | null;
    traffic_source_id: number | null;
    redirect_url: string;
  } {
    const fallback = this.client.isLive ? null : 1;
    return {
      domain_id: parseOptionalInt(process.env.KEITARO_DEFAULT_DOMAIN_ID) ?? fallback,
      group_id: parseOptionalInt(process.env.KEITARO_DEFAULT_GROUP_ID) ?? fallback,
      traffic_source_id:
        parseOptionalInt(process.env.KEITARO_DEFAULT_TRAFFIC_SOURCE_ID) ?? fallback,
      redirect_url:
        (process.env.KEITARO_REDIRECT_URL ?? "").trim() || "https://www.google.com",
    };
  }

  async createIdempotent(
    input: { name: string; country_code: string; offer_id: number },
    operationUuid: string,
  ): Promise<{ result: CampaignCreationResult; created: boolean }> {
    const fingerprint = [
      input.name.trim(),
      input.country_code.trim().toUpperCase(),
      input.offer_id,
    ] as const;

    const existing = this.store.getOperationByUuid(operationUuid);
    if (existing) {
      if (
        existing.name !== fingerprint[0] ||
        existing.country_code !== fingerprint[1] ||
        existing.offer_id !== fingerprint[2]
      ) {
        throw new AppError(
          409,
          "IDEMPOTENCY_KEY_REUSED",
          `idempotency key ${operationUuid} was reused`,
          { operation_uuid: operationUuid },
        );
      }
      return { result: this.toResult(existing), created: false };
    }

    const name = input.name.trim();
    if (!name) {
      throw new AppError(422, "CAMPAIGN_INPUT_REJECTED", "campaign name must not be empty", {
        reason: "campaign name must not be empty",
      });
    }
    const countryCode = input.country_code.trim().toUpperCase();
    if (!/^[A-Z]{2}$/.test(countryCode)) {
      throw new AppError(422, "CAMPAIGN_INPUT_REJECTED", "country_code must be an ISO-2 code", {
        reason: "country_code must be an ISO-2 code",
      });
    }
    if (!Number.isInteger(input.offer_id) || input.offer_id <= 0) {
      throw new AppError(422, "CAMPAIGN_INPUT_REJECTED", "offer_id must be positive", {
        reason: "offer_id must be positive",
      });
    }

    const config = this.getConfig();
    if (config.domain_id === null) {
      throw new AppError(409, "CAMPAIGN_CONFIGURATION_INVALID", "domain ID is not configured", {
        reason: "domain ID is not configured",
      });
    }
    if (config.group_id === null) {
      throw new AppError(
        409,
        "CAMPAIGN_CONFIGURATION_INVALID",
        "campaign group ID is not configured",
        { reason: "campaign group ID is not configured" },
      );
    }
    if (config.traffic_source_id === null) {
      throw new AppError(
        409,
        "CAMPAIGN_CONFIGURATION_INVALID",
        "traffic source ID is not configured",
        { reason: "traffic source ID is not configured" },
      );
    }

    try {
      await this.client.getOffer(input.offer_id);
    } catch (err) {
      if (upstreamStatus(err) === 404) {
        throw new AppError(
          422,
          "CAMPAIGN_INPUT_REJECTED",
          `offer ${input.offer_id} does not exist`,
          { reason: `offer ${input.offer_id} does not exist` },
        );
      }
      throw err;
    }

    const [domains, groups, sources] = await Promise.all([
      this.client.listDomains(),
      this.client.listGroups("campaigns"),
      this.client.listTrafficSources(),
    ]);
    this.requireResource(domains, config.domain_id, "domain");
    this.requireResource(groups, config.group_id, "campaign group");
    this.requireResource(sources, config.traffic_source_id, "traffic source");

    const { operation, created } = this.store.getOrCreateOperation({
      operation_uuid: operationUuid,
      alias: makeAlias(operationUuid),
      name,
      country_code: countryCode,
      offer_id: input.offer_id,
      domain_id: config.domain_id,
      group_id: config.group_id,
      traffic_source_id: config.traffic_source_id,
      redirect_url: config.redirect_url,
      status: "started",
      keitaro_campaign_id: null,
      flow_country_id: null,
      flow_offer_id: null,
      error_message: null,
      campaign_step: "not_started",
      country_flow_step: "not_started",
      offer_flow_step: "not_started",
    });

    if (!created) {
      if (
        operation.name !== fingerprint[0] ||
        operation.country_code !== fingerprint[1] ||
        operation.offer_id !== fingerprint[2]
      ) {
        throw new AppError(
          409,
          "IDEMPOTENCY_KEY_REUSED",
          `idempotency key ${operationUuid} was reused`,
          { operation_uuid: operationUuid },
        );
      }
      return { result: this.toResult(operation), created: false };
    }

    const result = await this.execute(operation);
    return { result, created: true };
  }

  async get(operationUuid: string): Promise<CampaignCreationResult> {
    const operation = this.store.getOperationByUuid(operationUuid);
    if (!operation) {
      throw new AppError(404, "OPERATION_NOT_FOUND", `unknown operation ${operationUuid}`);
    }
    return this.toResult(operation);
  }

  async resume(operationUuid: string): Promise<CampaignCreationResult> {
    const operation = this.store.getOperationByUuid(operationUuid);
    if (!operation) {
      throw new AppError(404, "OPERATION_NOT_FOUND", `unknown operation ${operationUuid}`);
    }
    if (isTerminalOpStatus(operation.status)) {
      return this.toResult(operation);
    }
    return await this.execute(operation);
  }

  private requireResource(
    items: { id: number; state?: string | null }[],
    resourceId: number,
    label: string,
  ): void {
    const match = items.find((item) => item.id === resourceId);
    if (!match) {
      throw new AppError(
        409,
        "CAMPAIGN_CONFIGURATION_INVALID",
        `${label} ${resourceId} not found`,
        { reason: `${label} ${resourceId} not found` },
      );
    }
    if (match.state != null && match.state.toLowerCase() !== "active") {
      throw new AppError(
        409,
        "CAMPAIGN_CONFIGURATION_INVALID",
        `${label} ${resourceId} is not active`,
        { reason: `${label} ${resourceId} is not active` },
      );
    }
  }

  private async execute(
    operation: CampaignCreationOperation,
  ): Promise<CampaignCreationResult> {
    const stages: CreationStage[] = ["campaign", "country_flow", "offer_flow"];
    let current = operation;

    for (const stage of stages) {
      const state = this.getStep(current, stage);
      if (state === "confirmed") {
        continue;
      }
      if (state === "not_started") {
        const claimed = this.store.claimStep(current.operation_uuid, stage);
        if (!claimed) {
          const fresh = this.store.getOperationByUuid(current.operation_uuid);
          if (!fresh) return this.toResult(current);
          current = fresh;
          const freshState = this.getStep(current, stage);
          if (freshState === "confirmed") continue;
          if (freshState === "in_flight") {
            const recovered = await this.recoverStep(current, stage);
            current = this.store.getOperationByUuid(current.operation_uuid) ?? current;
            if (!recovered) return this.toResult(current);
            continue;
          }
          return this.toResult(current);
        }
        current = claimed;
        const ok = await this.postAndConfirm(current, stage);
        current = this.store.getOperationByUuid(current.operation_uuid) ?? current;
        if (!ok) return this.toResult(current);
      } else {
        const ok = await this.recoverStep(current, stage);
        current = this.store.getOperationByUuid(current.operation_uuid) ?? current;
        if (!ok) return this.toResult(current);
      }
    }

    current.status = "created";
    current.error_message = null;
    current = this.store.saveOperation(current);
    return this.toResult(current);
  }

  private async postAndConfirm(
    operation: CampaignCreationOperation,
    stage: CreationStage,
  ): Promise<boolean> {
    let resourceId: number;
    try {
      if (stage === "campaign") {
        const c = await this.client.createCampaign({
          alias: operation.alias,
          name: operation.name,
          domain_id: operation.domain_id,
          group_id: operation.group_id,
          traffic_source_id: operation.traffic_source_id,
        });
        resourceId = c.id;
      } else if (stage === "country_flow") {
        const s = await this.client.createStream({
          campaign_id: operation.keitaro_campaign_id!,
          type: "forced",
          name: `${operation.alias}-country`,
          schema: "redirect",
          action_type: "http",
          action_payload: operation.redirect_url,
          filters: [
            {
              name: "country",
              mode: "accept",
              payload: [operation.country_code],
            },
          ],
          comments: operation.operation_uuid,
        });
        resourceId = s.id;
      } else {
        const s = await this.client.createStream({
          campaign_id: operation.keitaro_campaign_id!,
          type: "default",
          name: `${operation.alias}-offer`,
          schema: "landings",
          action_type: "http",
          offers: [
            { offer_id: operation.offer_id, share: 100, state: "active" },
          ],
          comments: operation.operation_uuid,
        });
        resourceId = s.id;
      }
    } catch (err) {
      const code = upstreamStatus(err);
      const definitive = isDefinitiveRejection(code);
      const targetStatus: OperationStatus | null =
        operation.status === "ambiguous"
          ? null
          : definitive
            ? "failed"
            : "ambiguous";
      const message = definitive
        ? `${stage}: definitive rejection (${err instanceof Error ? err.message : String(err)})`
        : `${stage}: uncertain outcome (${err instanceof Error ? err.message : String(err)})`;
      this.store.markStepAmbiguous(
        operation.operation_uuid,
        stage,
        targetStatus,
        message,
      );
      if (definitive) {
        return false;
      }
      return await this.recoverStep(operation, stage);
    }

    const nextStatus =
      stage === "campaign" && operation.status === "started" ? "partial" : null;
    const confirmed = this.store.confirmStep(
      operation.operation_uuid,
      stage,
      resourceId,
      nextStatus,
      null,
    );
    return confirmed !== null;
  }

  private async recoverStep(
    operation: CampaignCreationOperation,
    stage: CreationStage,
  ): Promise<boolean> {
    if (stage === "campaign") {
      const campaigns = await this.client.listCampaigns(0, 100);
      const matches = campaigns.filter((c) => c.alias === operation.alias);
      if (matches.length === 1) {
        this.store.confirmStep(
          operation.operation_uuid,
          stage,
          matches[0].id,
          operation.status === "started" ? "partial" : null,
          null,
        );
        return true;
      }
      this.store.markStepAmbiguous(
        operation.operation_uuid,
        stage,
        operation.status === "ambiguous" ? null : "ambiguous",
        `${stage}: unresolved (${matches.length})`,
      );
      return false;
    }

    if (operation.keitaro_campaign_id === null) {
      this.store.markStepAmbiguous(
        operation.operation_uuid,
        stage,
        operation.status === "ambiguous" ? null : "ambiguous",
        `${stage}: unresolved (0)`,
      );
      return false;
    }

    const flowType = stage === "country_flow" ? "forced" : "default";
    const streams = await this.client.listCampaignStreams(
      operation.keitaro_campaign_id,
    );
    const matches = streams.filter(
      (s) => s.comments === operation.operation_uuid && s.type === flowType,
    );
    if (matches.length === 1) {
      this.store.confirmStep(
        operation.operation_uuid,
        stage,
        matches[0].id,
        null,
        null,
      );
      return true;
    }
    this.store.markStepAmbiguous(
      operation.operation_uuid,
      stage,
      operation.status === "ambiguous" ? null : "ambiguous",
      `${stage}: unresolved (${matches.length})`,
    );
    return false;
  }

  private getStep(op: CampaignCreationOperation, stage: CreationStage) {
    switch (stage) {
      case "campaign":
        return op.campaign_step;
      case "country_flow":
        return op.country_flow_step;
      case "offer_flow":
        return op.offer_flow_step;
    }
  }

  private toResult(op: CampaignCreationOperation): CampaignCreationResult {
    return {
      operation_uuid: op.operation_uuid,
      alias: op.alias,
      status: op.status,
      campaign_id: op.keitaro_campaign_id,
      country_flow_id: op.flow_country_id,
      offer_flow_id: op.flow_offer_id,
      error_message: op.error_message,
    };
  }
}

export class FetchService {
  constructor(
    private readonly client: KeitaroClientPort,
    private readonly store: InMemoryStore,
  ) {}

  async fetch(campaignId: number): Promise<FetchOutcome> {
    if (!Number.isInteger(campaignId) || campaignId <= 0) {
      throw new AppError(422, "DOMAIN_POLICY_REJECTION", "campaign_id must be a positive int", {
        reason: "campaign_id must be a positive int",
      });
    }
    const streams = await this.client.listCampaignStreams(campaignId);
    const capturedAt = new Date().toISOString();
    const fetched = streams.map((stream) => this.toFetched(stream, campaignId));
    return this.store.reconcile({
      campaign_id: campaignId,
      streams: fetched,
      captured_at: capturedAt,
    });
  }

  listStreams(campaignId: number): StreamRefView[] {
    if (!Number.isInteger(campaignId) || campaignId <= 0) {
      throw new AppError(422, "DOMAIN_POLICY_REJECTION", "campaign_id must be a positive int", {
        reason: "campaign_id must be a positive int",
      });
    }
    return this.store.listStreamsByCampaign(campaignId);
  }

  getCommitted(campaignId: number, streamRef: number): CommittedStateView {
    return this.store.getCommittedState(campaignId, streamRef);
  }

  private toFetched(stream: KeitaroStream, campaignId: number): FetchedStream {
    if (stream.campaign_id !== campaignId) {
      throw new AppError(
        422,
        "DOMAIN_POLICY_REJECTION",
        `stream ${stream.id}: campaign_id ${stream.campaign_id} does not match ${campaignId}`,
      );
    }
    const offers: RemoteOffer[] = stream.offers.map((offer, position) => ({
      offer_id: offer.offer_id,
      share: offer.share,
      state: offer.state,
      position,
    }));
    const validated = validateRemoteOffers(offers);
    return {
      stream_ref: stream.id,
      campaign_id: stream.campaign_id,
      type: stream.type,
      name: stream.name,
      schema: stream.schema,
      offers: validated,
      comments: stream.comments,
    };
  }
}

export class DraftService {
  constructor(private readonly store: InMemoryStore) {}

  getViewForCampaign(campaignId: number, streamRef: number): DraftView | null {
    const owner = this.store.getStreamCampaign(streamRef);
    if (owner !== campaignId) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${streamRef} not found`, {
        campaign_id: campaignId,
        stream_ref: streamRef,
      });
    }
    return this.store.getDraftView(streamRef);
  }

  add(campaignId: number, streamRef: number, expectedVersion: number, offerId: number): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "add",
      offer_id: offerId,
    });
  }

  remove(campaignId: number, streamRef: number, expectedVersion: number, offerId: number): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "remove",
      offer_id: offerId,
    });
  }

  restore(campaignId: number, streamRef: number, expectedVersion: number, offerId: number): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "restore",
      offer_id: offerId,
    });
  }

  setPin(campaignId: number, streamRef: number, expectedVersion: number, offerId: number, pinned: boolean): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "set_pin",
      offer_id: offerId,
      value: pinned,
    });
  }

  setShare(campaignId: number, streamRef: number, expectedVersion: number, offerId: number, share: number): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "set_share",
      offer_id: offerId,
      value: share,
    });
  }

  cancel(campaignId: number, streamRef: number, expectedVersion: number): DraftCommandResult {
    return this.store.applyDraftCommand(campaignId, streamRef, expectedVersion, {
      kind: "cancel",
    });
  }
}

export class ConflictResolutionService {
  constructor(
    private readonly client: KeitaroClientPort,
    private readonly store: InMemoryStore,
  ) {}

  async acceptRemote(params: {
    campaign_id: number;
    stream_ref: number;
    expected_version: number;
    expected_conflicts_fingerprint: string;
  }): Promise<AcceptRemoteResult> {
    const owner = this.store.getStreamCampaign(params.stream_ref);
    if (owner !== params.campaign_id) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${params.stream_ref} not found`, {
        campaign_id: params.campaign_id,
        stream_ref: params.stream_ref,
      });
    }
    const view = this.store.getDraftView(params.stream_ref);
    if (!view) {
      throw new AppError(422, "DOMAIN_POLICY_REJECTION", "no draft exists for this stream", {
        reason: "no draft exists for this stream",
      });
    }
    if (view.version !== params.expected_version) {
      throw new AppError(409, "DRAFT_VERSION_CONFLICT", "draft version changed", {
        stream_ref: params.stream_ref,
        expected_version: params.expected_version,
        current_version: view.version,
      });
    }
    if (view.conflicts_fingerprint !== params.expected_conflicts_fingerprint) {
      throw new AppError(409, "DRAFT_CONFLICT_CHANGED", "conflict details changed", {
        stream_ref: params.stream_ref,
        expected: params.expected_conflicts_fingerprint,
        observed: view.conflicts_fingerprint,
      });
    }
    if (!view.is_stale && view.conflicts.length === 0) {
      return {
        stream_ref: params.stream_ref,
        draft: view,
        noop: true,
        version: view.version,
      };
    }

    const doc = await this.client.getStreamDocument(params.stream_ref);
    const rawOffers = Array.isArray(doc.offers) ? doc.offers : [];
    const offers: RemoteOffer[] = validateRemoteOffers(
      rawOffers.map((item: Record<string, unknown>, position: number) => ({
        offer_id: Number(item.offer_id),
        share: Number(item.share),
        state: item.state != null ? String(item.state) : null,
        position,
      })),
    );
    const observation: FetchedStream = {
      stream_ref: params.stream_ref,
      campaign_id: params.campaign_id,
      type: String(doc.type ?? ""),
      name: String(doc.name ?? ""),
      schema: String(doc.schema ?? ""),
      offers,
      comments: typeof doc.comments === "string" ? doc.comments : null,
    };
    return this.store.acceptRemote(
      params.campaign_id,
      params.stream_ref,
      params.expected_version,
      params.expected_conflicts_fingerprint,
      observation,
      new Date().toISOString(),
    );
  }
}

export class PublicationService {
  constructor(
    private readonly client: KeitaroClientPort,
    private readonly store: InMemoryStore,
  ) {}

  private get allowLiveWrites(): boolean {
    const disabled = (process.env.KEITARO_DISABLE_PUBLICATION_WRITES ?? "")
      .trim()
      .toLowerCase();
    return !(disabled === "true" || disabled === "1" || disabled === "yes");
  }

  getLatest(campaignId: number, streamRef: number): PublicPublicationView | null {
    const ref = this.store.getStreamRef(streamRef);
    if (!ref || ref.campaign_id !== campaignId) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${streamRef} not found`, {
        campaign_id: campaignId,
        stream_ref: streamRef,
      });
    }
    const op = this.store.getLatestPublication(campaignId, streamRef);
    return op ? toPublicPublicationView(op) : null;
  }

  listRecoverable(limit = 100): PublicPublicationView[] {
    return this.store
      .listRecoverablePublications(Math.min(limit, 100))
      .map(toPublicPublicationView);
  }

  cancel(campaignId: number, streamRef: number): CancelResult {
    const ref = this.store.getStreamRef(streamRef);
    if (!ref || ref.campaign_id !== campaignId) {
      throw new AppError(404, "STREAM_NOT_FOUND", `stream ${streamRef} not found`, {
        campaign_id: campaignId,
        stream_ref: streamRef,
      });
    }
    const operation = this.store.getLatestPublication(campaignId, streamRef);
    if (!operation) {
      return {
        stream_ref: streamRef,
        status: null,
        reason: "NO_PUBLICATION",
        draft_discarded: false,
      };
    }
    const status = operation.status;
    if (status === "in_progress" || status === "ambiguous") {
      return {
        stream_ref: streamRef,
        status,
        reason: "RECOVERY_REQUIRED",
        draft_discarded: false,
      };
    }
    if (status === "failed" && !operation.evidence) {
      return {
        stream_ref: streamRef,
        status,
        reason: "ABSENCE_EVIDENCE_REQUIRED",
        draft_discarded: false,
      };
    }
    if (status !== "pending" && status !== "failed") {
      return {
        stream_ref: streamRef,
        status,
        reason: "NOT_CANCELLABLE",
        draft_discarded: false,
      };
    }
    const cancelled = this.store.cancelPublication(
      campaignId,
      streamRef,
      "operator cancel",
    );
    if (!cancelled) {
      return {
        stream_ref: streamRef,
        status,
        reason: "CONCURRENT_PUBLICATION",
        draft_discarded: false,
      };
    }
    return {
      stream_ref: streamRef,
      status: "abandoned",
      reason: null,
      draft_discarded: cancelled.draft_discarded,
    };
  }

  async push(campaignId: number, streamRefs: number[]): Promise<PushResult> {
    const dirty = this.store.listDirtyDrafts(campaignId);
    const byRef = new Map(dirty.map((d) => [d.stream_ref, d]));
    const outcomes: PushStreamOutcome[] = [];
    const targets: DirtyDraftView[] = [];

    if (streamRefs.length > 0) {
      for (const ref of streamRefs) {
        const item = byRef.get(ref);
        if (!item) {
          outcomes.push({
            stream_ref: ref,
            status: null,
            operation_id: null,
            reason: "NO_DIRTY_DRAFT",
            reason_code: "NO_DIRTY_DRAFT",
          });
        } else if (item.is_stale) {
          outcomes.push({
            stream_ref: ref,
            status: null,
            operation_id: null,
            reason: "STALE_CONFLICT",
            reason_code: "STALE_CONFLICT",
          });
        } else {
          targets.push(item);
        }
      }
    } else {
      for (const item of dirty) {
        if (item.is_stale) {
          outcomes.push({
            stream_ref: item.stream_ref,
            status: null,
            operation_id: null,
            reason: "STALE_CONFLICT_SKIPPED",
            reason_code: "STALE_CONFLICT_SKIPPED",
          });
        } else {
          targets.push(item);
        }
      }
    }

    if (targets.length === 0) {
      return { campaign_id: campaignId, outcomes };
    }

    const planning: DirtyDraftView[] = [];
    const resumable: PublicationOperationRecord[] = [];
    for (const target of targets) {
      const unresolved = this.store.getUnresolvedPublication(
        campaignId,
        target.stream_ref,
      );
      if (!unresolved) {
        planning.push(target);
      } else if (
        unresolved.status === "pending" &&
        unresolved.draft_version === target.version &&
        JSON.stringify(unresolved.intent) === JSON.stringify(target.intent)
      ) {
        resumable.push(unresolved);
      } else {
        outcomes.push({
          stream_ref: target.stream_ref,
          status: null,
          operation_id: null,
          reason: "UNRESOLVED_PUBLICATION",
          reason_code: "UNRESOLVED_PUBLICATION",
        });
      }
    }

    if (planning.length === 0 && resumable.length === 0) {
      return { campaign_id: campaignId, outcomes };
    }

    if (!this.allowLiveWrites) {
      for (const t of planning) {
        outcomes.push({
          stream_ref: t.stream_ref,
          status: null,
          operation_id: null,
          reason: "LIVE_WRITES_DISABLED: offers-only PUT preservation is not verified",
          reason_code: "LIVE_WRITES_DISABLED",
        });
      }
      for (const r of resumable) {
        outcomes.push({
          stream_ref: r.stream_ref,
          status: null,
          operation_id: null,
          reason: "LIVE_WRITES_DISABLED: offers-only PUT preservation is not verified",
          reason_code: "LIVE_WRITES_DISABLED",
        });
      }
      return { campaign_id: campaignId, outcomes };
    }

    const plans: PublicationPlan[] = [];
    for (const target of planning) {
      const view = this.store.getDraftView(target.stream_ref);
      if (!view || view.version !== target.version) {
        outcomes.push({
          stream_ref: target.stream_ref,
          status: null,
          operation_id: null,
          reason: "DRAFT_CHANGED",
          reason_code: "DRAFT_CHANGED",
        });
        continue;
      }
      const verdict = evaluateEligibility(
        view.offers,
        view.policy_cases,
        view.unallocated,
      );
      if (!verdict.eligible) {
        outcomes.push({
          stream_ref: target.stream_ref,
          status: null,
          operation_id: null,
          reason: verdict.reason ?? "invalid_total",
          reason_code: (verdict.reason ?? "invalid_total").toUpperCase(),
        });
        continue;
      }
      const entries = buildOfferEntries(view.offers);
      const document = await this.client.getStreamDocument(target.stream_ref);
      const cached = this.store.getRemoteSnapshot(target.stream_ref);
      const remoteOffers = this.documentOffers(document);
      if (
        !cached ||
        JSON.stringify(cached.offers) !== JSON.stringify(remoteOffers)
      ) {
        outcomes.push({
          stream_ref: target.stream_ref,
          status: null,
          operation_id: null,
          reason: "STALE_REMOTE_STATE",
          reason_code: "STALE_REMOTE_STATE",
        });
        continue;
      }
      plans.push({
        campaign_id: campaignId,
        stream_ref: target.stream_ref,
        draft_version: target.version,
        intent: target.intent,
        staged_offers: view.offers,
        payload: {
          offers: entries.map((e) =>
            e.state !== null
              ? { offer_id: e.offer_id, share: e.share, state: e.state }
              : { offer_id: e.offer_id, share: e.share },
          ),
        },
        expected_entries: entries,
        preimage: document,
        correlation_marker: crypto.randomUUID(),
      });
    }

    const createdOps =
      plans.length > 0 ? this.store.createPendingPublications(plans) : [];
    const allOps = [...createdOps, ...resumable];
    const workerToken = crypto.randomUUID();

    for (const op of allOps) {
      const claimed = this.store.claimPublication(op.id, workerToken);
      if (!claimed) {
        outcomes.push({
          stream_ref: op.stream_ref,
          status: null,
          operation_id: null,
          reason: "CONCURRENT_PUBLICATION",
          reason_code: "CONCURRENT_PUBLICATION",
        });
        continue;
      }
      try {
        await this.client.updateStream(op.stream_ref, op.request_payload);
        const doc = await this.client.getStreamDocument(op.stream_ref);
        const observed = this.documentOffers(doc);
        const entries: OfferEntry[] = observed.map((o) => ({
          offer_id: o.offer_id,
          share: o.share,
          state: o.state,
        }));
        if (entriesMatch(entries, op.effective_entries)) {
          this.store.finalizePublicationSuccess(op.id, observed);
          outcomes.push({
            stream_ref: op.stream_ref,
            status: "succeeded",
            operation_id: op.id,
            reason: null,
            reason_code: null,
          });
        } else {
          this.store.markPublicationAmbiguous(
            op.id,
            "read-back did not match published intent",
          );
          outcomes.push({
            stream_ref: op.stream_ref,
            status: "ambiguous",
            operation_id: op.id,
            reason: "read-back did not match published intent",
            reason_code: null,
          });
        }
      } catch (err) {
        const msg = `PUT failed: ${err instanceof Error ? err.message : String(err)}`;
        this.store.markPublicationAmbiguous(op.id, msg);
        outcomes.push({
          stream_ref: op.stream_ref,
          status: "ambiguous",
          operation_id: op.id,
          reason: msg,
          reason_code: null,
        });
      }
    }

    return { campaign_id: campaignId, outcomes };
  }

  async recover(): Promise<RecoveryResult> {
    const ops = this.store.listRecoverablePublications(50);
    let remaining = ops.length;
    let processed = 0;
    let confirmed = 0;
    let unresolved = 0;
    let degraded = 0;

    for (const op of ops) {
      if (op.status === "pending") continue;
      processed += 1;
      remaining -= 1;
      try {
        const doc = await this.client.getStreamDocument(op.stream_ref);
        const observed = this.documentOffers(doc);
        const entries: OfferEntry[] = observed.map((o) => ({
          offer_id: o.offer_id,
          share: o.share,
          state: o.state,
        }));
        if (entriesMatch(entries, op.effective_entries)) {
          const verified = this.store.finalizePublicationSuccess(op.id, observed);
          if (verified) {
            confirmed += 1;
          } else {
            unresolved += 1;
          }
        } else {
          this.store.markPublicationAmbiguous(
            op.id,
            "read-back did not match the published intent",
          );
          unresolved += 1;
        }
      } catch (err) {
        degraded += 1;
        unresolved += 1;
        this.store.markPublicationAmbiguous(
          op.id,
          `reconciliation read failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    }

    return {
      processed,
      confirmed,
      unresolved,
      degraded,
      remaining,
      deadline_reached: false,
    };
  }

  private documentOffers(document: Record<string, unknown>): RemoteOffer[] {
    const raw = Array.isArray(document.offers) ? document.offers : [];
    return validateRemoteOffers(
      raw.map((item: Record<string, unknown>, position: number) => ({
        offer_id: Number(item.offer_id),
        share: Number(item.share),
        state: item.state != null ? String(item.state) : null,
        position,
      })),
    );
  }
}
