export type PolicyCase =
  | "no_active_offers"
  | "single_active_offer"
  | "unallocated_remainder";

export type BindingLifecycle = "ACTIVE" | "ARCHIVED";

export type TransitionKind =
  | "CREATED"
  | "ARCHIVED"
  | "REACTIVATED"
  | "SHARE_CHANGED";

export type OperationStatus =
  | "started"
  | "partial"
  | "ambiguous"
  | "created"
  | "failed"
  | "abandoned";

export type StepState = "not_started" | "in_flight" | "confirmed" | "ambiguous";

export type CreationStage = "campaign" | "country_flow" | "offer_flow";

export type PublicationStatus =
  | "pending"
  | "in_progress"
  | "succeeded"
  | "failed"
  | "ambiguous"
  | "abandoned";

export const UNRESOLVED_PUBLICATION_STATUSES: ReadonlySet<PublicationStatus> =
  new Set(["pending", "in_progress", "ambiguous"]);

export interface RemoteOffer {
  offer_id: number;
  share: number;
  state: string | null;
  position: number;
}

export interface BindingState {
  offer_id: number;
  lifecycle: BindingLifecycle;
  is_pinned: boolean;
  last_remote_share: number | null;
}

export interface BindingTransition {
  offer_id: number;
  kind: TransitionKind;
  share: number | null;
}

export interface ConflictDetail {
  stream_ref: number;
  offer_id: number;
  field: string;
  expected: number | null;
  observed: number | null;
  reason: string;
}

export interface BaselineBinding {
  offer_id: number;
  lifecycle: BindingLifecycle;
  is_pinned: boolean;
}

export interface StagedOffer {
  offer_id: number;
  lifecycle: BindingLifecycle;
  is_pinned: boolean;
  share: number;
  state: string | null;
  position: number;
}

export interface DraftIntent {
  additions: number[];
  archives: number[];
  restores: number[];
  pin_toggles: Record<number, boolean>;
  share_edits: Record<number, number>;
}

export interface DraftView {
  stream_ref: number;
  offers: StagedOffer[];
  baseline_offers: RemoteOffer[];
  staged_changes: Record<string, unknown>;
  version: number;
  is_dirty: boolean;
  is_stale: boolean;
  conflicts: ConflictDetail[];
  conflicts_fingerprint: string;
  policy_cases: PolicyCase[];
  unallocated: number;
}

export interface DraftCommandResult {
  view: DraftView;
  created: boolean;
  noop: boolean;
}

export interface StreamRefView {
  stream_ref: number;
  campaign_id: number;
  type: string;
  name: string;
  schema: string;
  comments: string | null;
}

export interface RemoteSnapshotView {
  offers: RemoteOffer[];
  captured_at: string;
}

export interface CommittedStateView {
  stream_ref: number;
  bindings: {
    offer_id: number;
    lifecycle: BindingLifecycle;
    is_pinned: boolean;
    last_remote_share: number | null;
  }[];
  snapshot: RemoteSnapshotView | null;
}

export interface FetchOutcome {
  campaign_id: number;
  stream_count: number;
  reconciled_stream_count: number;
  created_bindings: number;
  archived_bindings: number;
  reactivated_bindings: number;
  share_updates: number;
  conflicted_drafts: number;
}

export interface AcceptRemoteResult {
  stream_ref: number;
  draft: DraftView | null;
  noop: boolean;
  version: number | null;
}

export interface OfferEntry {
  offer_id: number;
  share: number;
  state: string | null;
}

export interface PublicPublicationView {
  operation_uuid: string;
  campaign_id: number;
  stream_ref: number;
  status: PublicationStatus;
  draft_version: number;
  effective_entries: OfferEntry[];
  attempt_count: number;
}

export interface CancelResult {
  stream_ref: number;
  status: PublicationStatus | null;
  reason: string | null;
  draft_discarded: boolean;
}

export interface PushStreamOutcome {
  stream_ref: number;
  status: PublicationStatus | null;
  operation_id: number | null;
  reason: string | null;
  reason_code: string | null;
}

export interface PushResult {
  campaign_id: number;
  outcomes: PushStreamOutcome[];
}

export interface RecoveryResult {
  processed: number;
  confirmed: number;
  unresolved: number;
  degraded: number;
  remaining: number;
  deadline_reached: boolean;
}

export interface CampaignCreationOperation {
  id: number;
  operation_uuid: string;
  alias: string;
  name: string;
  country_code: string;
  offer_id: number;
  domain_id: number;
  group_id: number;
  traffic_source_id: number;
  redirect_url: string;
  status: OperationStatus;
  keitaro_campaign_id: number | null;
  flow_country_id: number | null;
  flow_offer_id: number | null;
  error_message: string | null;
  campaign_step: StepState;
  country_flow_step: StepState;
  offer_flow_step: StepState;
  created_at: string;
  updated_at: string;
}

export interface CampaignCreationResult {
  operation_uuid: string;
  alias: string;
  status: OperationStatus;
  campaign_id: number | null;
  country_flow_id: number | null;
  offer_flow_id: number | null;
  error_message: string | null;
}

export class AppError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: Record<string, unknown>;

  constructor(
    status: number,
    code: string,
    message: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "AppError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export class UpstreamError extends AppError {
  readonly upstreamStatus: number;
  readonly upstreamMessage: string;

  constructor(upstreamStatus: number, upstreamMessage: string) {
    super(
      502,
      "KEITARO_UPSTREAM_FAILURE",
      `Keitaro upstream error (${upstreamStatus}): ${upstreamMessage}`,
      { upstream: { status: upstreamStatus, message: upstreamMessage } },
    );
    this.upstreamStatus = upstreamStatus;
    this.upstreamMessage = upstreamMessage;
  }
}
