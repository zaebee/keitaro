import { Router, type NextFunction, type Request, type Response } from "express";
import { AppError } from "./types.js";
import { createKeitaroClient } from "./keitaroClient.js";
import { InMemoryStore } from "./store.js";
import {
  CampaignCreationService,
  ConflictResolutionService,
  DraftService,
  FetchService,
  PublicationService,
} from "./services.js";

const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const CANCEL_ERROR_CODES: Record<string, string> = {
  RECOVERY_REQUIRED: "PUBLICATION_RECOVERY_REQUIRED",
  ABSENCE_EVIDENCE_REQUIRED: "PUBLICATION_ABSENCE_EVIDENCE_REQUIRED",
  NOT_CANCELLABLE: "PUBLICATION_NOT_CANCELLABLE",
  DRAFT_CHANGED: "PUBLICATION_CONCURRENT_CHANGE",
  CONCURRENT_PUBLICATION: "PUBLICATION_CONCURRENT_CHANGE",
};

function parsePositiveIntParam(raw: string | string[], name: string): number {
  const valueStr = Array.isArray(raw) ? raw[0] : raw;
  if (!/^\d+$/.test(valueStr)) {
    throw new AppError(422, "INVALID_REQUEST", `${name} must be a positive integer`);
  }
  const n = Number(valueStr);
  if (!Number.isSafeInteger(n) || n <= 0) {
    throw new AppError(422, "INVALID_REQUEST", `${name} must be greater than 0`);
  }
  return n;
}

function parseUuidParam(raw: string | string[]): string {
  const valueStr = Array.isArray(raw) ? raw[0] : raw;
  if (!UUID_RE.test(valueStr)) {
    throw new AppError(422, "INVALID_REQUEST", "operation_uuid must be a valid UUID");
  }
  return valueStr;
}

function assertObjectBody(
  body: unknown,
  allowedKeys: string[],
): Record<string, unknown> {
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    throw new AppError(422, "INVALID_REQUEST", "request body must be an object");
  }
  const obj = body as Record<string, unknown>;
  const allowed = new Set(allowedKeys);
  for (const key of Object.keys(obj)) {
    if (!allowed.has(key)) {
      throw new AppError(422, "INVALID_REQUEST", `unexpected field: ${key}`);
    }
  }
  return obj;
}

function requireStrictInt(obj: Record<string, unknown>, key: string): number {
  const value = obj[key];
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new AppError(422, "INVALID_REQUEST", `${key} must be an integer`);
  }
  return value;
}

function requireStrictString(obj: Record<string, unknown>, key: string): string {
  const value = obj[key];
  if (typeof value !== "string") {
    throw new AppError(422, "INVALID_REQUEST", `${key} must be a string`);
  }
  return value;
}

export function createApiRouter(): Router {
  const router = Router();
  const client = createKeitaroClient();
  const store = new InMemoryStore();

  const campaignService = new CampaignCreationService(client, store);
  const fetchService = new FetchService(client, store);
  const draftService = new DraftService(store);
  const conflictService = new ConflictResolutionService(client, store);
  const publicationService = new PublicationService(client, store);

  const wrap =
    (fn: (req: Request, res: Response) => Promise<void> | void) =>
    (req: Request, res: Response, next: NextFunction) => {
      Promise.resolve(fn(req, res)).catch(next);
    };

  router.get("/health", (_req, res) => {
    const disabled = (process.env.KEITARO_DISABLE_PUBLICATION_WRITES ?? "")
      .trim()
      .toLowerCase();
    const writesEnabled = !(
      disabled === "true" ||
      disabled === "1" ||
      disabled === "yes"
    );
    res.json({
      status: "ok",
      mode: client.isLive ? "live" : "sandbox",
      writes_enabled: writesEnabled,
    });
  });

  router.get(
    "/campaigns",
    wrap(async (req, res) => {
      const rawLimit = req.query.limit ? Number(req.query.limit) : 100;
      const limit =
        Number.isInteger(rawLimit) && rawLimit > 0 && rawLimit <= 100
          ? rawLimit
          : 100;
      const campaigns = await client.listCampaigns(0, limit);
      res.json(campaigns);
    }),
  );

  router.get(
    "/campaigns/:campaignId",
    wrap(async (req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      try {
        const campaign = await client.getCampaign(campaignId);
        res.json(campaign);
      } catch (err) {
        if (err instanceof AppError && err.details?.upstream) {
          const upstream = err.details.upstream as { status?: number };
          if (upstream.status === 404) {
            throw new AppError(404, "CAMPAIGN_NOT_FOUND", `campaign ${campaignId} not found`, {
              campaign_id: campaignId,
            });
          }
        }
        throw err;
      }
    }),
  );

  router.post(
    "/campaign-creations",
    wrap(async (req, res) => {
      const body = assertObjectBody(req.body, [
        "operation_uuid",
        "name",
        "country_code",
        "offer_id",
      ]);
      const operationUuid = requireStrictString(body, "operation_uuid");
      if (!UUID_RE.test(operationUuid)) {
        throw new AppError(422, "INVALID_REQUEST", "operation_uuid must be a valid UUID");
      }
      const name = requireStrictString(body, "name");
      const countryCode = requireStrictString(body, "country_code");
      const offerId = requireStrictInt(body, "offer_id");

      const outcome = await campaignService.createIdempotent(
        { name, country_code: countryCode, offer_id: offerId },
        operationUuid,
      );
      if (outcome.created) {
        res.setHeader("Location", `/campaign-creations/${outcome.result.operation_uuid}`);
        res.status(201).json(outcome.result);
      } else {
        res.status(200).json(outcome.result);
      }
    }),
  );

  router.get(
    "/campaign-creations/:operationUuid",
    wrap(async (req, res) => {
      const uuid = parseUuidParam(req.params.operationUuid);
      res.json(await campaignService.get(uuid));
    }),
  );

  router.post(
    "/campaign-creations/:operationUuid/resume",
    wrap(async (req, res) => {
      const uuid = parseUuidParam(req.params.operationUuid);
      res.json(await campaignService.resume(uuid));
    }),
  );

  router.post(
    "/campaigns/:campaignId/fetch",
    wrap(async (req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      res.json(await fetchService.fetch(campaignId));
    }),
  );

  router.get(
    "/campaigns/:campaignId/streams",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      res.json(fetchService.listStreams(campaignId));
    }),
  );

  router.get(
    "/campaigns/:campaignId/streams/:streamRef/committed",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      res.json(fetchService.getCommitted(campaignId, streamRef));
    }),
  );

  router.get(
    "/campaigns/:campaignId/streams/:streamRef/draft",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const draft = draftService.getViewForCampaign(campaignId, streamRef);
      res.json({ stream_ref: streamRef, draft });
    }),
  );

  router.get(
    "/campaigns/:campaignId/streams/:streamRef/publication",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const operation = publicationService.getLatest(campaignId, streamRef);
      res.json({ stream_ref: streamRef, operation });
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/add",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version", "offer_id"]);
      res.json(
        draftService.add(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/remove",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version", "offer_id"]);
      res.json(
        draftService.remove(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/restore",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version", "offer_id"]);
      res.json(
        draftService.restore(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/pin",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version", "offer_id"]);
      res.json(
        draftService.setPin(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
          true,
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/unpin",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version", "offer_id"]);
      res.json(
        draftService.setPin(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
          false,
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/share",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, [
        "expected_version",
        "offer_id",
        "share",
      ]);
      res.json(
        draftService.setShare(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
          requireStrictInt(body, "offer_id"),
          requireStrictInt(body, "share"),
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/cancel",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, ["expected_version"]);
      res.json(
        draftService.cancel(
          campaignId,
          streamRef,
          requireStrictInt(body, "expected_version"),
        ),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/draft/accept-remote",
    wrap(async (req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const body = assertObjectBody(req.body, [
        "expected_version",
        "expected_conflicts_fingerprint",
      ]);
      const expectedVersion = requireStrictInt(body, "expected_version");
      const expectedFingerprint = requireStrictString(
        body,
        "expected_conflicts_fingerprint",
      );
      if (expectedVersion <= 0 || expectedFingerprint.length < 7) {
        throw new AppError(422, "INVALID_REQUEST", "invalid accept-remote payload");
      }
      res.json(
        await conflictService.acceptRemote({
          campaign_id: campaignId,
          stream_ref: streamRef,
          expected_version: expectedVersion,
          expected_conflicts_fingerprint: expectedFingerprint,
        }),
      );
    }),
  );

  router.post(
    "/campaigns/:campaignId/push",
    wrap(async (req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const body = assertObjectBody(req.body ?? {}, ["stream_refs"]);
      const refs = Array.isArray(body.stream_refs)
        ? body.stream_refs.map((r) => Number(r))
        : [];
      const result = await publicationService.push(campaignId, refs);
      if (result.outcomes.some((o) => o.reason_code === "LIVE_WRITES_DISABLED")) {
        throw new AppError(
          503,
          "PUBLICATION_WRITES_DISABLED",
          "offers-only PUT publication writes are disabled",
          { result },
        );
      }
      res.json(result);
    }),
  );

  router.post(
    "/campaigns/:campaignId/streams/:streamRef/publication/cancel",
    wrap((req, res) => {
      const campaignId = parsePositiveIntParam(req.params.campaignId, "campaign_id");
      const streamRef = parsePositiveIntParam(req.params.streamRef, "stream_ref");
      const result = publicationService.cancel(campaignId, streamRef);
      if (result.reason === "NO_PUBLICATION" || result.status === "abandoned") {
        res.json(result);
        return;
      }
      const code =
        CANCEL_ERROR_CODES[result.reason ?? ""] ?? "PUBLICATION_NOT_CANCELLABLE";
      const details: Record<string, unknown> = { result };
      if (result.status !== null) details.status = result.status;
      if (result.reason !== null) details.reason = result.reason;
      throw new AppError(409, code, `publication cancel refused: ${result.reason}`, details);
    }),
  );

  router.get(
    "/publications/recoverable",
    wrap((req, res) => {
      const rawLimit = req.query.limit ? Number(req.query.limit) : 100;
      const limit =
        Number.isInteger(rawLimit) && rawLimit > 0 && rawLimit <= 100
          ? rawLimit
          : 100;
      res.json(publicationService.listRecoverable(limit));
    }),
  );

  router.post(
    "/publications/recover",
    wrap(async (_req, res) => {
      const result = await publicationService.recover();
      if (result.degraded > 0 || (result.deadline_reached && result.remaining > 0)) {
        throw new AppError(
          503,
          "RECOVERY_DEGRADED",
          "publication recovery did not complete",
          { result },
        );
      }
      res.json(result);
    }),
  );

  router.use((_req, res) => {
    res.status(404).json({
      error: {
        code: "NOT_FOUND",
        message: "API route not found",
        details: {},
      },
    });
  });

  router.use(
    (err: unknown, _req: Request, res: Response, _next: NextFunction) => {
      if (err instanceof AppError) {
        res.status(err.status).json({
          error: {
            code: err.code,
            message: err.message,
            details: err.details,
          },
        });
        return;
      }
      res.status(500).json({
        error: {
          code: "INTERNAL_ERROR",
          message: "internal server error",
          details: {},
        },
      });
    },
  );

  return router;
}
