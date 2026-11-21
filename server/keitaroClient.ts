import { UpstreamError } from "./types.js";

export interface KeitaroDomain {
  id: number;
  name: string;
  state: string | null;
}

export interface KeitaroGroup {
  id: number;
  name: string;
  type: string | null;
}

export interface KeitaroTrafficSource {
  id: number;
  name: string;
  state: string | null;
}

export interface KeitaroOffer {
  id: number;
  name: string;
  state: string | null;
  group_id: number | null;
}

export interface KeitaroCampaign {
  id: number;
  name: string;
  alias: string | null;
  state: string | null;
  domain_id: number | null;
  group_id: number | null;
  traffic_source_id: number | null;
}

export interface KeitaroStreamFilter {
  name: string;
  mode: string;
  payload: string[];
}

export interface KeitaroStreamOffer {
  offer_id: number;
  share: number;
  state: string | null;
}

export interface KeitaroStream {
  id: number;
  campaign_id: number;
  type: string;
  name: string;
  schema: string;
  action_type: string | null;
  action_payload: string | null;
  filters: KeitaroStreamFilter[];
  offers: KeitaroStreamOffer[];
  comments: string | null;
}

export interface KeitaroClientPort {
  readonly isLive: boolean;
  listDomains(): Promise<KeitaroDomain[]>;
  listGroups(gtype: string): Promise<KeitaroGroup[]>;
  listTrafficSources(): Promise<KeitaroTrafficSource[]>;
  getOffer(offerId: number): Promise<KeitaroOffer>;
  createCampaign(payload: {
    alias: string;
    name: string;
    domain_id: number | null;
    group_id: number | null;
    traffic_source_id: number | null;
  }): Promise<KeitaroCampaign>;
  listCampaigns(offset?: number, limit?: number): Promise<KeitaroCampaign[]>;
  getCampaign(campaignId: number): Promise<KeitaroCampaign>;
  listCampaignStreams(campaignId: number): Promise<KeitaroStream[]>;
  getStreamDocument(streamId: number): Promise<Record<string, unknown>>;
  createStream(payload: {
    campaign_id: number;
    type: string;
    name: string;
    schema: string;
    action_type: string;
    action_payload?: string | null;
    filters?: KeitaroStreamFilter[];
    offers?: KeitaroStreamOffer[];
    comments?: string | null;
  }): Promise<KeitaroStream>;
  updateStream(
    streamId: number,
    fields: Record<string, unknown>,
  ): Promise<KeitaroStream>;
}

class LiveKeitaroClient implements KeitaroClientPort {
  readonly isLive = true;
  private readonly baseUrl: string;
  private readonly apiKey: string;
  private readonly timeoutMs: number;

  constructor(baseUrl: string, apiKey: string, timeoutMs = 30000) {
    this.baseUrl = baseUrl.replace(/\/+$/, "") + "/admin_api/v1";
    this.apiKey = apiKey;
    this.timeoutMs = timeoutMs;
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown,
    retries = 0,
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), this.timeoutMs);
      try {
        const res = await fetch(url, {
          method,
          headers: {
            "Api-Key": this.apiKey,
            "content-type": "application/json",
          },
          body: body !== undefined ? JSON.stringify(body) : undefined,
          signal: controller.signal,
        });
        clearTimeout(timer);
        if (res.status >= 400) {
          throw new UpstreamError(res.status, res.statusText || `HTTP ${res.status}`);
        }
        const text = await res.text();
        return (text ? JSON.parse(text) : null) as T;
      } catch (err) {
        clearTimeout(timer);
        if (err instanceof UpstreamError) {
          throw err;
        }
        if (method === "GET" && attempt < retries) {
          await new Promise((r) => setTimeout(r, 100));
          continue;
        }
        throw new UpstreamError(
          0,
          err instanceof Error ? err.message : "upstream request failed",
        );
      }
    }
    throw new UpstreamError(0, "request failed");
  }

  async listDomains(): Promise<KeitaroDomain[]> {
    const data = await this.request<Record<string, unknown>[]>("GET", "/domains", undefined, 2);
    return data.map((d) => ({
      id: Number(d.id),
      name: String(d.name ?? ""),
      state: d.state != null ? String(d.state) : null,
    }));
  }

  async listGroups(gtype: string): Promise<KeitaroGroup[]> {
    const data = await this.request<Record<string, unknown>[]>(
      "GET",
      `/groups?type=${encodeURIComponent(gtype)}`,
      undefined,
      2,
    );
    return data.map((g) => ({
      id: Number(g.id),
      name: String(g.name ?? ""),
      type: g.type != null ? String(g.type) : null,
    }));
  }

  async listTrafficSources(): Promise<KeitaroTrafficSource[]> {
    const data = await this.request<Record<string, unknown>[]>(
      "GET",
      "/traffic_sources",
      undefined,
      2,
    );
    return data.map((s) => ({
      id: Number(s.id),
      name: String(s.name ?? ""),
      state: s.state != null ? String(s.state) : null,
    }));
  }

  async getOffer(offerId: number): Promise<KeitaroOffer> {
    const o = await this.request<Record<string, unknown>>(
      "GET",
      `/offers/${offerId}`,
      undefined,
      2,
    );
    return {
      id: Number(o.id),
      name: String(o.name ?? ""),
      state: o.state != null ? String(o.state) : null,
      group_id: o.group_id != null ? Number(o.group_id) : null,
    };
  }

  async createCampaign(payload: {
    alias: string;
    name: string;
    domain_id: number | null;
    group_id: number | null;
    traffic_source_id: number | null;
  }): Promise<KeitaroCampaign> {
    const c = await this.request<Record<string, unknown>>("POST", "/campaigns", payload);
    return {
      id: Number(c.id),
      name: String(c.name ?? payload.name),
      alias: c.alias != null ? String(c.alias) : null,
      state: c.state != null ? String(c.state) : null,
      domain_id: c.domain_id != null ? Number(c.domain_id) : null,
      group_id: c.group_id != null ? Number(c.group_id) : null,
      traffic_source_id:
        c.traffic_source_id != null ? Number(c.traffic_source_id) : null,
    };
  }

  async listCampaigns(offset = 0, limit = 100): Promise<KeitaroCampaign[]> {
    const raw = await this.request<Record<string, unknown>[]>(
      "GET",
      `/campaigns?offset=${offset}&limit=${limit}`,
      undefined,
      2,
    );
    const data = Array.isArray(raw) ? raw : [];
    return data.map((c) => ({
      id: Number(c.id),
      name: String(c.name ?? ""),
      alias: c.alias != null ? String(c.alias) : null,
      state: c.state != null ? String(c.state) : null,
      domain_id: c.domain_id != null ? Number(c.domain_id) : null,
      group_id: c.group_id != null ? Number(c.group_id) : null,
      traffic_source_id:
        c.traffic_source_id != null ? Number(c.traffic_source_id) : null,
    }));
  }

  async getCampaign(campaignId: number): Promise<KeitaroCampaign> {
    const c = await this.request<Record<string, unknown>>(
      "GET",
      `/campaigns/${campaignId}`,
      undefined,
      2,
    );
    return {
      id: Number(c.id),
      name: String(c.name ?? ""),
      alias: c.alias != null ? String(c.alias) : null,
      state: c.state != null ? String(c.state) : null,
      domain_id: c.domain_id != null ? Number(c.domain_id) : null,
      group_id: c.group_id != null ? Number(c.group_id) : null,
      traffic_source_id:
        c.traffic_source_id != null ? Number(c.traffic_source_id) : null,
    };
  }

  async listCampaignStreams(campaignId: number): Promise<KeitaroStream[]> {
    const data = await this.request<Record<string, unknown>[]>(
      "GET",
      `/campaigns/${campaignId}/streams`,
      undefined,
      2,
    );
    return data.map((s) => this.toStream(s, campaignId));
  }

  async getStreamDocument(streamId: number): Promise<Record<string, unknown>> {
    return await this.request<Record<string, unknown>>(
      "GET",
      `/streams/${streamId}`,
      undefined,
      2,
    );
  }

  async createStream(payload: {
    campaign_id: number;
    type: string;
    name: string;
    schema: string;
    action_type: string;
    action_payload?: string | null;
    filters?: KeitaroStreamFilter[];
    offers?: KeitaroStreamOffer[];
    comments?: string | null;
  }): Promise<KeitaroStream> {
    const s = await this.request<Record<string, unknown>>("POST", "/streams", payload);
    return this.toStream(s, payload.campaign_id);
  }

  async updateStream(
    streamId: number,
    fields: Record<string, unknown>,
  ): Promise<KeitaroStream> {
    const s = await this.request<Record<string, unknown>>(
      "PUT",
      `/streams/${streamId}`,
      fields,
    );
    return this.toStream(s, Number(s.campaign_id ?? 0));
  }

  private toStream(s: Record<string, unknown>, fallbackCampaignId: number): KeitaroStream {
    const rawFilters = Array.isArray(s.filters) ? s.filters : [];
    const rawOffers = Array.isArray(s.offers) ? s.offers : [];
    return {
      id: Number(s.id),
      campaign_id: s.campaign_id != null ? Number(s.campaign_id) : fallbackCampaignId,
      type: String(s.type ?? ""),
      name: String(s.name ?? ""),
      schema: String(s.schema ?? ""),
      action_type: s.action_type != null ? String(s.action_type) : null,
      action_payload: s.action_payload != null ? String(s.action_payload) : null,
      filters: rawFilters.map((f: Record<string, unknown>) => ({
        name: String(f.name ?? ""),
        mode: String(f.mode ?? "accept"),
        payload: Array.isArray(f.payload) ? f.payload.map(String) : [],
      })),
      offers: rawOffers.map((o: Record<string, unknown>) => ({
        offer_id: Number(o.offer_id),
        share: Number(o.share),
        state: o.state != null ? String(o.state) : null,
      })),
      comments: s.comments != null ? String(s.comments) : null,
    };
  }
}

/**
 * In-memory Keitaro sandbox used when KEITARO_API_KEY is not configured.
 * Provides pre-seeded domain, group, traffic source, offers, and demo campaign #42
 * so every UI workflow works out of the box in AI Studio.
 */
class SandboxKeitaroClient implements KeitaroClientPort {
  readonly isLive = false;
  private nextCampaignId = 43;
  private nextStreamId = 103;
  private readonly domains: KeitaroDomain[] = [
    { id: 1, name: "tlgk.host", state: "active" },
  ];
  private readonly groups: KeitaroGroup[] = [
    { id: 1, name: "Default Campaigns", type: "campaigns" },
  ];
  private readonly trafficSources: KeitaroTrafficSource[] = [
    { id: 1, name: "Organic / Direct", state: "active" },
  ];
  private readonly campaigns = new Map<number, KeitaroCampaign>();
  private readonly streams = new Map<number, KeitaroStream>();

  constructor() {
    this.campaigns.set(42, {
      id: 42,
      name: "Demo US Finance",
      alias: "slug-demo0042",
      state: "active",
      domain_id: 1,
      group_id: 1,
      traffic_source_id: 1,
    });
    this.streams.set(101, {
      id: 101,
      campaign_id: 42,
      type: "regular",
      name: "Main stream",
      schema: "landings",
      action_type: "http",
      action_payload: null,
      filters: [],
      offers: [
        { offer_id: 7, share: 60, state: "active" },
        { offer_id: 8, share: 40, state: "active" },
      ],
      comments: null,
    });
    this.streams.set(102, {
      id: 102,
      campaign_id: 42,
      type: "forced",
      name: "US Geo Fallback",
      schema: "redirect",
      action_type: "http",
      action_payload: "https://www.google.com",
      filters: [{ name: "country", mode: "accept", payload: ["US"] }],
      offers: [],
      comments: null,
    });
  }

  async listDomains(): Promise<KeitaroDomain[]> {
    return [...this.domains];
  }

  async listGroups(): Promise<KeitaroGroup[]> {
    return [...this.groups];
  }

  async listTrafficSources(): Promise<KeitaroTrafficSource[]> {
    return [...this.trafficSources];
  }

  async getOffer(offerId: number): Promise<KeitaroOffer> {
    if (!Number.isInteger(offerId) || offerId <= 0 || offerId > 10000) {
      throw new UpstreamError(404, `Offer ${offerId} not found`);
    }
    return {
      id: offerId,
      name: `Offer #${offerId}`,
      state: "active",
      group_id: 1,
    };
  }

  async createCampaign(payload: {
    alias: string;
    name: string;
    domain_id: number | null;
    group_id: number | null;
    traffic_source_id: number | null;
  }): Promise<KeitaroCampaign> {
    const id = this.nextCampaignId++;
    const campaign: KeitaroCampaign = {
      id,
      name: payload.name,
      alias: payload.alias,
      state: "active",
      domain_id: payload.domain_id,
      group_id: payload.group_id,
      traffic_source_id: payload.traffic_source_id,
    };
    this.campaigns.set(id, campaign);
    return { ...campaign };
  }

  async listCampaigns(offset = 0, limit = 100): Promise<KeitaroCampaign[]> {
    return Array.from(this.campaigns.values())
      .slice(offset, offset + limit)
      .map((c) => ({ ...c }));
  }

  async getCampaign(campaignId: number): Promise<KeitaroCampaign> {
    const campaign = this.campaigns.get(campaignId);
    if (!campaign) {
      throw new UpstreamError(404, `Campaign ${campaignId} not found`);
    }
    return { ...campaign };
  }

  async listCampaignStreams(campaignId: number): Promise<KeitaroStream[]> {
    if (!this.campaigns.has(campaignId)) {
      throw new UpstreamError(404, `Campaign ${campaignId} not found`);
    }
    return Array.from(this.streams.values())
      .filter((s) => s.campaign_id === campaignId)
      .map((s) => ({
        ...s,
        filters: s.filters.map((f) => ({ ...f, payload: [...f.payload] })),
        offers: s.offers.map((o) => ({ ...o })),
      }));
  }

  async getStreamDocument(streamId: number): Promise<Record<string, unknown>> {
    const stream = this.streams.get(streamId);
    if (!stream) {
      throw new UpstreamError(404, `Stream ${streamId} not found`);
    }
    return {
      ...stream,
      filters: stream.filters.map((f) => ({ ...f, payload: [...f.payload] })),
      offers: stream.offers.map((o) => ({ ...o })),
    };
  }

  async createStream(payload: {
    campaign_id: number;
    type: string;
    name: string;
    schema: string;
    action_type: string;
    action_payload?: string | null;
    filters?: KeitaroStreamFilter[];
    offers?: KeitaroStreamOffer[];
    comments?: string | null;
  }): Promise<KeitaroStream> {
    const id = this.nextStreamId++;
    const stream: KeitaroStream = {
      id,
      campaign_id: payload.campaign_id,
      type: payload.type,
      name: payload.name,
      schema: payload.schema,
      action_type: payload.action_type,
      action_payload: payload.action_payload ?? null,
      filters: (payload.filters ?? []).map((f) => ({
        ...f,
        payload: [...f.payload],
      })),
      offers: (payload.offers ?? []).map((o) => ({ ...o })),
      comments: payload.comments ?? null,
    };
    this.streams.set(id, stream);
    return { ...stream };
  }

  async updateStream(
    streamId: number,
    fields: Record<string, unknown>,
  ): Promise<KeitaroStream> {
    const existing = this.streams.get(streamId);
    if (!existing) {
      throw new UpstreamError(404, `Stream ${streamId} not found`);
    }
    const rawOffers = Array.isArray(fields.offers) ? fields.offers : existing.offers;
    const updated: KeitaroStream = {
      ...existing,
      offers: rawOffers.map((o: Record<string, unknown>) => ({
        offer_id: Number(o.offer_id),
        share: Number(o.share),
        state: o.state != null ? String(o.state) : null,
      })),
    };
    this.streams.set(streamId, updated);
    return { ...updated };
  }
}

export function createKeitaroClient(): KeitaroClientPort {
  const apiKey = (process.env.KEITARO_API_KEY ?? "").trim();
  const baseUrl = (process.env.KEITARO_BASE_URL ?? "https://tlgk.host").trim();
  if (apiKey.length > 0) {
    return new LiveKeitaroClient(baseUrl, apiKey);
  }
  return new SandboxKeitaroClient();
}
