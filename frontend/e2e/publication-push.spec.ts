import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { mockStreams } from "./streams";

async function mockEditor(
  page: Page,
  options: {
    dirty?: boolean;
    onPush?: (body: unknown) => void;
  } = {},
) {
  const dirty = options.dirty ?? false;
  await page.route("**/api/health", (route) =>
    route.fulfill({ json: { status: "ok", mode: "live", writes_enabled: true } }),
  );
  await page.route("**/api/campaigns/42", (route) =>
    route.fulfill({
      json: { id: 42, name: "Campaign #42", state: "active", group_id: null },
    }),
  );
  await page.route("**/api/campaigns/42/push", (route) => {
    options.onPush?.(route.request().postDataJSON());
    return route.fulfill({
      json: {
        outcomes: [
          {
            stream_ref: 101,
            operation_uuid: "op-push-1",
            status: "succeeded",
            reason: null,
          },
        ],
      },
    });
  });
  await page.route("**/api/campaigns/42/streams/101/committed", (route) =>
    route.fulfill({ json: { stream_ref: 101, bindings: [], snapshot: null } }),
  );
  await mockStreams(page);
  await page.route("**/api/campaigns/42/streams/101/draft", (route) =>
    route.fulfill({
      json: {
        stream_ref: 101,
        draft: dirty
          ? {
              stream_ref: 101,
              offers: [
                {
                  offer_id: 7,
                  lifecycle: "ACTIVE",
                  is_pinned: true,
                  share: 60,
                  state: "active",
                  position: 0,
                },
                {
                  offer_id: 8,
                  lifecycle: "ACTIVE",
                  is_pinned: false,
                  share: 40,
                  state: "active",
                  position: 1,
                },
              ],
              baseline_offers: [
                { offer_id: 7, share: 50, state: "active", position: 0 },
                { offer_id: 8, share: 50, state: "active", position: 1 },
              ],
              staged_changes: { share_edits: { 7: 60 } },
              version: 2,
              is_dirty: true,
              is_stale: false,
              conflicts: [],
              policy_cases: [],
              unallocated: 0,
              conflicts_fingerprint: "fp-1",
            }
          : null,
      },
    }),
  );
  await page.route("**/api/campaigns/42/streams/101/publication", (route) =>
    route.fulfill({ json: { stream_ref: 101, operation: null } }),
  );
  await page.route("**/api/publications/recoverable*", (route) =>
    route.fulfill({ json: [] }),
  );
}

test("editor enables Push to Keitaro for a ready dirty draft and calls POST /push", async ({
  page,
}) => {
  let pushes = 0;
  let pushPayload: unknown = null;
  await mockEditor(page, {
    dirty: true,
    onPush: (body) => {
      pushes += 1;
      pushPayload = body;
    },
  });
  await page.goto("/campaigns/42/streams/101");

  await expect(
    page.getByRole("heading", { name: "Ready to push to Keitaro" }),
  ).toBeVisible();
  const push = page.getByRole("button", { name: "Push to Keitaro" });
  await expect(push).toBeVisible();
  await expect(push).toBeEnabled();

  await push.click();
  await expect(
    page.getByText("Pushed stream #101 to Keitaro. Committed state is now synced."),
  ).toBeVisible();
  expect(pushes).toBe(1);
  expect(pushPayload).toEqual({ stream_refs: [101] });

  const results = await new AxeBuilder({ page }).analyze();
  expect(
    results.violations.filter(
      (violation) =>
        violation.impact === "critical" || violation.impact === "serious",
    ),
  ).toEqual([]);
});

test("recovery queue stays reachable when no draft is staged", async ({
  page,
}) => {
  let pushes = 0;
  await mockEditor(page, {
    dirty: false,
    onPush: () => {
      pushes += 1;
    },
  });
  await page.goto("/campaigns/42/streams/101");

  await expect(page.getByRole("button", { name: "Push to Keitaro" })).toBeDisabled();
  await page.getByRole("link", { name: "inspect the recoverable queue" }).click();
  await expect(
    page.getByRole("heading", { name: "Recoverable publications" }),
  ).toBeVisible();
  expect(pushes).toBe(0);
});
