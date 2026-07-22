import assert from "node:assert/strict";
import worker from "./r2_view_bench_worker.js";

const calls = [];
const env = { MERKLE_BUCKET: {
  async get(key, options) {
    calls.push({ key, options });
    const length = options?.range.length ?? 3276;
    return { body: new Uint8Array(length), size: length,
             httpEtag: '"test-etag"', etag: "test-etag",
             async arrayBuffer() { return new Uint8Array(length).buffer; } };
  },
}, ASSETS: {
  async fetch() { return new Response("compiled browser asset"); },
}};

const origin = "https://example.test";
const range = await worker.fetch(new Request(`${origin}/objects/view-pack`, {
  headers: { Range: "bytes=100-199" },
}), env);
assert.equal(range.status, 206);
assert.equal(range.headers.get("content-range"), "bytes 100-199/8346444");
assert.equal(range.headers.get("content-length"), "100");
assert.equal(range.headers.get("etag"), '"test-etag"');
assert.match(range.headers.get("cache-control"), /immutable/);
assert.equal((await range.arrayBuffer()).byteLength, 100);
assert.deepEqual(calls[0].options, { range: { offset: 100, length: 100 } });

const missingRange = await worker.fetch(new Request(`${origin}/objects/view-pack`), env);
assert.equal(missingRange.status, 416);

const notModified = await worker.fetch(new Request(`${origin}/objects/view-pack`, {
  headers: { Range: "bytes=100-199", "If-None-Match": '"test-etag"' },
}), env);
assert.equal(notModified.status, 304);

const oversized = await worker.fetch(new Request(`${origin}/objects/view-pack`, {
  headers: { Range: "bytes=0-1048576" },
}), env);
assert.equal(oversized.status, 416);

const page = await worker.fetch(new Request(`${origin}/`), env);
assert.equal(page.status, 200);
assert.match(await page.text(), /Kotobase Browser Range Benchmark/);

const config = await worker.fetch(new Request(`${origin}/e2e/config`), env);
assert.equal(config.status, 200);
assert.deepEqual(await config.json(), {
  bundleCid: "bafyreihdvnslp2kwkmx4g4ut74fubk7dko4ezq4dkgr5mbuxnahkoz6kee",
  queryKey: "tenant-a/000000500",
});

const bundle = await worker.fetch(new Request(`${origin}/e2e/bundle`), env);
assert.equal(bundle.status, 200);
assert.equal(bundle.headers.get("content-length"), "3276");
assert.match(bundle.headers.get("cache-control"), /immutable/);

const e2eRange = await worker.fetch(new Request(`${origin}/e2e/object`, {
  headers: { Range: "bytes=100-199" },
}), env);
assert.equal(e2eRange.status, 206);
assert.equal(e2eRange.headers.get("content-range"), "bytes 100-199/148676");
assert.deepEqual(calls.at(-1).options, { range: { offset: 100, length: 100 } });

const e2ePage = await worker.fetch(new Request(`${origin}/e2e`), env);
assert.equal(e2ePage.status, 200);
assert.match(await e2ePage.text(), /bundle CID → plan → R2 Range/);
assert.match(await (await worker.fetch(new Request(`${origin}/e2e`), env)).text(),
             /view-e2e\.js\?v=refresh-stats-v1/);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/encrypted-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/rotation-v2`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v2`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v3`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/noncontig-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/cost-join-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/bundle-stats-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/refresh-stats-v1`), env)).status, 200);

const browserAsset = await worker.fetch(new Request(`${origin}/view-e2e.js`), env);
assert.equal(await browserAsset.text(), "compiled browser asset");

const protectedEnv = { ...env, E2E_BEARER_TOKEN: "test-capability",
                       E2E_DEK_V1_B64: "fixture-key-v1",
                       E2E_DEK_V2_B64: "fixture-key-v2" };
const callsBeforeDenied = calls.length;
const denied = await worker.fetch(new Request(`${origin}/e2e/object`, {
  headers: { Range: "bytes=100-199" },
}), protectedEnv);
assert.equal(denied.status, 401);
assert.equal(calls.length, callsBeforeDenied);

const allowed = await worker.fetch(new Request(`${origin}/e2e/object`, {
  headers: { Range: "bytes=100-199", Authorization: "Bearer test-capability" },
}), protectedEnv);
assert.equal(allowed.status, 206);
assert.equal(calls.at(-1).key, "bench/e2e/view-pack-v1");
assert.match(allowed.headers.get("cache-control"), /private/);
assert.equal(allowed.headers.get("vary"), "authorization");

const key = await worker.fetch(new Request(`${origin}/e2e/key?keyId=tenant-a%2Fdek-v2`, {
  headers: { Authorization: "Bearer test-capability" },
}), protectedEnv);
assert.equal(key.status, 200);
assert.deepEqual(await key.json(), { keyId: "tenant-a/dek-v2", key: "fixture-key-v2" });
assert.equal(key.headers.get("cache-control"), "no-store");

const retired = await worker.fetch(new Request(`${origin}/e2e/key?keyId=tenant-a%2Fdek-v1`, {
  headers: { Authorization: "Bearer test-capability" },
}), { ...protectedEnv, E2E_DEK_V1_B64: undefined });
assert.equal(retired.status, 404);

const casObjects = new Map();
let casVersion = 0;
const casEnv = {E2E_BEARER_TOKEN: "cas-capability", MERKLE_BUCKET: {
  async get(key) {
    const entry = casObjects.get(key);
    if (!entry) return null;
    return {etag: entry.etag, async text() { return entry.value; }};
  },
  async put(key, body, options) {
    const current = casObjects.get(key);
    const condition = options?.onlyIf;
    const won = !condition ||
      (condition.etagMatches && current?.etag === condition.etagMatches) ||
      (condition.etagDoesNotMatch === "*" && !current);
    if (!won) return null;
    const value = typeof body === "string" ? body : "block";
    const entry = {value, etag: `cas-${++casVersion}`};
    casObjects.set(key, entry);
    return entry;
  },
  async delete(key) { casObjects.delete(key); },
}};
const unconfiguredCas = await worker.fetch(
  new Request(`${origin}/bench/write-cas`, {method: "POST"}), env);
assert.equal(unconfiguredCas.status, 503);
const deniedCas = await worker.fetch(new Request(`${origin}/bench/write-cas`, {method: "POST"}), casEnv);
assert.equal(deniedCas.status, 401);
const casResponse = await worker.fetch(new Request(
  `${origin}/bench/write-cas?samples=4&concurrency=2`,
  {method: "POST", headers: {Authorization: "Bearer cas-capability"}}), casEnv);
assert.equal(casResponse.status, 200);
const casResult = await casResponse.json();
assert.equal(casResult.finalHead, 4);
assert.equal(casResult.lostUpdates, 0);
assert.equal(casObjects.size, 0, "benchmark objects and head are deleted in finally");

const gcObjects = new Map();
let gcVersion = 0;
const gcEnv = {E2E_BEARER_TOKEN: "gc-capability", MERKLE_BUCKET: {
  async get(key) {
    const entry = gcObjects.get(key);
    if (!entry) return null;
    return {etag: entry.etag, async text() { return entry.value; }};
  },
  async put(key, body) {
    const entry = {value: typeof body === "string" ? body : String(body),
                   etag: `gc-${++gcVersion}`};
    gcObjects.set(key, entry);
    return entry;
  },
  async list({prefix}) {
    return {objects: [...gcObjects.keys()].filter(key => key.startsWith(prefix))
      .map(key => ({key})), truncated: false};
  },
  async delete(keys) {
    for (const key of Array.isArray(keys) ? keys : [keys]) gcObjects.delete(key);
  },
}};
const gcResponse = await worker.fetch(new Request(`${origin}/bench/orphan-gc`, {
  method: "POST", headers: {Authorization: "Bearer gc-capability"},
}), gcEnv);
assert.equal(gcResponse.status, 200);
const gcResult = await gcResponse.json();
assert.deepEqual({heads: gcResult.heads, reachable: gcResult.reachable,
                  dryRunCandidates: gcResult.dryRunCandidates,
                  deleted: gcResult.deleted, liveAfter: gcResult.liveAfter,
                  orphanExistsAfter: gcResult.orphanExistsAfter},
                 {heads: 2, reachable: 4, dryRunCandidates: 1,
                  deleted: 1, liveAfter: 4, orphanExistsAfter: false});
assert.equal(gcObjects.size, 0, "GC drill objects and heads are deleted in finally");

console.log(JSON.stringify({ tests: 18, assertions: 53, outcome: "succeeded" }));
