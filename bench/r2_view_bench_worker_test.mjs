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
  bundleCid: "bafyreihmpkqjjekl2uixpxk25dhazq7xg7s4umv3bzausa46jtmhfh6s7m",
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
             /view-e2e\.js\?v=join-v3/);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/encrypted-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/rotation-v2`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v1`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v2`), env)).status, 200);
assert.equal((await worker.fetch(new Request(`${origin}/e2e/join-v3`), env)).status, 200);

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

console.log(JSON.stringify({ tests: 14, assertions: 40, outcome: "succeeded" }));
