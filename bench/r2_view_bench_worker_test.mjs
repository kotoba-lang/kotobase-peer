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
  bundleCid: "bafyreiczu47uqhv2mbid765rkv2zukfu7l664kblf22oodrkm72swyd3ge",
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
assert.equal(e2eRange.headers.get("content-range"), "bytes 100-199/63090");
assert.deepEqual(calls.at(-1).options, { range: { offset: 100, length: 100 } });

const e2ePage = await worker.fetch(new Request(`${origin}/e2e`), env);
assert.equal(e2ePage.status, 200);
assert.match(await e2ePage.text(), /bundle CID → plan → R2 Range/);

const browserAsset = await worker.fetch(new Request(`${origin}/view-e2e.js`), env);
assert.equal(await browserAsset.text(), "compiled browser asset");

console.log(JSON.stringify({ tests: 10, assertions: 24, outcome: "succeeded" }));
