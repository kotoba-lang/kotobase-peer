import assert from "node:assert/strict";
import worker from "./r2_view_bench_worker.js";

const calls = [];
const env = { MERKLE_BUCKET: {
  async get(key, options) {
    calls.push({ key, options });
    const length = options.range.length;
    return { body: new Uint8Array(length), httpEtag: '"test-etag"', etag: "test-etag",
             async arrayBuffer() { return new Uint8Array(length).buffer; } };
  },
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

console.log(JSON.stringify({ tests: 5, assertions: 14, outcome: "succeeded" }));
