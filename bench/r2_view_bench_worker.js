const OBJECT_KEY = "bench/materialized-view-pack-v1-8346444";
const OBJECT_BYTES = 8_346_444;

function percentile(values, p) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(p * (sorted.length - 1))];
}

function response(value, status = 200) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/bench") {
      const samples = Math.min(100, Math.max(1, Number(url.searchParams.get("samples") || 30)));
      const length = Math.min(1_048_576, Math.max(1, Number(url.searchParams.get("length") || 42207)));
      const timings = [];
      let received = 0;
      for (let i = 0; i < samples + 2; i += 1) {
        const offset = (i * 7919) % (OBJECT_BYTES - length);
        const started = performance.now();
        const object = await env.MERKLE_BUCKET.get(OBJECT_KEY, { range: { offset, length } });
        if (!object) return response({ error: "benchmark object is not seeded" }, 404);
        const body = await object.arrayBuffer();
        if (i >= 2) timings.push(performance.now() - started);
        received += body.byteLength;
      }
      return response({ backend: "cloudflare-r2", samples, rangeBytes: length,
                        receivedBytes: received, p50Ms: percentile(timings, 0.50),
                        p95Ms: percentile(timings, 0.95),
                        p99Ms: percentile(timings, 0.99),
                        meanMs: timings.reduce((a, b) => a + b, 0) / timings.length });
    }
    return response({ service: "kotobase-view-bench", routes: ["GET /bench"] });
  },
};
