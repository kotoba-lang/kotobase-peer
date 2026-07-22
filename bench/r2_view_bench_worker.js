const OBJECT_KEY = "bench/materialized-view-pack-v1-8346444";
const OBJECT_BYTES = 8_346_444;
const E2E_PACK_KEY = "bench/e2e/view-pack-v1";
const E2E_PACK_BYTES = 148_676;
const E2E_BUNDLE_KEY = "bench/e2e/query-bundle-v1";
const E2E_BUNDLE_CID = "bafyreihmpkqjjekl2uixpxk25dhazq7xg7s4umv3bzausa46jtmhfh6s7m";
const E2E_QUERY_KEY = "tenant-a/000000500";
const E2E_ASSET_VERSION = "join-v3";

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

function immutableHeaders(object, offset, length, totalBytes,
                          contentType = "application/vnd.kotobase.view-pack") {
  const etag = object.httpEtag || `"${object.etag}"`;
  return {
    "access-control-allow-origin": "*",
    "access-control-expose-headers": "accept-ranges, content-length, content-range, etag",
    "accept-ranges": "bytes",
    "cache-control": "public, max-age=31536000, immutable",
    "content-type": contentType,
    "content-length": String(length),
    "content-range": `bytes ${offset}-${offset + length - 1}/${totalBytes}`,
    etag,
  };
}

function parseRange(value, totalBytes) {
  const match = /^bytes=(\d+)-(\d+)$/.exec(value || "");
  if (!match) return null;
  const start = Number(match[1]);
  const end = Number(match[2]);
  const length = end - start + 1;
  if (start < 0 || end < start || end >= totalBytes || length > 1_048_576) return null;
  return { offset: start, length };
}

function authorized(request, env) {
  if (!env.E2E_BEARER_TOKEN) return true;
  return request.headers.get("authorization") === `Bearer ${env.E2E_BEARER_TOKEN}`;
}

const BENCH_PAGE = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>Kotobase Browser Range Benchmark</title></head>
<body><main><h1>Kotobase Browser Range Benchmark</h1>
<p>R2 immutable HTTP Range, no persistent local database.</p>
<pre id="result" data-status="running">running</pre></main>
<script>
const result = document.querySelector('#result');
const target = '/objects/view-pack';
async function sample(cache, count) {
  const values = [];
  let bytes = 0;
  for (let i = 0; i < count; i++) {
    const started = performance.now();
    const response = await fetch(target, {cache, headers: {Range: 'bytes=1048576-1090782'}});
    if (response.status !== 206) throw new Error('expected 206, got ' + response.status);
    bytes += (await response.arrayBuffer()).byteLength;
    values.push(performance.now() - started);
  }
  values.sort((a, b) => a - b);
  return {samples: count, bytes, p50Ms: values[Math.floor((count - 1) * .5)],
          p95Ms: values[Math.floor((count - 1) * .95)], values};
}
(async () => {
  try {
    const cold = await sample('reload', 10);
    const warm = await sample('force-cache', 20);
    result.dataset.status = 'done';
    result.textContent = JSON.stringify({cold, warm, userAgent: navigator.userAgent}, null, 2);
  } catch (error) {
    result.dataset.status = 'error'; result.textContent = String(error); console.error(error);
  }
})();
</script></body></html>`;

const E2E_PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width">
<title>Kotobase Full Browser Query E2E</title></head><body><main>
<h1>Kotobase Full Browser Query E2E</h1>
<p>point/range/cold/warm/concurrent: bundle CID → plan → R2 Range → block CID → DAG-CBOR → render</p>
<pre id="result" data-status="running">running</pre>
</main><script src="/view-e2e.js?v=${E2E_ASSET_VERSION}"></script></body></html>`;

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (["/e2e/config", "/e2e/bundle", "/e2e/object", "/e2e/key"].includes(url.pathname) &&
        !authorized(request, env)) {
      return response({ error: "unauthorized" }, 401);
    }
    if (request.method === "OPTIONS" && url.pathname === "/objects/view-pack") {
      return new Response(null, { status: 204, headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, HEAD, OPTIONS",
        "access-control-allow-headers": "range",
        "access-control-max-age": "86400",
      }});
    }
    if ((request.method === "GET" || request.method === "HEAD") &&
        url.pathname === "/objects/view-pack") {
      const range = parseRange(request.headers.get("range"), OBJECT_BYTES);
      if (!range) return new Response("A single bounded byte range is required", {
        status: 416, headers: { "content-range": `bytes */${OBJECT_BYTES}` },
      });
      const object = await env.MERKLE_BUCKET.get(OBJECT_KEY, { range });
      if (!object) return response({ error: "benchmark object is not seeded" }, 404);
      const headers = immutableHeaders(object, range.offset, range.length, OBJECT_BYTES);
      if (request.headers.get("if-none-match") === headers.etag)
        return new Response(null, { status: 304, headers });
      return new Response(request.method === "HEAD" ? null : object.body,
                          { status: 206, headers });
    }
    if (request.method === "GET" && url.pathname === "/e2e/config") {
      const result = response({ bundleCid: E2E_BUNDLE_CID, queryKey: E2E_QUERY_KEY });
      result.headers.set("cache-control", "no-store");
      result.headers.set("vary", "authorization");
      return result;
    }
    if (request.method === "GET" && url.pathname === "/e2e/key") {
      const keyId = url.searchParams.get("keyId");
      const keys = {"tenant-a/dek-v1": env.E2E_DEK_V1_B64,
                    "tenant-a/dek-v2": env.E2E_DEK_V2_B64};
      if (!keyId || !keys[keyId]) return response({ error: "key unavailable" }, 404);
      const result = response({ keyId, key: keys[keyId] });
      result.headers.set("cache-control", "no-store");
      result.headers.set("vary", "authorization");
      return result;
    }
    if (request.method === "GET" && url.pathname === "/e2e/bundle") {
      const object = await env.MERKLE_BUCKET.get(E2E_BUNDLE_KEY);
      if (!object) return response({ error: "E2E bundle is not seeded" }, 404);
      return new Response(object.body, { headers: {
        "content-type": "application/vnd.ipld.dag-cbor",
        "content-length": String(object.size),
        "cache-control": "private, max-age=31536000, immutable",
        "vary": "authorization",
        etag: object.httpEtag || `"${object.etag}"`,
      }});
    }
    if (request.method === "GET" && url.pathname === "/e2e/object") {
      const range = parseRange(request.headers.get("range"), E2E_PACK_BYTES);
      if (!range) return new Response("A single bounded byte range is required", {
        status: 416, headers: { "content-range": `bytes */${E2E_PACK_BYTES}` },
      });
      const object = await env.MERKLE_BUCKET.get(E2E_PACK_KEY, { range });
      if (!object) return response({ error: "E2E pack is not seeded" }, 404);
      const headers = immutableHeaders(object, range.offset, range.length, E2E_PACK_BYTES);
      headers["cache-control"] = "private, max-age=31536000, immutable";
      headers.vary = "authorization";
      return new Response(object.body, { status: 206, headers });
    }
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
    if (request.method === "GET" && url.pathname === "/") {
      return new Response(BENCH_PAGE, { headers: {
        "content-type": "text/html; charset=utf-8", "cache-control": "no-store",
      }});
    }
    if (request.method === "GET" &&
        (url.pathname === "/e2e" || url.pathname === "/e2e/encrypted-v1" ||
         url.pathname === "/e2e/rotation-v2" || url.pathname === "/e2e/join-v1" ||
         url.pathname === "/e2e/join-v2" || url.pathname === "/e2e/join-v3")) {
      return new Response(E2E_PAGE, { headers: {
        "content-type": "text/html; charset=utf-8", "cache-control": "no-store",
      }});
    }
    if (request.method === "GET" && url.pathname === "/view-e2e.js")
      return env.ASSETS.fetch(request);
    return response({ service: "kotobase-view-bench",
                      routes: ["GET /", "GET|HEAD|OPTIONS /objects/view-pack", "GET /bench"] });
  },
};
