import { spawnSync } from "node:child_process";
import { performance } from "node:perf_hooks";

const steps = [
  ["jvm-test", "clojure", ["-M:test"]],
  ["lint", "clojure", ["-M:lint"]],
  ["cljs-test", "npm", ["run", "test:cljs"]],
  ["r2-gateway-test", "npm", ["run", "test:r2-gateway"]],
  ["merkle-bench", "clojure", ["-M:merkle-bench", "1000"]],
  ["view-bench", "clojure", ["-M:view-bench", "1000", "128"]],
  ["view-delta-bench", "clojure", ["-M:view-delta-bench", "1000", "100", "128"]],
];
const receipt = { schema: 1, runner: "kotobase-peer", startedAt: new Date().toISOString(), steps: [] };
for (const [name, command, args] of steps) {
  const started = performance.now();
  const result = spawnSync(command, args, { stdio: "inherit", env: process.env });
  const step = { name, durationMs: Math.round(performance.now() - started), exitCode: result.status ?? 1 };
  receipt.steps.push(step);
  if (step.exitCode !== 0) {
    receipt.outcome = "failed";
    receipt.finishedAt = new Date().toISOString();
    console.log(JSON.stringify(receipt));
    process.exit(step.exitCode);
  }
}
receipt.outcome = "succeeded";
receipt.finishedAt = new Date().toISOString();
console.log(JSON.stringify(receipt));
