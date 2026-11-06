#!/usr/bin/env node

/**
 * Kamish CLI Executable Wrapper (Phase 6 / ADR-006).
 * Resolves the platform-native Rust binary and forwards all arguments and signals.
 */

import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PLATFORM_PACKAGES = {
  "linux-x64": { pkg: "@kamish/kamish-linux-x64", bin: "kamish" },
  "linux-arm64": { pkg: "@kamish/kamish-linux-arm64", bin: "kamish" },
  "darwin-x64": { pkg: "@kamish/kamish-darwin-x64", bin: "kamish" },
  "darwin-arm64": { pkg: "@kamish/kamish-darwin-arm64", bin: "kamish" },
  "win32-x64": { pkg: "@kamish/kamish-win32-x64", bin: "kamish.exe" },
};

function resolveBinary() {
  // 1. Explicit override via environment variable
  if (process.env.KAMISH_BINARY_PATH) {
    if (fs.existsSync(process.env.KAMISH_BINARY_PATH)) {
      return process.env.KAMISH_BINARY_PATH;
    }
    console.error(
      `[kamish] Specified KAMISH_BINARY_PATH does not exist: ${process.env.KAMISH_BINARY_PATH}`
    );
  }

  const platformKey = `${process.platform}-${process.arch}`;
  const spec = PLATFORM_PACKAGES[platformKey];

  if (!spec) {
    console.error(
      `[kamish] Unsupported operating system or architecture: ${process.platform} (${process.arch}).`
    );
    console.error(
      `Supported platforms: linux-x64, linux-arm64, darwin-x64, darwin-arm64, win32-x64.`
    );
    process.exit(1);
  }

  // 2. Try resolving via Node module lookup (optionalDependencies)
  try {
    const resolvedPath = require.resolve(`${spec.pkg}/bin/${spec.bin}`);
    if (fs.existsSync(resolvedPath)) {
      return resolvedPath;
    }
  } catch {
    // Fall through to manual candidate scanning
  }

  // 3. Search node_modules tree upwards
  let currentDir = __dirname;
  while (true) {
    const candidate = path.join(currentDir, "node_modules", spec.pkg, "bin", spec.bin);
    if (fs.existsSync(candidate)) {
      return candidate;
    }
    const siblingCandidate = path.join(path.dirname(currentDir), spec.pkg, "bin", spec.bin);
    if (fs.existsSync(siblingCandidate)) {
      return siblingCandidate;
    }
    const parent = path.dirname(currentDir);
    if (parent === currentDir) break;
    currentDir = parent;
  }

  // 4. Local workspace target fallback (for development and repo builds)
  const repoTargets = [
    path.resolve(process.cwd(), `target/release/${spec.bin}`),
    path.resolve(process.cwd(), `target/debug/${spec.bin}`),
    path.resolve(process.cwd(), "target/release/kamish"),
    path.resolve(process.cwd(), "target/debug/kamish"),
    path.resolve(__dirname, `../../../target/release/${spec.bin}`),
    path.resolve(__dirname, `../../../target/debug/${spec.bin}`),
    path.resolve(__dirname, `../../target/release/${spec.bin}`),
    path.resolve(__dirname, "../../../target/release/kamish"),
    path.resolve(__dirname, "../../../target/debug/kamish"),
    path.resolve(__dirname, "../../target/release/kamish"),
  ];
  for (const t of repoTargets) {
    if (fs.existsSync(t)) {
      return t;
    }
  }

  console.error(
    `\x1b[1;31m[kamish] Error: Could not locate native binary for '${platformKey}' (${spec.pkg}).\x1b[0m`
  );
  console.error(`Please verify that optional dependencies were installed during npm install.`);
  console.error(`To specify an external binary directly, set KAMISH_BINARY_PATH=/path/to/kamish`);
  process.exit(1);
}

const binaryPath = resolveBinary();

try {
  const result = spawnSync(binaryPath, process.argv.slice(2), {
    stdio: "inherit",
    env: process.env,
  });

  if (result.error) {
    console.error(`[kamish] Failed to execute native binary: ${result.error.message}`);
    process.exit(1);
  }

  process.exit(result.status ?? (result.signal ? 1 : 0));
} catch (err) {
  console.error(`[kamish] Unexpected execution error: ${err.message}`);
  process.exit(1);
}
