#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

PLAYWRIGHT_BASE_URL="${PLAYWRIGHT_BASE_URL:?PLAYWRIGHT_BASE_URL is required}"
UI_WORKDIR="${UI_WORKDIR:-explorer}"
PLAYWRIGHT_WORKERS="${PLAYWRIGHT_WORKERS:-4}"
PLAYWRIGHT_BROWSERS_PATH="${PLAYWRIGHT_BROWSERS_PATH:-${REPO_ROOT}/.cache/ms-playwright}"

export PLAYWRIGHT_WORKERS
export PLAYWRIGHT_BROWSERS_PATH

echo "Checking explorer frontend at ${PLAYWRIGHT_BASE_URL}/login"
for i in $(seq 1 30); do
  http_status="$(curl -s -o /tmp/explorer-login.html -w "%{http_code}" "${PLAYWRIGHT_BASE_URL}/login")"
  if [ "$http_status" = "200" ]; then
    if grep -q '<script' /tmp/explorer-login.html; then
      echo "Explorer frontend is ready (HTML contains script tags)"
      break
    fi

    echo "WARNING: Login page HTML has no script tags — frontend may not be embedded"
    echo "=== Page content ==="
    cat /tmp/explorer-login.html
    echo "===================="
    exit 1
  fi

  echo "Attempt ${i}/30: Explorer returned HTTP ${http_status}, retrying in 2s..."
  sleep 2
done

cd "$UI_WORKDIR"
pnpm install --frozen-lockfile --prefer-offline
pnpm exec playwright install chromium --with-deps
echo "Running deep-link frontend preflight at ${PLAYWRIGHT_BASE_URL}/dataIn/Task"
node --input-type=module <<'NODE'
import { chromium } from 'playwright';

const baseUrl = process.env.PLAYWRIGHT_BASE_URL;

if (!baseUrl) {
  throw new Error('PLAYWRIGHT_BASE_URL is required');
}

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();

try {
  await page.goto(`${baseUrl}/dataIn/Task`, { waitUntil: 'domcontentloaded' });

  await page.waitForFunction(
    () => Array.from(document.scripts).some(script => script.src.includes('/js/')),
    { timeout: 15000 }
  );

  const scriptSrcs = await page.evaluate(() =>
    Array.from(document.scripts)
      .map(script => script.src)
      .filter(Boolean)
  );

  const brokenDeepLinkAsset = scriptSrcs.find(src => src.includes('/dataIn/js/'));
  if (brokenDeepLinkAsset) {
    throw new Error(`Deep-link asset resolution is broken: ${brokenDeepLinkAsset}`);
  }

  await page.waitForFunction(() => !document.querySelector('#loader-wrapper'), { timeout: 15000 });

  console.log(`Deep-link frontend preflight passed at ${page.url()}`);
  console.log(`Resolved scripts: ${scriptSrcs.join(', ')}`);
} finally {
  await browser.close();
}
NODE
echo "Running Playwright tests with ${PLAYWRIGHT_WORKERS} workers"
pnpm exec playwright test
