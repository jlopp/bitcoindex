// k6 load test for `bidx serve`.
//
//   BASE_URL=https://api.example.com RATE=2000 DURATION=5m k6 run scripts/loadtest/bidx.js
//
// Drives a realistic explorer/wallet mix at a fixed arrival rate (open model:
// k6 keeps sending even when the server slows down, which is what real
// traffic does and what exposes queueing/shedding):
//
//   30% tip height/hash     — wallets polling for new blocks
//   20% recent blocks       — explorers rendering the last ~100 blocks
//   10% deep blocks         — historical lookups (immutable, cache-friendly)
//   15% tx by id            — txids harvested from the sample addresses
//   20% address summary/utxo/txs
//    5% Electrum JSON-RPC   — headers.subscribe / block.header
//
// Env:
//   BASE_URL   API root (default http://127.0.0.1:8432)
//   RATE       requests/second (default 200)
//   DURATION   e.g. 30s, 5m (default 1m)
//   API_KEY    sent as `Authorization: Bearer` when set
//   ADDRESSES  comma-separated sample addresses (default: a few well-known
//              mainnet ones; set your own for other networks)
//   P99_MS     p99 latency threshold in ms (default 250)

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = (__ENV.BASE_URL || 'http://127.0.0.1:8432').replace(/\/$/, '');
const RATE = parseInt(__ENV.RATE || '200', 10);
const DURATION = __ENV.DURATION || '1m';
const P99_MS = parseInt(__ENV.P99_MS || '250', 10);
const ADDRESSES = (__ENV.ADDRESSES ||
  [
    '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa', // genesis coinbase
    '12c6DSiU4Rq3P4ZxziKxzrGq8ZqTUrEr8j', // early miner
    '1HLoD9E4SDFFPDiYfNYnkBLQ85Y51J3Zb1', // early miner
    'bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh', // BIP-173 example
  ].join(',')).split(',').map((a) => a.trim()).filter(Boolean);

const params = { headers: {}, timeout: '10s' };
if (__ENV.API_KEY) params.headers.Authorization = `Bearer ${__ENV.API_KEY}`;

const shed = new Counter('bidx_shed_or_limited');

export const options = {
  discardResponseBodies: false,
  scenarios: {
    mixed: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.max(50, Math.ceil(RATE / 10)),
      maxVUs: Math.max(500, RATE * 2),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: [`p(99)<${P99_MS}`, 'p(95)<100'],
    'http_req_duration{route:tip}': [`p(99)<${P99_MS}`],
    'http_req_duration{route:address}': [`p(99)<${P99_MS * 2}`],
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const r = http.get(`${BASE}/blocks/tip/height`, params);
  if (r.status !== 200) {
    throw new Error(`setup: /blocks/tip/height returned ${r.status}: ${r.body}`);
  }
  const tip = parseInt(r.body, 10);
  const txids = [];
  for (const a of ADDRESSES) {
    const t = http.get(`${BASE}/address/${a}/txs?limit=25`, params);
    if (t.status === 200) {
      for (const tx of t.json()) if (tx.txid) txids.push(tx.txid);
    }
  }
  console.log(`setup: tip=${tip}, ${ADDRESSES.length} addresses, ${txids.length} txids`);
  return { tip, txids };
}

function pick(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function get(path, route) {
  const r = http.get(`${BASE}${path}`, Object.assign({ tags: { route } }, params));
  if (r.status === 503 || r.status === 429) shed.add(1);
  check(r, { [`${route} ok`]: (x) => x.status === 200 || x.status === 404 });
  return r;
}

export default function (data) {
  const roll = Math.random() * 100;
  if (roll < 30) {
    get(Math.random() < 0.5 ? '/blocks/tip/height' : '/blocks/tip/hash', 'tip');
  } else if (roll < 50) {
    get(`/blocks/${data.tip - Math.floor(Math.random() * 100)}`, 'block_recent');
  } else if (roll < 60) {
    get(`/blocks/${Math.floor(Math.random() * (data.tip - 100))}`, 'block_deep');
  } else if (roll < 75) {
    if (data.txids.length) get(`/tx/${pick(data.txids)}`, 'tx');
    else get(`/block-height/${Math.floor(Math.random() * data.tip)}`, 'tx');
  } else if (roll < 95) {
    const a = pick(ADDRESSES);
    const sub = pick(['', '/utxo', '/txs']);
    get(`/address/${a}${sub}`, 'address');
  } else {
    const body = Math.random() < 0.5
      ? { id: 1, method: 'blockchain.headers.subscribe', params: [] }
      : { id: 2, method: 'blockchain.block.header', params: [Math.floor(Math.random() * data.tip)] };
    const r = http.post(`${BASE}/`, JSON.stringify(body), Object.assign(
      { tags: { route: 'electrum' } },
      { headers: Object.assign({ 'Content-Type': 'application/json' }, params.headers), timeout: '10s' },
    ));
    check(r, { 'electrum ok': (x) => x.status === 200 && !!x.json('result') });
  }
}
