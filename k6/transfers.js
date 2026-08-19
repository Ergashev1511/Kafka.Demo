import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5122';

export const errorRate = new Rate('errors');
export const publishDuration = new Trend('publish_duration', true);

export const options = {
  scenarios: {
    ramping: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '30s', target: 10000 },
        { duration: '1m', target: 10000 },
        { duration: '30s', target: 0 },
      ],
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    errors: ['rate<0.01'],
    http_req_duration: ['p(95)<1000'],
  },
};

export default function () {
  const uniqueSuffix = `${String(__VU).padStart(6, '0')}${String(__ITER).padStart(6, '0')}`;
  const payload = JSON.stringify({
    senderAccount: `8600${uniqueSuffix}`,
    receiverAccount: `9860${uniqueSuffix}`,
    amount: 10000 + __ITER,
    currency: 'UZS',
  });

  const res = http.post(`${BASE_URL}/api/transfers`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  const ok = check(res, {
    'status is 200': (r) => r.status === 200,
  });

  errorRate.add(!ok);
  publishDuration.add(res.timings.duration);

  sleep(0.1);
}
