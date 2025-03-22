import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = 'http://client:8080/store';

export let options = {
    stages: [
        { duration: '30s', target: 25 }, // Ramp up to 100 VUs
        { duration: '1m', target: 25 }, // Stay at 100 VUs
        { duration: '30s', target: 0 },  // Ramp down to 0 VUs
    ],
    thresholds: {
        'http_req_duration': ['p(95)<300'], // 95% of requests must complete below 300ms
        'http_req_failed': ['rate<0.01'], // Less than 1% of requests should fail
    },
};

let latencyTrend = new Trend('latency');
let successRate = new Rate('success_rate');

export default function () {
    const key = `test-key-${__VU}-${__ITER}`;
    const value = `value-${__ITER}`;

    // Set value
    let setRes = http.put(`${BASE_URL}/${key}`, JSON.stringify({ value: value }), {
        headers: { 'Content-Type': 'application/json' },
    });

    check(setRes, {
        'set request successful': (res) => res.status === 200,
    });

    // Get value
    let getRes = http.get(`${BASE_URL}/${key}`);

    let success = check(getRes, {
        'get request successful': (res) => res.status === 200,
        'value was set correctly': (res) => res.json().value === value,
    });

    // Record metrics
    latencyTrend.add(getRes.timings.duration);
    successRate.add(success);

    sleep(0.5); // Simulate real user wait time
}
