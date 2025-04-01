import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = 'http://client:8080/store';

export const options = {
  scenarios: {
    ramping_scenario: {
      executor: 'ramping-arrival-rate',
      startRate: 0, // Start with 0 requests per second
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 200,
      stages: [
        { duration: '10m', target: 50 }, // Ramp up to 100 req/sec over 10 minutes
      ],
    },
  },
};

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
}
