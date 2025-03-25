import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const BASE_URL = 'http://client:8080/store';

export const options = {
  scenarios: {
    contacts: {
      executor: 'constant-arrival-rate',
      duration: '5m',
      rate: 5,
      timeUnit: '1s',
      preAllocatedVUs: 100,
      maxVUs: 200,
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
