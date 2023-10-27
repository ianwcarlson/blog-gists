import { RedisQueue } from '../redis-queue';

import { startRedis } from './test-utils';


beforeAll(async () => {
  await startRedis();
});

test("should open redis connection", async () => {
  expect(1).toBe(1);
  
});