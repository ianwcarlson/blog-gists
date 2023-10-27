import { RedisQueue } from '../redis-queue';
import { closeRedisConnection } from '../redis-connection';

beforeAll(async () => {
});

afterAll(() => {
  closeRedisConnection();
});

test("should open redis connection", async () => {
  expect(1).toBe(1);

  const testPayload = { test: 123 };

  const redisQueue = new RedisQueue<typeof testPayload>("testQueue");

  redisQueue.writeToQueue("partitionKeyA", testPayload);
});