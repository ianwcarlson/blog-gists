import { RedisQueue } from '../redis-queue';
import rc, { closeRedisConnection } from '../redis-connection';

beforeAll(async () => {
  rc.flushall();
});

afterAll(() => {
  closeRedisConnection();
});

test("Queue should demonstration desired behavior", async () => {
  expect(1).toBe(1);

  const testPayload1 = { test: 123 };
  const testPayload2 = { test: 456 };
  const testPayload3 = { test: 789 };

  const redisQueue = new RedisQueue<typeof testPayload1>("testQueue");

  // Even though we insert 2 messages into partition A initially,
  // the queue will read out the messages in round robin fashion
  redisQueue.writeToQueue("partitionKeyA", testPayload1);
  redisQueue.writeToQueue("partitionKeyA", testPayload2);
  redisQueue.writeToQueue("partitionKeyB", testPayload3);

  const readMessage1 = await redisQueue.readFromQueue(true);
  expect(readMessage1).not.toBeNull;
  expect(readMessage1?.test).toBe(123);

  const readMessage2 = await redisQueue.readFromQueue(true);
  expect(readMessage2).not.toBeNull;
  expect(readMessage2?.test).toBe(789);

  const readMessage3 = await redisQueue.readFromQueue(true);
  expect(readMessage3).not.toBeNull;
  expect(readMessage3?.test).toBe(456);

  const readMessage4 = await redisQueue.readFromQueue(false);
  expect(readMessage4).toBeNull;
});