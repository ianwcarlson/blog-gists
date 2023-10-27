import Redis, { Result, Callback } from "ioredis";

declare module "ioredis" {
  interface RedisCommander<Context> {
    execWriteQueueLuaScript(
      key1: string,
      key2: string,
      key3: string,
      arg1: string,
      arg2: string,
      callback?: Callback<string>
    ): Result<string, Context>;
  }
}

const port = parseInt(process.env.REDIS_PORT || "6379");
const host = process.env.REDIS_HOST || "127.0.0.1";

const connection =
  process.env.REDIS_MODE === "CLUSTER"
    ? new Redis.Cluster([
        {
          port,
          host,
        },
      ])
    : new Redis(port, host, {
      maxRetriesPerRequest: 1,
    });

export function closeRedisConnection() {
  connection.quit();
}

// One connection for the whole process since runtime is single threaded.
export default connection;
