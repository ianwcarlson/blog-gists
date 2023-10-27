import rc from './redis-connection';

class RedisQueue<T> {
  private static DEFAULT_BLOCK_READ_TIMEOUT_SECS = "10";
  private blockReadTimeout;
  private static RETRY_FOR_BATCH_SIZE_WAIT = 200;

  public static WRITE_TO_QUEUE_SCRIPT =
    'redis.call("INCRBY", KEYS[3], ARGV[2]) -- increment queue size counter\n' +
    'local isRegistered = redis.call("SISMEMBER", KEYS[1], ARGV[1]);\n' +
    'local roundRobinQueueInitialized = redis.call("EXISTS", KEYS[2]);\n' +
    "if isRegistered == 0 or roundRobinQueueInitialized == 0 then\n" +
    '  redis.call("RPUSH", KEYS[2], ARGV[1]);\n' +
    '  redis.call("SADD", KEYS[1], ARGV[1]);\n' +
    "end\n" +
    "return;";

  private queueName: string;
  private partitionSetKey: string;
  private roundRobinQueueKey: string;
  private roundRobinTotalCountKey: string;
  private keyCreated = false;

  public constructor(queueName: string) {
    this.queueName = queueName;
    this.blockReadTimeout = parseInt(
      process.env.BLOCK_READ_TIMEOUT ||
        RedisQueue.DEFAULT_BLOCK_READ_TIMEOUT_SECS
    );
    this.partitionSetKey = RedisQueue.buildPartitionSetQueueKey(queueName); // KEYS[1]
    this.roundRobinQueueKey = RedisQueue.buildRoundRobinQueueKey(queueName); // KEYS[2]
    this.roundRobinTotalCountKey =
      RedisQueue.buildRoundRobinTotalSizeKey(queueName); // KEYS[3]

    rc.defineCommand("execWriteQueueLuaScript", {
      numberOfKeys: 3,
      lua: RedisQueue.WRITE_TO_QUEUE_SCRIPT,
    });
  }

  // public getQueueSize() {
  //   Integer totalSize = 0;
  //   Set<String> partitionSetValues;

  //   try {
  //     partitionSetValues = new RedisSet(partitionSetKey).get();
  //   } catch (Exception e) {
  //     ExceptionUtils.getStackTrace(e);
  //     LOGGER.warn(e.getMessage());
  //     LOGGER.warn("This error is not fatal so returning 0 as default");
  //     return 0;
  //   }

  //   // Add the lengths of all the data queues associated with this queue
  //   for (String partition: partitionSetValues) {
  //     String dataQueueKey = RedisQueue.generateDateQueueKey(queueName, partition);
  //     totalSize += Math.toIntExact(Optional.ofNullable(
  //       RedisClusterConnection.runJedisCommand(rc ->
  //         rc.llen(dataQueueKey)
  //       )
  //     ).orElse(0L));
  //   }

  //   return totalSize;
  // }

  // We must use a Lua script to perform a write to guarantee atomic behavior.
  // Essentially, each partition will have a dedicated data queue. Each data
  // queue will be read in round-robin fashion. The main use case is to not
  // allow any partition/tenant/client to hog compute resources.
  // This implementation was based off this article:
  // https://medium.com/@yoav.kaplan/implement-fairness-queue-using-redis-606609923dd9
  public writeToQueue(partitionKey: string, data: T) {
    const dataQueueKey = RedisQueue.buildDateQueueKey(
      this.queueName,
      partitionKey
    );

    const keys: string[] = [
      this.partitionSetKey,
      this.roundRobinQueueKey,
      this.roundRobinTotalCountKey,
    ];
    const serializedData = JSON.stringify(data);

    const args: string[] = [
      partitionKey,
      "1", // ARGV[2]
    ];

    try {
      // Push the data first. Originally tried pushing the data inside the
      // lua script, but every key inside the script has to live on the same
      // node, so it forced a single node to handle all the data traffic (hotspot).
      // Now, we only do the control logic inside the script and all the data
      // queues will be evenly distributed. The transactional behavior is lost
      // so there are theoretical scenarios that would yield inconsistent data.
      // Can't think of a scenario where a failure wouldn't be recoverable though.
      // If the initial data write fails, the control logic is never executed so
      // nothing happens. Redis commands should be atomic so don't think there
      // will be write-latency timing issues either.

      rc.rpush(dataQueueKey, serializedData);

      rc.execWriteQueueLuaScript(
        keys[0],
        keys[1],
        keys[2],
        args[0],
        args[1]
      );
    } catch (e: any) {}
  }

  // public List<T> readFromQueue(
  //   final Class<? extends T> dataPayloadClass,
  //   final int readBatchSize,
  //   final int retryCount
  // ) {
  //   return readFromQueue(dataPayloadClass, readBatchSize, retryCount, true);
  // }

  // public List<T> readFromQueue(
  //   final Class<? extends T> dataPayloadClass,
  //   final int readBatchSize,
  //   final int retryCount,
  //   final Boolean blockedRead
  // ) {
  //   String roundRobinQueueKey = RedisQueue.generateRoundRobinQueueKey(queueName);
  //   String partitionSetKey = RedisQueue.generatePartitionSetQueueKey(queueName);
  //   String roundRobinTotalCountKey = RedisQueue.generateRoundRobinTotalSizeKey(queueName);

  //   Optional<List<String>> nextPartitionToReadOpt =
  //     readFromRoundRobinQueue(roundRobinQueueKey);

  //   if (nextPartitionToReadOpt.isPresent() && !nextPartitionToReadOpt.get().isEmpty()) {
  //     String partitionKey = nextPartitionToReadOpt.get().get(1);
  //     String dataQueueKey = generateDateQueueKey(queueName, partitionKey);
  //     ArrayList<T> readBatchData = readBatch(
  //       dataQueueKey,
  //       dataPayloadClass,
  //       readBatchSize,
  //       retryCount,
  //       blockedRead
  //     );

  //     // The only way to get transaction support with jedis is to write
  //     // a lua script. It doesn't seem like we actually need it, but this
  //     // comment serves as a reminder in case there are issues.

  //     // If there are more messages in the same partition then push another
  //     // message to the back of the queue. If there's no work to be done,
  //     // remove the partition from the active partition set.
  //     if (readBatchData.size() >= readBatchSize) {
  //       RedisClusterConnection.runJedisCommand(rc ->
  //         rc.rpush(roundRobinQueueKey, partitionKey)
  //       );
  //     } else {
  //       RedisClusterConnection.runJedisCommand(rc ->
  //         rc.srem(partitionSetKey, partitionKey)
  //       );
  //     }
  //     RedisClusterConnection.runJedisCommand(rc ->
  //       rc.decrBy(roundRobinTotalCountKey, readBatchData.size())
  //     );

  //     return readBatchData;
  //   }
  //   return new ArrayList<>();
  // }

  // private ArrayList<T> readBatch(
  //   final String dataQueueKey,
  //   final Class<? extends T> dataPayloadClass,
  //   final int readBatchSize,
  //   final int retryCount,
  //   final Boolean blockedRead
  // ) {
  //   Optional<String> readData = Optional.empty();
  //   ArrayList<T> readBatchData = new ArrayList<>();
  //   int messageCount = readBatchSize;
  //   int localRetryCount = retryCount;

  //   while ((readData.isPresent() || localRetryCount > 0) && messageCount > 0) {
  //     readData = popFromQueue(dataQueueKey, blockedRead);
  //     LOGGER.debug("readData: " + readData);
  //     if (readData.isPresent()) {
  //       Optional<T> deserializedMessage = deserializeMessage(readData.get(), dataPayloadClass);
  //       if (deserializedMessage.isPresent()) {
  //         messageCount -= 1;
  //         readBatchData.add(deserializedMessage.get());
  //       }
  //     } else {
  //       localRetryCount -= 1;
  //       if (localRetryCount > 0) {
  //         try {
  //           Thread.sleep(RETRY_FOR_BATCH_SIZE_WAIT);
  //         } catch (InterruptedException e) {
  //           LOGGER.debug("Sleep interrupted");
  //           LOGGER.debug(e.getMessage());
  //         }
  //       }
  //     }
  //   }
  //   return readBatchData;
  // }

  // private Optional<String> popFromQueue(
  //   final String dataQueueKey,
  //   final Boolean blockedRead
  // ) {
  //   try {
  //     List<String> readData;
  //     if (blockedRead) {
  //       readData = RedisClusterConnection.runJedisCommand(rc ->
  //         rc.blpop(blockReadTimeout, dataQueueKey)
  //       );
  //     } else {
  //       readData = RedisClusterConnection.runJedisCommand(rc ->
  //         rc.lpop(dataQueueKey, 1)
  //       );
  //     }
  //     LOGGER.debug(readData.toString());
  //     int dataIdx = blockedRead ? 1 : 0;
  //     return Optional.of(readData.get(dataIdx));
  //   } catch (Exception e) {
  //     return Optional.empty();
  //   }
  // }

  // private Optional<T> deserializeMessage(
  //   final String readData,
  //   final Class<? extends T> dataPayloadClass
  // ) {
  //   try {
  //     return Optional.of(MAPPER.readValue(
  //       readData,
  //       dataPayloadClass
  //     ));
  //   } catch (IOException e) {
  //     LOGGER.debug("Unable to deserialize: " + readData);
  //     LOGGER.error(e.getMessage());
  //     return Optional.empty();
  //   }
  // }

  // private Optional<List<String>> readFromRoundRobinQueue(final String roundRobinQueueKey) {
  //   try {
  //     return Optional.ofNullable(RedisClusterConnection.runJedisCommand(rc ->
  //       rc.blpop(blockReadTimeout, roundRobinQueueKey))
  //     );
  //   } catch (Exception e) {
  //     LOGGER.debug("Failed to read from round robin queue: " + roundRobinQueueKey);
  //     LOGGER.debug(e.toString());
  //     return Optional.of(new ArrayList<>());
  //   }
  // }

  public static buildRoundRobinQueueKey(queueName: string) {
    return "{" + queueName + "}:RoundRobin";
  }

  public static buildPartitionSetQueueKey(queueName: string) {
    return "{" + queueName + "}:RegisteredPartitions";
  }

  public static buildRoundRobinTotalSizeKey(queueName: string) {
    return "{" + queueName + "}:RoundRobinSize";
  }

  public static buildDateQueueKey(queueName: string, partitionKey: string) {
    return "DataQueue:" + queueName + ":" + partitionKey;
  }

  public remove(async: boolean) {
    try {
      if (async) {
        rc.unlink(RedisQueue.buildRoundRobinQueueKey(this.queueName));
        rc.unlink(RedisQueue.buildPartitionSetQueueKey(this.queueName));
        rc.unlink(RedisQueue.buildRoundRobinTotalSizeKey(this.queueName));
        rc.unlink(RedisQueue.buildDateQueueKey(this.queueName, "*"));
      } else {
        rc.del(RedisQueue.buildRoundRobinQueueKey(this.queueName));
        rc.del(RedisQueue.buildPartitionSetQueueKey(this.queueName));
        rc.del(RedisQueue.buildRoundRobinTotalSizeKey(this.queueName));
        rc.del(RedisQueue.buildDateQueueKey(this.queueName, "*"));
      }
    } catch (e: any) {}
  }
}

export { RedisQueue };
