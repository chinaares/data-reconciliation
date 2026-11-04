package io.github.kugin.reconciliation.executor;

import io.github.kugin.reconciliation.enums.ExecutorStatusEnum;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class RedisExecutorManagerTest {

    private RedisServer redisServer;
    private RedissonClient redissonClient;

    @BeforeEach
    public void setUp() throws IOException {
        redisServer = RedisServer.newRedisServer()
                .port(63790)
                .setting("maxmemory 128M")
                .build();
        redisServer.start();
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:63790");
        redissonClient = Redisson.create(config);
    }

    @AfterEach
    public void tearDown() throws IOException {
        redissonClient.shutdown();
        redisServer.stop();
    }

    @Test
    public void testNormalLifecycle() {
        RedisExecutorManager manager = new RedisExecutorManager("test-lifecycle", redissonClient);
        manager.initDate("20260426");

        String executorKey = manager.getExecutorKey();
        assertEquals("day:check:test-lifecycle:20260426", executorKey);

        manager.setStatus(ExecutorStatusEnum.BEFORE);
        assertEquals("BEFORE", manager.getCurrentStatus());
        assertTrue(manager.isProcessing());

        manager.setStatus(ExecutorStatusEnum.CHECK);
        assertEquals("CHECK", manager.getCurrentStatus());
        assertTrue(manager.isProcessing());

        manager.setStatus(ExecutorStatusEnum.AFTER);
        assertEquals("AFTER", manager.getCurrentStatus());
        assertTrue(manager.isProcessing());

        manager.setStatus(ExecutorStatusEnum.END);
        assertEquals("END", manager.getCurrentStatus());
        assertFalse(manager.isProcessing());
        assertTrue(manager.isComplete());
    }

    @Test
    public void testReentrantBlocking() {
        RedisExecutorManager manager1 = new RedisExecutorManager("test-reentrant", redissonClient);
        manager1.initDate("20260426");
        manager1.setStatus(ExecutorStatusEnum.BEFORE);
        assertTrue(manager1.isProcessing());

        RedisExecutorManager manager2 = new RedisExecutorManager("test-reentrant", redissonClient);
        manager2.initDate("20260426");
        assertTrue(manager2.isProcessing());
        // manager2 cannot acquire the lock already held by manager1,
        // but can still read the shared status set by manager1
        manager2.setStatus(ExecutorStatusEnum.BEFORE);
        assertEquals("BEFORE", manager2.getCurrentStatus());
    }

    @Test
    public void testPauseByError() {
        RedisExecutorManager manager = new RedisExecutorManager("test-error", redissonClient);
        manager.initDate("20260426");
        manager.setStatus(ExecutorStatusEnum.BEFORE);
        assertTrue(manager.isProcessing());

        manager.pauseByError();
        assertFalse(manager.isProcessing());

        manager.setStatus(ExecutorStatusEnum.ERROR);
        assertEquals("ERROR", manager.getCurrentStatus());
    }

    @Test
    public void testStatusTTL() {
        RedisExecutorManager manager = new RedisExecutorManager("test-ttl", redissonClient);
        manager.initDate("20260426");
        manager.setStatus(ExecutorStatusEnum.BEFORE);

        long remainTimeToLive = redissonClient.getBucket(manager.getExecutorKey()).remainTimeToLive();
        assertTrue(remainTimeToLive > 0, "status key must expire");
        assertTrue(remainTimeToLive <= TimeUnit.HOURS.toMillis(24), "status key TTL must not exceed 24h");

        // After the key expires/disappears the manager treats the date as never run
        redissonClient.getBucket(manager.getExecutorKey()).delete();
        assertNull(manager.getCurrentStatus());
        assertFalse(manager.isComplete());
    }

    @Test
    public void testNotCompleteWhenNoStatus() {
        RedisExecutorManager manager = new RedisExecutorManager("test-nostatus", redissonClient);
        manager.initDate("20260426");
        assertNull(manager.getCurrentStatus());
        assertFalse(manager.isComplete());
    }
}