package org.cliffc.high_scale_lib.non_blocking_hashmap;

import junit.framework.TestCase;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.*;
import java.util.concurrent.*;

public class NonBlockingHashMapLongZeroKeyConcurrentGetPutIfAbsentTest extends TestCase {
    private static final int N_THREADS = 8;
    private static final int ITERATIONS = 1000000;
    private ExecutorService service;

    protected void setUp() {
        service = Executors.newFixedThreadPool(N_THREADS);
    }

    protected void tearDown() throws Exception {
        service.shutdownNow();
        service.awaitTermination(1, TimeUnit.HOURS);
    }


    public void test_empty_map() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            Set<String> results = runIteration(new NonBlockingHashMapLong<String>());
            assertEquals(results.toString(), 1, results.size());
        }
    }

    public void test_zero_key_assigned() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<String>();
            map.put(0, "Initial value");

            Set<String> results = runIteration(map);
            assertEquals(Collections.singleton("Initial value"), results);
        }
    }

    public void test_other_keys_assigned() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<String>();
            map.put(Long.MIN_VALUE, "abc");
            map.put(Long.MAX_VALUE, "xyz");
            map.put(123, "123");

            Set<String> results = runIteration(map);
            assertEquals(results.toString(), 1, results.size());
        }
    }

    private Set<String> runIteration(NonBlockingHashMapLong<String> map) throws Exception {
        List<Callable<String>> tasks = new ArrayList<Callable<String>>();
        for (int i = 1; i <= N_THREADS; i++) {
            tasks.add(new PutIfAbsent(map, "worker #" + i));
        }

        List<String> results = executeTasks(tasks);

        return new HashSet<String>(results);
    }

    private List<String> executeTasks(List<Callable<String>> tasks) throws Exception {
        List<Future<String>> futures = new ArrayList<Future<String>>();
        final CountDownLatch startLatch = new CountDownLatch(N_THREADS + 1);
        for (Callable<String> t : tasks) {
            final Callable<String> task = t;
            Future<String> future = service.submit(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    startLatch.countDown();
                    startLatch.await();
                    return task.call();
                }
            });
            futures.add(future);
        }

        startLatch.countDown();

        List<String> results = new ArrayList<String>();
        for (Future<String> f : futures) {
            results.add(f.get());
        }
        return results;
    }

    private static class PutIfAbsent implements Callable<String> {
        private final NonBlockingHashMapLong<String> map;
        private final String newValue;

        public PutIfAbsent(NonBlockingHashMapLong<String> map, String newValue) {
            this.map = map;
            this.newValue = newValue;
        }

        @Override
        public String call() throws Exception {
            String value = map.get(0);
            if (value == null) {
                value = newValue;
                String tmp = map.putIfAbsent(0, value);
                if (tmp != null) {
                    return tmp;
                }
            }
            return value;
        }
    }
}
