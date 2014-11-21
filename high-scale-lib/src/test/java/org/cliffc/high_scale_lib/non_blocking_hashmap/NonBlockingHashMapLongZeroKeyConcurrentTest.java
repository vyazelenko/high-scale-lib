package org.cliffc.high_scale_lib.non_blocking_hashmap;

import junit.framework.TestCase;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

public class NonBlockingHashMapLongZeroKeyConcurrentTest extends TestCase {
    private static final int N_THREADS = 8;
    private ExecutorService service;

    protected void setUp() {
        service = Executors.newFixedThreadPool(N_THREADS);
    }

    protected void tearDown() throws Exception {
        service.shutdownNow();
        service.awaitTermination(1, TimeUnit.HOURS);
    }

    private List<String> execute(List<Callable<String>> tasks) throws Exception {
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

    public void test_get_putIfAbsent() throws Exception {
        for (int i = 0; i < 10000; i++) {
            get_putIfAbsent();
        }
    }

    private void get_putIfAbsent() throws Exception {
        final NonBlockingHashMapLong<String> map = new NonBlockingHashMapLong<String>();
        List<Callable<String>> tasks = new ArrayList<Callable<String>>();
        for (int i = 0; i < N_THREADS; i++) {
            final int j = i;
            tasks.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    String value = map.get(0);
                    if (value == null) {
                        value = "thread-" + j;
                        String tmp = map.putIfAbsent(0, value);
                        if (tmp != null) {
                            return tmp;
                        }
                    }
                    return value;
                }
            });
        }

        List<String> results = execute(tasks);

        assertEquals("get_putIfAbsent failed", 1, new HashSet<String>(results).size());
    }
}
