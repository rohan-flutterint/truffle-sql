package com.fivetran.truffle.benchmark;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Measures the performance of converting a push-based query like:
 *
 * for (row in query)
 *   doSomething(row)
 *
 * into a pull-based iterator like:
 *
 * lock = new Lock()
 * nextValue = null
 * spawn
 *   for (row in query)
 *     nextValue = row
 *     lock.wait()
 *   nextValue = DONE
 * while (nextValue != DONE)
 *   doSomething(nextValue)
 *   lock.notify()
 */
public class PushToPull {
    public static final int N = 1_000;

    /**
     * The simplest possible strategy: a simple loop that adds up values
     */
    @Benchmark
    public void push() {
        Random random = new Random();

        int sum = 0;

        for (int i = 0; i < N; i++) {
            int row = random.nextInt(2);

            sum += row;
        }

        if (sum < N * .4)
            throw new RuntimeException(sum + " < 0.4");
        else if (sum > N * .6)
            throw new RuntimeException(sum + " > 0.6");
    }

    /**
     * Simulates an iterator-like strategy. A producer sets a value, a consumer adds up a running total,
     * and a lock coordinates the two threads.
     */
    @Benchmark
    public void pull() {
        IntIterator it = new IntIterator();

        Thread producer = new Thread(() -> {
            Random random = new Random();

            for (int i = 0; i < N; i++) {
                int row = random.nextInt(2);

                it.put(row);
            }

            it.close();
        });

        producer.start();

        int sum = 0;

        try {
            while (true) {
                int value = it.get();

                sum += value;
            }
        } catch (ClosedException e) {
            // Done
        }

        if (sum < N * .4)
            throw new RuntimeException(sum + " < 0.4");
        else if (sum > N * .6)
            throw new RuntimeException(sum + " > 0.6");
    }

    /**
     * Simulates an iterator-like strategy. A producer sets a value, a consumer adds up a running total,
     * and a lock coordinates the two threads.
     */
    @Benchmark
    public void customQueue() {
        IntQueue it = new IntQueue();

        Thread producer = new Thread(() -> {
            Random random = new Random();

            for (int i = 0; i < N; i++) {
                int row = random.nextInt(2);

                it.put(row);
            }

            it.close();
        });

        producer.start();

        int sum = 0;

        try {
            while (true) {
                int value = it.get();

                sum += value;
            }
        } catch (ClosedException e) {
            // Done
        }

        if (sum < N * .4)
            throw new RuntimeException(sum + " < 0.4");
        else if (sum > N * .6)
            throw new RuntimeException(sum + " > 0.6");
    }

    /**
     * Use a blocking queue with 100 elements to implement an iterator-like strategy.
     * Similar to {@link this#pull()}, but allows the producer thread to run for longer before it blocks.
     */
    @Benchmark
    public void javaQueue() {
        BlockingQueue<Integer> q = new ArrayBlockingQueue<>(100);
        int done = Integer.MAX_VALUE;

        Thread producer = new Thread(() -> {
            Random random = new Random();

            for (int i = 0; i < N; i++) {
                int row = random.nextInt(2);

                try {
                    q.put(row);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            try {
                q.put(done);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        producer.start();

        int sum = 0;

        while (true) {
            try {
                int value = q.take();

                if (value == done)
                    break;
                else
                    sum += value;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (sum < N * .4)
            throw new RuntimeException(sum + " < 0.4");
        else if (sum > N * .6)
            throw new RuntimeException(sum + " > 0.6");
    }

    /**
     * Decouple producer and consumer stages, while using only one thread.
     * Consumer maintains an extra bit of state, so it knows when it is receiving the first row.
     * Producer makes an extra call at the end to invoke any final operations.
     */
    @Benchmark
    public void stateful() {
        Random random = new Random();
        IntStage next = new IntStage();

        for (int i = 0; i < N; i++) {
            int row = random.nextInt(2);

            next.accept(row, false);
        }

        next.accept(-1, true);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(PushToPull.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}

class IntStage {
    int sum = -1;
    boolean firstRow = true;

    void accept(int value, boolean finished) {
        if (firstRow) {
            firstRow = false;
            sum = value;
        }
        else if (!finished) {
            sum += value;
        }
        else {
            if (sum < PushToPull.N * .4)
                throw new RuntimeException(sum + " < 0.4");
            else if (sum > PushToPull.N * .6)
                throw new RuntimeException(sum + " > 0.6");
        }
    }
}

enum IteratorState {
    Empty,
    Full,
    Closed
}

class ClosedException extends Exception {
}

class IntIterator {
    private int next = -1;
    private IteratorState state = IteratorState.Empty;

    synchronized void put(int value) {
        // Wait for consumer to get value
        while (state == IteratorState.Full) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        switch (state) {
            // Set value, and maybe notify waiting consumer
            case Empty:
                next = value;
                state = IteratorState.Full;
                notify();

                return;
            // This should never happen--value should be closed by producer
            case Closed:
                throw new IllegalStateException();
                // This should never happen--method is synchronized, and we waited for != Full
            case Full:
                throw new IllegalStateException();
        }
    }

    synchronized int get() throws ClosedException {
        // Wait for producer to put value, or close
        while (state == IteratorState.Empty) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        switch (state) {
            // Get value, and maybe notify waiting producer
            case Full:
                int result = next;
                state = IteratorState.Empty;
                notify();

                return result;
            // Producer closed, throw exception
            case Closed:
                throw new ClosedException();
                // This should never happen--method is synchronized, and we waited for != Empty
            case Empty:
            default:
                throw new IllegalStateException();
        }
    }

    synchronized void close() {
        // Wait for consumer to get value
        while (state == IteratorState.Full) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        switch (state) {
            // Close iterator, next call to get() will throw ClosedException
            case Empty:
                state = IteratorState.Closed;
                notify();

                return;
            // This should never happen--method is synchronized, and we waited for != Full
            case Full:
                throw new IllegalStateException();
                // Nothing to do
            case Closed:
        }
    }
}

class IntQueue {
    /**
     * This is a FILO queue.
     * A FIFO queue would make more sense for a real query engine,
     * but this is simpler and good enough for benchmarking.
     */
    private final int[] next = new int[100];
    /**
     * Number of elements in the queue.
     * -1 indicates queue has been closed by the producer
     */
    private int size = 0;

    synchronized void put(int value) {
        // Wait for consumer to get value
        while (size == next.length) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Set value, and maybe notify waiting consumer
        next[size] = value;
        size++;
        notify();
    }

    synchronized int get() throws ClosedException {
        // Wait for producer to put value, or close
        while (size == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        if (size == -1)
            throw new ClosedException();
        else {
            // Get value, and maybe notify waiting producer
            // This is FILO not FIFO, which is not how we would do it in a real query engine
            int result = next[size - 1];

            size--;
            notify();

            return result;
        }
    }

    synchronized void close() {
        // Wait for consumer to get value
        while (size > 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Close iterator, next call to get() will throw ClosedException
        size = -1;
        notify();
    }
}
