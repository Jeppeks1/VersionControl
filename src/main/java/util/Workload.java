package main.java.util;

import java.util.ArrayList;
import java.util.LinkedList;

public class Workload {

    private final int RATE_QUEUE_LIMIT = 10000;

    private final LinkedList<SubmittedProcedure> workQueue;
    private ArrayList<LatencyRecord.Sample> samples = new ArrayList<>();
    private LatencyRecord latencies;

    private int rate = 2000; // TPS
    private int time = 100; // Duration
    private int previousSecond = 0; // Logging purposes
    private boolean arrival = false; // Possion = true, Regular = false

    private int cnt = 0;
    private int totalCount = 0;

    public Workload(LinkedList<SubmittedProcedure> workQueue, int rate) {
        this.workQueue = workQueue;
        this.rate = rate;
    }

    public void executeWorkload() {
        System.out.println("Executing workload");

        // Begin measuring the completion time
        long startTime = System.nanoTime();

        // Determine the sleeping interval to meet the specified TPS
        long intervalNs = getInterval(rate, arrival);

        // Set the test duration in nanoseconds based on the user
        // input stored in the current phase.
        long testDurationNs = time * 1000000000L;

        // Prepare values for the main loop
        long nextInterval = startTime + intervalNs;
        boolean resetQueues = true;
        int nextToAdd = 1;

        // Main Loop
        boolean execute = true;
        while (execute) {
            // posting new work... and reseting the queue in case we have new
            // portion of the workload...

            addToQueue(nextToAdd, resetQueues);
            resetQueues = false;

            // Determine the time to sleep
            long now = System.nanoTime();
            long sleep = nextInterval - now;

            while (sleep > 0) {
                // Sleep for the required duration
                long sleepMs = sleep / 1000000;
                long sleepNs = sleep % 1000000;

                try {
                    Thread.sleep(sleepMs, (int) sleepNs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                //
                now = System.nanoTime();
                sleep = nextInterval - now;
            }

            // Compute the next interval and how many messages to deliver
            intervalNs = 0;
            nextToAdd = 0;
            do {
                intervalNs += getInterval(rate, arrival);
                nextToAdd++;
            } while ( -sleep > intervalNs);

            nextInterval += intervalNs;


            totalCount++;


            // Check if the current phase is complete
            boolean phaseComplete = (now - startTime >= testDurationNs);

            if (phaseComplete) {
                execute = false;
            }
        }

        System.out.println("Final count: " + totalCount + " and it should be: " + time * rate);
    }


    private void addToQueue(int amount, boolean resetQueues) {
        if (resetQueues)
            workQueue.clear();

        assert amount > 0;

        // Add the specified number of procedures to the end of the queue.
        for (int i = 0; i < amount; ++i)
            workQueue.add(new SubmittedProcedure(4));

        if (cnt >= rate) {
            System.out.println("workQueue size: " + workQueue.size());
            cnt = 0;
        } else
            cnt++;


        // Can't keep up with current rate? Remove the oldest transactions
        // (from the front of the queue).
        while (workQueue.size() > RATE_QUEUE_LIMIT)
            workQueue.remove();
    }


    private long getInterval(int rate, boolean arrival) {
        if (arrival)
            return (long) ((-Math.log(1 - Math.random()) / rate) * 1000000000.);
        else
            return (long) (1000000000 / (double) rate + 0.5);
    }


    public class SubmittedProcedure {
        private final long submittedTime;
        private final long benchStartTime;
        private long currentTime = 0;

        SubmittedProcedure(long benchStartTime) {
            this.submittedTime = System.nanoTime();
            this.benchStartTime = benchStartTime;
        }

        long getDriftMs() {
            return (currentTime - submittedTime);
        }

        void setCurrentTime() {
            this.currentTime = System.nanoTime();
        }
    }
}
