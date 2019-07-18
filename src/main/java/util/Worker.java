package main.java.util;

import java.util.LinkedList;
import java.util.Random;

public class Worker implements Runnable {

    private final LinkedList<Workload.SubmittedProcedure> workQueue;
    private Random rng = new Random(System.currentTimeMillis());
    private int rate;
    private int cnt = 0;

    public Worker(LinkedList<Workload.SubmittedProcedure> workQueue, int rate){
        this.workQueue = workQueue;
        this.rate = rate;
    }

    @Override
    public void run() {
        try {
            System.out.println("Executing worker");
            long start = System.nanoTime();
            while (true){

                Workload.SubmittedProcedure proc = workQueue.poll();

                if (proc == null)
                    continue;

                proc.setCurrentTime();

                if (cnt >= rate) {
                    System.out.println("Difference between submitted and polled time: " + proc.getDriftMs());
                    cnt = 0;
                } else
                    cnt++;

                int threshold = 1000 / rate; // Rate at which the Worker is just able to keep up with the queue
                int thresholdAjd = threshold - threshold / 2; // Allow some space for random variance
                int sleep = thresholdAjd + rng.nextInt(threshold); // Add some random variance
                Thread.sleep(5);
            }
        } catch (InterruptedException ex) {
            System.out.println("Interrupted");
        }
    }
}