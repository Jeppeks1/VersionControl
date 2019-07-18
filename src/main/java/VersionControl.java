package main.java;

// Git - Rebase - ~2 - reword "fixed redundancy" fails


import main.java.util.Worker;
import main.java.util.Workload;

import java.util.*;

/**
 * Shit, this comment should be its own commit.
 */
public class VersionControl {

    private static final LinkedList<Workload.SubmittedProcedure> workQueue = new LinkedList<>();

    public static void main(String[] args) {

        int tps = 200;

        Workload workload = new Workload(workQueue, tps);
        Worker worker = new Worker(workQueue, tps);

        Thread workThread = new Thread(worker);
        workThread.start();

        workload.executeWorkload();

        workThread.interrupt();
    }

}
