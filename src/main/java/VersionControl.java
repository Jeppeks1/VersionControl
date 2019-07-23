package main.java;

// Git - Rebase - ~2 - reword "fixed redundancy" fails


import main.java.util.RandomParameters;
import main.java.util.Worker;
import main.java.util.Workload;

import java.util.*;

/**
 * Shit, this comment should be its own commit.
 */
public class VersionControl {

    private static final LinkedList<Workload.SubmittedProcedure> workQueue = new LinkedList<>();

    public static void main(String[] args) throws Exception {
        int warehouses = 4;
        int regionkey = 2;
        RandomParameters random = new RandomParameters("uniform", warehouses);

        for (int i = 0; i < 50; i++) {
            String nation1 = random.getRandomNation("Europe");

            System.out.println(nation1);
        }

    }


    public static void testWorkStuff() {
        int tps = 200;

        Workload workload = new Workload(workQueue, tps);
        Worker worker = new Worker(workQueue, tps);

        Thread workThread = new Thread(worker);
        workThread.start();

        workload.executeWorkload();

        workThread.interrupt();
    }

}
