package main.java.util;

/**
 * This class houses the density specification for a standard mix execution of TPC-C with think time.
 */
public class DensityConsultant {

    private final double m = 1.26956;
    private final double b = 0.0103497;
    private double density;
    private long deltaTS; //deltaTs in ms.
    private final int targetTPS;


    public DensityConsultant(int targetTPS) {
        this.targetTPS = targetTPS;
        this.computeDensity();
        this.computeDeltaTS();
    }

    /**
     * Computes the density found during the TPC-C execute stage. This value is pre-defines for the standard TPC-X Txn mix.
     */
    private void computeDensity() {
        this.density = m * targetTPS + b;
    }

    /**
     * Return how many seconds should the clock move forward at each new TS issue process.
     */
    private void computeDeltaTS() {
        this.deltaTS = (long) (1000 / density);
    }

    /**
     * Computes the Timestamp Delta used at each clock tick.
     */
    public long getDeltaTs() {
        return this.deltaTS;
    }
}
