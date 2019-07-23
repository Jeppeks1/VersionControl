package main.java.util;

import java.sql.Timestamp;
import java.util.*;

public class RandomParameters {

    private static final Map<String, String> nationToRegion = new LinkedHashMap<>();
    private static final List<String> nations;

    static {
        // The following pairs of nations and regions are *not* randomly selected from,
        // in order to have the highest possible representation from all regions. The
        // first six regions are always chosen in the order defined below, where the
        // number of nations for each region should equal the number of districts per
        // warehouse. This ensures that all nations are represented as well and empty
        // result sets are avoided.
        nationToRegion.put("Algeria","Africa");
        nationToRegion.put("Cameroon","Africa");
        nationToRegion.put("Ethiopia","Africa");
        nationToRegion.put("Kenya","Africa");
        nationToRegion.put("Madagascar","Africa");
        nationToRegion.put("Nigeria","Africa");
        nationToRegion.put("Rwanda","Africa");
        nationToRegion.put("South Africa","Africa");
        nationToRegion.put("Tanzania","Africa");
        nationToRegion.put("Zimbabwe","Africa");

        nationToRegion.put("Bangladesh","Asia");
        nationToRegion.put("China","Asia");
        nationToRegion.put("India","Asia");
        nationToRegion.put("Indonesia","Asia");
        nationToRegion.put("Japan","Asia");
        nationToRegion.put("South Korea","Asia");
        nationToRegion.put("Malaysia","Asia");
        nationToRegion.put("Mongolia","Asia");
        nationToRegion.put("Saudi Arabia","Asia");
        nationToRegion.put("Singapore","Asia");

        nationToRegion.put("Austria","Europe");
        nationToRegion.put("Belgium","Europe");
        nationToRegion.put("Denmark","Europe");
        nationToRegion.put("Finland","Europe");
        nationToRegion.put("Germany","Europe");
        nationToRegion.put("Greece","Europe");
        nationToRegion.put("Italy","Europe");
        nationToRegion.put("Norway","Europe");
        nationToRegion.put("Spain","Europe");
        nationToRegion.put("United Kingdom","Europe");

        nationToRegion.put("Bahamas","North America");
        nationToRegion.put("Canada","North America");
        nationToRegion.put("Cuba","North America");
        nationToRegion.put("Guatemala","North America");
        nationToRegion.put("Haiti","North America");
        nationToRegion.put("Honduras","North America");
        nationToRegion.put("Jamaica","North America");
        nationToRegion.put("Mexico","North America");
        nationToRegion.put("Puerto Rico","North America");
        nationToRegion.put("United States","North America");

        nationToRegion.put("Australia","Oceania");
        nationToRegion.put("Fiji","Oceania");
        nationToRegion.put("Marshall Islands","Oceania");
        nationToRegion.put("Nauru","Oceania");
        nationToRegion.put("New Zealand","Oceania");
        nationToRegion.put("Papua New Guinea","Oceania");
        nationToRegion.put("Samoa","Oceania");
        nationToRegion.put("Solomon Islands","Oceania");
        nationToRegion.put("Tonga","Oceania");
        nationToRegion.put("Tuvalu","Oceania");

        nationToRegion.put("Argentina","South America");
        nationToRegion.put("Brazil","South America");
        nationToRegion.put("Chile","South America");
        nationToRegion.put("Colombia","South America");
        nationToRegion.put("Ecuador","South America");
        nationToRegion.put("Panama","South America");
        nationToRegion.put("Paraguay","South America");
        nationToRegion.put("Peru","South America");
        nationToRegion.put("Uruguay","South America");
        nationToRegion.put("Venezuela","South America");

        // List used to associate a key with a nation
        nations = new ArrayList<>(nationToRegion.keySet());
    }

    private static List<String> regions = Arrays.asList("Africa", "Asia", "Europe", "North America", "Oceania", "South America");
    private static List<Character> alphabet = Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z');
    private static List<String> su_comment = Arrays.asList("good", "bad");

    private String distributionType;
    private int regionCount = 0;
    private int nationCount = 0;
    private int warehouses;

    public RandomParameters(String distributionType, int warehouses) {
        this.distributionType = distributionType;
        this.warehouses = warehouses;

        // The nation and region pairs are considered in groups. The number of warehouses
        // (regions) is the limiting factor, so the regions are selected in groups of regions.size().
        // If there are fewer than regions.size() warehouses, then the number of regions and
        // nations is equal to the number of warehouses.
    }

    public static long convertDateToLong(int year, int month, int day) {
        Timestamp ts = new Timestamp(year - 1900, month - 1, day, 0, 0, 0, 0);
        return ts.getTime();
    }

    public static long addMonthsToDate(long ts, int months) {
        Date date = new Date(ts);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, months);
        return cal.getTime().getTime();
    }

    public static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public static double randDoubleBetween(int start, int end) {
        return start + Math.random() * (end - start);
    }

    // ***********************************************
    //               Nation and region
    // ***********************************************

    /**
     * Returns a random nationkey within a region that is guaranteed to
     * be a part of the generated initial dataset.
     *
     * @return a random nationkey
     */
    public int getRandomNationKey() {
        // The universe of acceptable keys are bounded by the minimum value
        // of regions.size() or the number of warehouses. For each region,
        // there are configDistPerWhse number of nations.
        int bound = warehouses > regions.size() ? regions.size() : warehouses;
        Distribution dist = getDistributionType(bound * 10);

        // Make sure as many nations are represented as possible
        if (nationCount < bound)
            return nationCount++;
        else
            return dist.nextInt();
    }

    /**
     * Returns a random nationkey within the region associated with the
     * given regionkey. The nationkey is guaranteed to be a part of the
     * initially generated dataset.
     *
     * @param regionkey the regionkey within which a random nation should be selected
     * @return a random nationkey
     */
    public int getRandomNationKey(int regionkey) {
        int count = 0;
        while (true) {
            int nationkey = getRandomNationKey();
            if (regionkey == getRegionKey(nationkey))
                return nationkey;

            count++;

            // Error out if we have tried too many times
            if (count >= 1000)
                throw new RuntimeException("Error trying to generate a random nationkey with a region. " +
                        "Stuck in an infinite loop");
        }
    }

    public int getRegionKey(int nationkey) {
        String nation = nations.get(nationkey);
        String region = nationToRegion.get(nation);
        return regions.indexOf(region);
    }

    public String getRandomRegion() {
        return regions.get(getRandomRegionKey());
    }

    public String getRandomNation() {
        return nations.get(getRandomNationKey());
    }

    public String getRandomNation(String region) {
        int regionkey = regions.indexOf(region);
        int nationkey = getRandomNationKey(regionkey);
        return nations.get(nationkey);
    }

    private int getRandomRegionKey(){
        int bound = warehouses > regions.size() ? regions.size() : warehouses;
        Distribution dist = getDistributionType(bound);

        // Make sure as many nations are represented as possible
        if (regionCount < bound)
            return regionCount++;
        else
            return dist.nextInt();
    }


    // ***********************************************
    //                      Other
    // ***********************************************

    public Character generateRandomCharacter() {
        Distribution dist = getDistributionType(alphabet.size());
        int rand = dist.nextInt();
        return alphabet.get(rand);
    }

    public String getRandomSuComment() {
        Distribution dist = getDistributionType(su_comment.size());
        int rand = dist.nextInt();
        return su_comment.get(rand);
    }

    public String getRandomPhoneCountryCode() {
        Distribution dist = getDistributionType(nations.size());
        int rand = dist.nextInt() + 10;
        return Integer.toString(rand);
    }

    private Distribution getDistributionType(int size) {
        Distribution dist = null;

        if (distributionType.equals("uniform")) {
            dist = new UniformDistribution(0, size - 1);
        }

        return dist;
    }

    /**
     * The exponential distribution used by the TPC, for instance to calculate the
     * transaction's thinktime.
     *
     * @param rand the random generator
     * @param max  the maximum number which could be accept for this distribution
     * @param lMax the maximum number which could be accept for the following
     *             execution rand.nexDouble
     * @param mu   the base value provided to calculate the exponential number.
     *             For instance, it could be the mean thinktime
     * @return the calculated exponential number
     */
    public static long negExp(Random rand, long max, double lMax, double mu) {
        double r = rand.nextDouble();

        if (r < lMax) {
            return (max);
        }
        return ((long) (-mu * Math.log(r)));

    }


    public class UniformDistribution extends Distribution{

        private int max;
        private int min;
        private int interval;

        public UniformDistribution(int min, int max) {
            super("uniform");
            this.min = min;
            this.max = max;
            this.interval = max-min+1;
        }

        @Override
        public int nextInt(){
            Random rng = new Random();
            return rng.nextInt(interval) + min;
        }

        public double mean() {
            return ((double)((long)(max + (long)max))) / 2.0;
        }

    }

    public abstract class Distribution {

        String name = null;

        public Distribution(String name){
            this.name=name;
        }

        public String getName(){
            return this.name;
        }

        public abstract int nextInt();
    }

}
