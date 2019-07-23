package main.java.util;


import java.util.*;
import java.util.Map.Entry;


/**
 * A very nice and simple generic Histogram
 */
public class Histogram<X> {

    private static final String MARKER = "*";
    private static final Integer MAX_CHARS = 80;
    private static final Integer MAX_VALUE_LENGTH = 20;

    private final SortedMap<X, Integer> histogram = new TreeMap<X, Integer>();
    private int num_samples = 0;
    private transient boolean dirty = false;

    /**
     *
     */
    private transient Map<Object, String> debug_names;

    /**
     * The Min counts are the values that have the greatest number of
     * occurrences in the histogram
     */
    private int tpcc_max_count = 0;
    private int tpch_max_count = 0;

    /**
     * A switchable flag that determines whether non-zero entries are kept or removed
     */
    private boolean keep_zero_entries = false;

    /**
     * Constructor
     */
    public Histogram() {
        // Nothing...
    }

    /**
     * Constructor
     */
    public Histogram(boolean keepZeroEntries) {
        this.keep_zero_entries = keepZeroEntries;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Histogram<?>) {
            Histogram<?> other = (Histogram<?>) obj;
            return (this.histogram.equals(other.histogram));
        }
        return (false);
    }

    private boolean hasDebugLabels() {
        return (this.debug_names != null && !this.debug_names.isEmpty());
    }


    /**
     * The main method that updates a value in the histogram with a given sample count
     * This should be called by one of the public interface methods that are synchronized
     * This method is not synchronized on purpose for performance
     */
    private void _put(X value, int count) {
        if (value == null) return;
        this.num_samples += count;

        // If we already have this value in our histogram, then add the new count
        // to its existing total
        if (this.histogram.containsKey(value)) {
            count += this.histogram.get(value);
        }
        assert (count >= 0) : "Invalid negative count for key '" + value + "' [count=" + count + "]";
        // If the new count is zero, then completely remove it if we're not allowed to have zero entries
        if (count == 0 && !this.keep_zero_entries) {
            this.histogram.remove(value);
        } else {
            this.histogram.put(value, count);
        }
        this.dirty = true;
    }

    /**
     * Recalculate the max count value sets.
     * Since this is expensive, this should only be done whenever that information is needed
     */
    @SuppressWarnings("unchecked")
    private synchronized void calculateInternalValues() {
        if (!this.dirty) return;

        // New Min/Max Counts
        // The reason we have to loop through and check every time is that our
        // value may be the current max count and thus it may or may not still
        // be after the count is changed.
        tpcc_max_count = 0;
        tpch_max_count = 0;

        for (Entry<X, Integer> e : this.histogram.entrySet()) {
            X key = e.getKey();
            int cnt = e.getValue();

            if (key.toString().startsWith("Q")){
                // The key is likely referring to a query; not the most robust check
                if (cnt > tpch_max_count) {
                    tpch_max_count = cnt;
                }
            } else {
                // The key is likely referring to a TPCC transaction
                if (cnt > tpcc_max_count) {
                    tpcc_max_count = cnt;
                }
            }
        } // FOR
        this.dirty = false;
    }

    /**
     * Returns true if the Histogram is empty.
     */
    public boolean isEmpty() {
        for (Entry<X, Integer> e : this.histogram.entrySet()) {
            if (e.getValue() > 0)
                return false;
        }

        return true;
    }

    /**
     * Increments the number of occurrences of this particular value i
     *
     * @param value the value to be added to the histogram
     */
    public synchronized void put(X value, int i) {
        this._put(value, i);
    }

    /**
     * Set the number of occurrences of this particular value i
     *
     * @param value the value to be added to the histogram
     */
    public synchronized void set(X value, int i) {
        Integer orig = this.get(value);
        if (orig != null && orig != i) {
            i = (orig > i ? -1 * (orig - i) : i - orig);
        }
        this._put(value, i);
    }

    /**
     * Increments the number of occurrences of this particular value i
     *
     * @param value the value to be added to the histogram
     */
    public synchronized void put(X value) {
        this._put(value, 1);
    }

    /**
     * Increment multiple values by the given count
     */
    public synchronized void putAll(Collection<X> values, int count) {
        for (X v : values) {
            this._put(v, count);
        }
    }

    /**
     * Add all the entries from the provided Histogram into this objects totals
     */
    public synchronized void putHistogram(Histogram<X> other) {
        for (Entry<X, Integer> e : other.histogram.entrySet()) {
            if (e.getValue() > 0) this._put(e.getKey(), e.getValue());
        } // FOR
    }

    /**
     * Returns the current count for the given value
     * If the value was never entered into the histogram, then the count will be null
     */
    public Integer get(X value) {
        return histogram.get(value);
    }

    /**
     * Returns the current count for the given value.
     * If that value was nevere entered in the histogram, then the value returned will be value_if_null
     */
    public int get(X value, int value_if_null) {
        Integer count = histogram.get(value);
        return (count == null ? value_if_null : count);
    }

    /**
     * Return all the values stored in the histogram
     */
    public Collection<X> values() {
        return (Collections.unmodifiableCollection(this.histogram.keySet()));
    }

    /**
     * Returns true if this histogram contains the specified key.
     */
    public boolean contains(X value) {
        return (this.histogram.containsKey(value));
    }

    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------

    /**
     * Histogram Pretty Print
     */
    public String toString() {
        return (this.toString(MAX_CHARS, MAX_VALUE_LENGTH));
    }

    /**
     * Histogram Pretty Print
     */
    public String toString(Integer max_chars) {
        return (this.toString(max_chars, MAX_VALUE_LENGTH));
    }

    /**
     * Histogram Pretty Print
     */
    public synchronized String toString(Integer max_chars, Integer max_length) {
        StringBuilder s = new StringBuilder();
        if (max_length == null) max_length = MAX_VALUE_LENGTH;

        this.calculateInternalValues();

        // Figure out the max size of the counts
        int max_ctr_length = 4;
        for (Integer ctr : this.histogram.values()) {
            max_ctr_length = Math.max(max_ctr_length, ctr.toString().length());
        } // FOR

        // Don't let anything go longer than MAX_VALUE_LENGTH chars
        String f = "%-" + max_length + "s [%" + max_ctr_length + "d] ";
        boolean first = true;
        boolean has_labels = this.hasDebugLabels();
        for (Object key : this.histogram.keySet()) {
            if (!first) s.append("\n");
            String str = null;
            if (has_labels) str = this.debug_names.get(key);
            if (str == null) str = (key != null ? key.toString() : "null");
            int value_str_len = str.length();
            if (value_str_len > max_length) str = str.substring(0, max_length - 3) + "...";

            int max_count = key.toString().startsWith("Q") ? tpch_max_count : tpcc_max_count;
            int cnt = (key != null ? this.histogram.get(key) : 0);
            int chars = (int) ((cnt / (double) max_count) * max_chars);
            s.append(String.format(f, str, cnt));
            for (int i = 0; i < chars; i++) s.append(MARKER);
            first = false;
        } // FOR
        if (this.histogram.isEmpty()) s.append("<EMPTY>");
        return (s.toString());
    }
}
