package simpledb;

import java.util.HashMap;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram{

    int buckets;
    int min;
    int max;
    int interval;
    int cnt = 0;
    Map<Integer,Integer> hist = new HashMap<>();

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.interval = (int) Math.ceil((max-min+1)*1.0/buckets);
        //System.out.println("IntHistogram :"+buckets+","+min+","+max+","+interval);
        for(int i=0;i<buckets;i++){
            hist.put(i,0);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(Object v) {
    	// some code goes here
        int index = getBucketIndex((Integer) v);
        hist.put(index,hist.get(index)+1);
        cnt ++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, Object v) {

    	// some code goes here
        int val = (int) v;
        int index = getBucketIndex(val);
        double total;
        switch (op){
            case EQUALS:
                if(val<min || val>max){
                    return 0.0;
                }
                return hist.get(index)*1.0/interval/cnt;
            case LESS_THAN:
                if(val<min){
                    return 0.0;
                }
                if(val>max){
                    return 1.0;
                }
                //System.out.println("LESS_THAN: "+hist+","+hist.get(index)+","+v);
                total = (getBucketOffset(val))*1.0/interval*hist.get(index);
                for(int i=0;i<index;i++){
                    total += hist.get(i);
                }
                //System.out.println("val: "+total/cnt);
                return total/cnt;
            case GREATER_THAN:
                if(val<min){
                    return 1.0;
                }
                if(val>max){
                    return 0.0;
                }
                //System.out.println("GREATER_THAN: "+hist+","+hist.get(index)+","+v);
                total = (interval-1-getBucketOffset(val))*1.0/interval*hist.get(index);
                for(int i=index+1;i<buckets;i++){
                    total += hist.get(i);
                }
                //System.out.println("val: "+total/cnt);
                return total/cnt;
            case NOT_EQUALS:
                return 1-estimateSelectivity(Predicate.Op.EQUALS,v);
            case LESS_THAN_OR_EQ:
                return 1-estimateSelectivity(Predicate.Op.GREATER_THAN,v);
            case GREATER_THAN_OR_EQ:
                return 1-estimateSelectivity(Predicate.Op.LESS_THAN,v);
        }
        return 0.0;
    }

    int getBucketIndex(int v){
        int index = (v-min+1)/interval-1;
        if((v-min+1)%interval == 0){
            return index;
        }else {
            return index+1;
        }
    }

    int getBucketOffset(int v){
        int offset = (v-min+1)%interval;
        if(offset == 0){
            return interval-1;
        }else {
            return offset-1;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return hist.toString();
    }
}
