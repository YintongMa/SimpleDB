package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    Map<Field,Integer> groupValToAggregateVal;
    Map<Field,Integer> groupValToGroupSize;
    TupleDesc tupleDesc;
    Field fieldHolder = new IntField(0);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupValToAggregateVal = new HashMap<>();
        this.groupValToGroupSize = new HashMap<>();
        if(gbfield == NO_GROUPING){
            this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        }else{
            this.tupleDesc = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE});
        }


    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupkey;
        if(gbfield == NO_GROUPING){
            groupkey = fieldHolder;
        }else{
            groupkey = tup.getField(gbfield);
        }
        int tupleVal = ((IntField)tup.getField(afield)).getValue();
        if(groupValToAggregateVal.containsKey(groupkey)){
            groupValToGroupSize.put(groupkey,groupValToGroupSize.get(groupkey)+1);
            Integer aggregateVal = groupValToAggregateVal.get(groupkey);
            switch (what){
                case MIN:
                    groupValToAggregateVal.put(groupkey,Math.min(aggregateVal,tupleVal));
                    break;
                case MAX:
                    groupValToAggregateVal.put(groupkey,Math.max(aggregateVal,tupleVal));
                    break;
                case SUM :
                case AVG:
                    groupValToAggregateVal.put(groupkey,aggregateVal+tupleVal);
                    break;
                case COUNT:
                    groupValToAggregateVal.put(groupkey,aggregateVal+1);
                    break;
            }

        }else {
            Integer aggregateBase = null;
            switch (what){
                case MIN:
                case SUM:
                case AVG:
                case MAX:
                    aggregateBase = tupleVal;
                    break;
                case COUNT:
                    aggregateBase = 1;
            }
            groupValToAggregateVal.putIfAbsent(groupkey,aggregateBase);
            groupValToGroupSize.putIfAbsent(groupkey,1);
        }

        //System.out.println("afetr input tuple: "+groupValToAggregateVal);

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        return new OpIterator(){
            Iterator<Tuple> it;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                List<Tuple> aggregateRets = new ArrayList<>();
                for(Map.Entry<Field,Integer> entry : groupValToAggregateVal.entrySet()){
                    Integer val = null;
                    switch (what){
                        case MIN:
                        case SUM:
                        case COUNT:
                        case MAX:
                            val = entry.getValue();
                            break;
                        case AVG:
                            val = entry.getValue()/groupValToGroupSize.get(entry.getKey());
                    }
                    if(gbfield == NO_GROUPING){
                        aggregateRets.add(new Tuple(tupleDesc).setFields(Arrays.asList(new Field[]{new IntField(val)})));
                    }else{
                        aggregateRets.add(new Tuple(tupleDesc).setFields(Arrays.asList(new Field[]{entry.getKey(), new IntField(val)})));
                    }

                }
                it = aggregateRets.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                //System.out.println("next aggreted tuple: "+tuple);
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return tupleDesc;
            }

            @Override
            public void close() {

            }
        };
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
    }

}
