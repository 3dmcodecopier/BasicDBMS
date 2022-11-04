package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> resultMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        resultMap = new HashMap<>();
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("Aggregation Operator Only Supports COUNT");
        else
            this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field tupGbfield = this.gbfield == Aggregator.NO_GROUPING ? null : tup.getField(gbfield);
        StringField tupAfield = (StringField) tup.getField(afield);
        if (!resultMap.containsKey(tupGbfield)) {
            resultMap.put(tupGbfield, 1);
        }
        else {
            resultMap.put(tupGbfield, resultMap.get(tupGbfield) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        List<Tuple> tuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            types = new Type[] {Type.INT_TYPE};
            names = new String[] {"aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0, new IntField(resultMap.get(null)));
            tuples.add(tuple);
        }
        else {
            types = new Type[] {gbfieldtype, Type.INT_TYPE};
            names = new String[] {"groupVal", "aggregateVal"};
            tupleDesc = new TupleDesc(types, names);
            for (Field field : resultMap.keySet()) {
                Tuple tuple = new Tuple(tupleDesc);
                if (field.getType().equals(Type.INT_TYPE)) {
                    tuple.setField(0, (IntField) field);
                }
                else {
                    tuple.setField(0, (StringField) field);
                }
                IntField resultField = new IntField(resultMap.get(field));
                tuple.setField(1, resultField);
                tuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, tuples);
    }

}
