package simpledb;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    int afield;
    TupleDesc originalTd;
    TupleDesc td;
    Type gbfieldtype;
    Op what;
    HashMap<Field, Integer> gval2agval;
    HashMap<Field, Integer[]> gval2count_sum;

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
        this.td=td;
        gval2agval = new HashMap<>();
        gval2count_sum = new HashMap<>();
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
        Field aggreField;
        Field gbField = null;
        Integer newVal;
        aggreField = tup.getField(afield);
        int toAggregate;
        if (aggreField.getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("not Type.INT_TYPE");
        }
        toAggregate = ((IntField) aggreField).getValue();
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("tuple inconsistent with tupleDesc");
        }
        if (gbfield != Aggregator.NO_GROUPING) {

            gbField = tup.getField(gbfield);
        }
        if (what == Op.AVG) {
            if (gval2count_sum.containsKey(gbField)) {
                Integer[] oldCountAndSum = gval2count_sum.get(gbField);
                int oldCount = oldCountAndSum[0];
                int oldSum = oldCountAndSum[1];
                gval2count_sum.put(gbField, new Integer[]{oldCount + 1, oldSum + toAggregate});
            } else {
                gval2count_sum.put(gbField, new Integer[]{1, toAggregate});
            }
        
            Integer[] c2s=gval2count_sum.get(gbField);
            int currentCount = c2s[0];
            int currentSum = c2s[1];
            gval2agval.put(gbField, currentSum / currentCount);
            return;
        }

        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = calcuNewValue(oldVal, toAggregate, what);
        } else if (what == Op.COUNT) {
            newVal = 1;
        } else {
            newVal = toAggregate;
        }
        gval2agval.put(gbField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gval2agval.entrySet()) {
            Tuple t = new Tuple(td);
            if (gbfield == Aggregator.NO_GROUPING) {
                t.setField(0, new IntField(g2a.getValue()));
            } else {
                t.setField(0, g2a.getKey());
                t.setField(1, new IntField(g2a.getValue()));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
