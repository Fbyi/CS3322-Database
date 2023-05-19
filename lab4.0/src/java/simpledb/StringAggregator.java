package simpledb;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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
        if (aggreOp != Op.COUNT) {
            throw new UnsupportedOperationException("String onlys supports count operation" + aggreOp);
        }
        this.gbIndex = gbIndex;
        this.agIndex = agIndex;
        this.aggreOp = aggreOp;
        this.td = td;
        this.gbFieldType = gbfieldtype;
        gval2agval = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggreField;
        Field gbField = null;
        Integer newVal;
        aggreField = tup.getField(agIndex);

        if (aggreField.getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("not Type.STRING_TYPE");
        }
        if (originalTd == null) {
            originalTd = tup.getTupleDesc();
        } else if (!originalTd.equals(tup.getTupleDesc())) {
            throw new IllegalArgumentException("tuple tupleDesc inconsisitent");
        }
        if (gbIndex != Aggregator.NO_GROUPING) {
            gbField = tup.getField(gbIndex);
        }

        if (gval2agval.containsKey(gbField)) {
            Integer oldVal = gval2agval.get(gbField);
            newVal = oldVal + 1;
        } else newVal = 1;
        gval2agval.put(gbField, newVal);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> g2a : gval2agval.entrySet()) {
            Tuple t = new Tuple(td);
            if (gbIndex == Aggregator.NO_GROUPING) {
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
