package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbFieldIndex;// 分组字段的序号
    private Type gbFieldType;// 分组字段的类型 (product_id)
    private int aggFieldIndex;// 聚合字段的序号 (quantity)
    private Op what;
    private Map<Field, List<Field>> group;// groupField 到 aggField 的映射

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
        this.gbFieldIndex = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aggFieldIndex = afield;
        this.what = what;
        this.group = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field aggField = tup.getField(aggFieldIndex);
        Field groupField = null;
        if(this.gbFieldIndex != -1) groupField = tup.getField(gbFieldIndex);
        if(group.containsKey(groupField)) {
            group.get(groupField).add(aggField);
        }
        else {
            List<Field> list = new ArrayList<>();
            list.add(aggField);
            group.put(groupField, list);
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
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new AggregateIter(group, gbFieldIndex, gbFieldType, what);
    }

}
