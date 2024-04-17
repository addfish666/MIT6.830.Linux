package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int aggFieldIndex;
    private int groupFieldIndex;
    private Aggregator.Op aop;
    private final Aggregator aggregator;
    private OpIterator opIterator; // a OpIterator over group aggregate results.

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggFieldIndex = afield;
        this.groupFieldIndex = gfield;
        this.aop = aop;
        Type aggFieldType = child.getTupleDesc().getFieldType(aggFieldIndex);
        Type groupFieldType = child.getTupleDesc().getFieldType(groupFieldIndex);
        if(groupFieldIndex != Aggregator.NO_GROUPING) {
            if(aggFieldType.equals(Type.INT_TYPE)) {
                aggregator = new IntegerAggregator(groupFieldIndex, groupFieldType , aggFieldIndex, aop);
            } else if(aggFieldType.equals(Type.STRING_TYPE)) {
                aggregator = new StringAggregator(groupFieldIndex, groupFieldType , aggFieldIndex, aop);
            } else {
                aggregator = null;
            }
        } else {
            if(aggFieldType.equals(Type.INT_TYPE)){
                aggregator = new IntegerAggregator(gfield,null,afield,aop);
            }else if(aggFieldType.equals(Type.STRING_TYPE)){
                aggregator = new StringAggregator(gfield,null,afield,aop);
            }else{
                aggregator = null;
            }
        }
        this.opIterator = aggregator.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if(groupFieldIndex==-1){
            return Aggregator.NO_GROUPING;
        }
        return groupFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if(groupFieldIndex==Aggregator.NO_GROUPING){
            return null;
        }
        try{
            String fieldName = child.getTupleDesc().getFieldName(groupFieldIndex);
            return fieldName;
        }catch (NoSuchElementException e){
            return null;
        }
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        try{
            String fieldName = child.getTupleDesc().getFieldName(aggFieldIndex);
            return fieldName;
        }catch (NoSuchElementException e) {
            return null;
        }
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while(child.hasNext()) {
            Tuple next = child.next();
            aggregator.mergeTupleIntoGroup(next);
        }
        opIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(opIterator != null && opIterator.hasNext()) {
            return opIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.rewind();
        child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return opIterator.getTupleDesc();
    }

    public void close() {
        // some code goes here
        child.close();
        opIterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
