package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
//实现连接的条件，和Predicate类似 ，是JoinPredicate的辅助类，对两个tuple中的 某一字段进行比较。
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int field1No;// tuple1中进行比较的字段的序号
    private Predicate.Op op;// 比较逻辑
    private int field2No;// tuple2中进行比较的字段的序号

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1No = field1;
        this.op = op;
        this.field2No = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(field1No).compare(op, t2.getField(field2No));
    }
    
    public int getField1()
    {
        // some code goes here
        return field1No;
    }
    
    public int getField2()
    {
        // some code goes here
        return field2No;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
