package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
/**
Filter实现了Operator接口。根据Predicate的判读结果，得到满足条件的tuples。
实现了where age > 18这样的操作。
* */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;// 对Predicate封装，通过predicate实现对每一个tuple的过滤操作
    private OpIterator child;// 待过滤的tuples的迭代器
    private TupleDesc tupleDesc;
    private Iterator<Tuple> it;// 最终的所有过滤结果是保存到it
    private final List<Tuple> childTuple = new ArrayList<>();

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    //返回待过滤元组的属性
    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()) {
            Tuple next = child.next();
            if(predicate.filter(next)) childTuple.add(next);
        }
        it = childTuple.iterator();
        super.open();// ??
    }

    public void close() {
        // some code goes here
        child.close();
        it = null;
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it = childTuple.iterator();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    //子类需要实现的抽象函数
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(it!=null && it.hasNext()){
            return it.next();
        }
        return null;
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
