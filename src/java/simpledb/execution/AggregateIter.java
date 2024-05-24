package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

public class AggregateIter implements OpIterator {
    private Iterator<Tuple> tupleIterator;
    private Map<Field, List<Field>> group;
    private List<Tuple> resultSet;//用来存储聚合后的数据
    private Aggregator.Op what;
    private Type gbFieldType;
    private TupleDesc tupleDesc;//a OpIterator whose tuples are the pair (groupVal, aggregateVal)
    private  int gbFieldIndex;

    public AggregateIter(Map<Field,List<Field>> group,int gbFieldIndex,Type gbFieldType,Aggregator.Op what){
        this.group = group;
        this.gbFieldType = gbFieldType;
        this.what = what;
        this.gbFieldIndex = gbFieldIndex;
        Type[] type;
        if(this.gbFieldIndex!=-1){
            type = new Type[2];
            type[0] = gbFieldType;
            type[1] = Type.INT_TYPE;
        }else{
            type = new Type[1];
            type[0] = Type.INT_TYPE;
        }
        this.tupleDesc = new TupleDesc(type);
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.resultSet = new ArrayList<>();
        if(what == Aggregator.Op.COUNT) {
            for(Field groupField : group.keySet()) {
                Tuple tuple = new Tuple(this.tupleDesc);
                if(groupField != null) {
                    tuple.setField(0, groupField);
                    tuple.setField(1, new IntField(group.get(groupField).size()));
                } else {
                    //此情况表示没有设置分组
                    tuple.setField(0,new IntField(group.get(groupField).size()));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.MIN) {
            for(Field groupField : group.keySet()) {
                int min = Integer.MAX_VALUE;
                Tuple tuple = new Tuple(this.tupleDesc);
                for(int i = 0; i < this.group.get(groupField).size(); i++) {
                    IntField field = (IntField) group.get(groupField).get(i);
                    if(field.getValue() < min) min = field.getValue();
                }
                if(groupField != null) {
                    tuple.setField(0,groupField);
                    tuple.setField(1,new IntField(min));
                } else {
                    tuple.setField(0,new IntField(min));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.MAX) {
            for(Field groupField : group.keySet()) {
                int max = Integer.MIN_VALUE;
                Tuple tuple = new Tuple(this.tupleDesc);
                for(int i = 0; i < this.group.get(groupField).size(); i++) {
                    IntField field = (IntField) group.get(groupField).get(i);
                    if(field.getValue() > max) max = field.getValue();
                }
                if(groupField != null) {
                    tuple.setField(0,groupField);
                    tuple.setField(1,new IntField(max));
                } else {
                    tuple.setField(0,new IntField(max));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.AVG) {
            for(Field groupfield: this.group.keySet()){
                int sum = 0;
                int size = this.group.get(groupfield).size();
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<size;i++){
                    IntField field1 = (IntField) group.get(groupfield).get(i);
                    sum += field1.getValue();
                }
                if(groupfield!=null){
                    tuple.setField(0,groupfield);
                    tuple.setField(1,new IntField(sum/size));
                }else{
                    tuple.setField(0,new IntField(sum/size));
                }
                resultSet.add(tuple);
            }
        }else if(what == Aggregator.Op.SUM) {
            for(Field groupfield: this.group.keySet()){
                int sum = 0;
                Tuple tuple = new Tuple(tupleDesc);
                for(int i=0;i<this.group.get(groupfield).size();i++){
                    IntField field1 = (IntField) group.get(groupfield).get(i);
                    sum += field1.getValue();
                }
                if(groupfield!=null){
                    tuple.setField(0,groupfield);
                    tuple.setField(1,new IntField(sum));
                }else{
                    tuple.setField(0,new IntField(sum));
                }
                resultSet.add(tuple);
            }
        }
        this.tupleIterator = resultSet.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(tupleIterator == null){
            return false;
        }
        return tupleIterator.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        if(resultSet != null){
            tupleIterator = resultSet.iterator();
        }
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void close() {
        this.tupleIterator = null;
    }
}
