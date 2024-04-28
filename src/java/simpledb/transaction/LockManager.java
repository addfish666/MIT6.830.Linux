package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器
 */
public class LockManager {
    //key：页id，value：作用于该页的所有lock
//    private Map<Integer, List<Lock>> lockCache;
    private Map<PageId, List<Lock>> lockCache;

    public LockManager() {
        this.lockCache = new ConcurrentHashMap<>();
    }
    /**
     * 获取锁
     * @param tid
     * @param pageId
     * @param permissions
     * @return
     */
//    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions) {
//        Lock lock = new Lock(tid, permissions);
//        int pid = pageId.getPageNumber();
//        List<Lock> locks = lockCache.get(pid);
//        // 该页上不存在锁，直接获取
//        if(locks == null || locks.size() == 0) {
//            locks = new ArrayList<>();
//            locks.add(lock);
//            lockCache.put(pid, locks);
//            return true;
//        }
//        if(locks.size() == 1) {
//            //当只有一个事务抢占锁
//            Lock curLock = locks.get(0);
//            // 判断是否是自己的锁
//            if(curLock.getTransactionId().equals(tid)) {
////                // 判断自己加的是什么锁，加读锁直接返回
////                if(permissions.equals(Permissions.READ_ONLY)) return true;
////                else {
////                    // 加写锁需要进行锁升级
////                    if(curLock.getPermissions().equals(Permissions.READ_ONLY)) {
////                        curLock.setPermissions(Permissions.READ_WRITE);
////                        return true;
////                    }
////                }
////                return true;
//                //判断是否进行锁升级
//                if(curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_WRITE)){
//                    curLock.setPermissions(Permissions.READ_WRITE);
//                }
//                return true;
//            }
//            else {
//                if(curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_ONLY)) {
//                    locks.add(lock);
//                    return true;
//                }
//                return false;
//            }
//        }
//        //当有多个事务抢占锁，说明必然是多个读事务
//        if(lock.getPermissions().equals(Permissions.READ_WRITE)) return false;
//        for(Lock l : locks) {
//            if(l.getTransactionId().equals(lock.getTransactionId())) return true;
//        }
//        locks.add(lock);
//        return true;
//    }

    public synchronized Boolean acquireLock(TransactionId tid, PageId pageId, Permissions permissions) {
        Lock lock = new Lock(tid, permissions);
        List<Lock> locks = lockCache.get(pageId);
        // 该页上不存在锁，直接获取
        if(locks == null || locks.size() == 0) {
            locks = new ArrayList<>();
            locks.add(lock);
            lockCache.put(pageId, locks);
            return true;
        }
        if(locks.size() == 1) {
            //当只有一个事务抢占锁
            Lock curLock = locks.get(0);
            // 判断是否是自己的锁
            if(curLock.getTransactionId().equals(tid)) {
//                // 判断自己加的是什么锁，加读锁直接返回
//                if(permissions.equals(Permissions.READ_ONLY)) return true;
//                else {
//                    // 加写锁需要进行锁升级
//                    if(curLock.getPermissions().equals(Permissions.READ_ONLY)) {
//                        curLock.setPermissions(Permissions.READ_WRITE);
//                        return true;
//                    }
//                }
//                return true;
                //判断是否进行锁升级
                if(curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_WRITE)){
                    curLock.setPermissions(Permissions.READ_WRITE);
                }
                return true;
            }
            else {
                if(curLock.getPermissions().equals(Permissions.READ_ONLY) && lock.getPermissions().equals(Permissions.READ_ONLY)) {
                    locks.add(lock);
                    return true;
                }
                return false;
            }
        }
        //当有多个事务抢占锁，说明必然是多个读事务
        if(lock.getPermissions().equals(Permissions.READ_WRITE)) return false;
        for(Lock l : locks) {
            if(l.getTransactionId().equals(lock.getTransactionId())) return true;
        }
        locks.add(lock);
        return true;
    }

    /**
     * 释放锁
     * @param tid
     * @param pageId
     */
//    public synchronized void releaseLock(TransactionId tid,PageId pageId){
//        int pid = pageId.getPageNumber();
//        List<Lock> locks = lockCache.get(pid);
//        for(Lock l : locks) {
//            if(l.getTransactionId().equals(tid)) {
//                locks.remove(l);
//                if(locks.size() == 0) lockCache.remove(pid);
//            }
//            return;
//        }
//    }

    public synchronized void releaseLock(TransactionId tid,PageId pageId){
        List<Lock> locks = lockCache.get(pageId);
        for(Lock l : locks) {
            if(l.getTransactionId().equals(tid)) {
                locks.remove(l);
                if(locks.size() == 0) lockCache.remove(pageId);
            }
            return;
        }
    }

    /**
     * 释放当前事务的所有锁
     * @param tid
     */
//    public synchronized void releaseAllLock(TransactionId tid){
//        for(Integer pid : lockCache.keySet()) {
//            List<Lock> locks = lockCache.get(pid);
//            for(Lock l : locks) {
//                if(l.getTransactionId().equals(tid)) {
//                    locks.remove(l);
//                    if(locks.size() == 0) lockCache.remove(pid);
//                }
//                break;
//            }
//        }
//    }

    public synchronized void releaseAllLock(TransactionId tid){
        for(PageId pageId : lockCache.keySet()) {
            List<Lock> locks = lockCache.get(pageId);
            for(Lock l : locks) {
                if(l.getTransactionId().equals(tid)) {
                    locks.remove(l);
                    if(locks.size() == 0) lockCache.remove(pageId);
                }
                break;
            }
        }
    }

    /**
     * 判断是否持有锁
     * @param tid
     * @param pageId
     * @return
     */
//    public synchronized Boolean holdsLock(TransactionId tid,PageId pageId){
//        int pid = pageId.getPageNumber();
//        List<Lock> locks = lockCache.get(pid);
//        for(Lock l : locks) {
//            if(l.getTransactionId().equals(tid)) {
//                return true;
//            }
//        }
//        return false;
//    }
    public synchronized Boolean holdsLock(TransactionId tid,PageId pageId){
        List<Lock> locks = lockCache.get(pageId);
        for(Lock l : locks) {
            if(l.getTransactionId().equals(tid)) {
                return true;
            }
        }
        return false;
    }
}
