package simpledb.transaction;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 锁管理器
 */
public class LockManager {
    //key：页id，value：作用于该页的所有lock
    private Map<Integer, List<Lock>> lockCache;

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
}
