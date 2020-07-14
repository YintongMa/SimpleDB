package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

public class LockManager {

    public Map<PageId, ReadWriteLock> pageIdToLock = new ConcurrentHashMap<>();
    public Map<PageId,PageLock> pageIdToSLock = new ConcurrentHashMap<>();
    public Map<PageId,PageLock> pageIdToXLock = new ConcurrentHashMap<>();

    public Map<PageLock,Set<TransactionId>> pageLockToTids = new ConcurrentHashMap<>();
    public Map<TransactionId,Set<PageLock>> tidToPageLocks = new ConcurrentHashMap<>();
    public Map<PageLock,Set<TransactionId>> pageLockToExpectedTid = new ConcurrentHashMap<>();



    Lock xlock = new ReentrantReadWriteLock().writeLock();

    class PageLock {

        final PageId pageId;
        Lock lock;
        final Permissions perm;

        public PageLock(PageId pageId,Permissions perm,ReadWriteLock readWriteLock){
            this.pageId = pageId;
            this.perm = perm;
            if(perm.equals(Permissions.READ_ONLY)){
                this.lock = readWriteLock.readLock();
            }else{
                this.lock = readWriteLock.writeLock();
            }
        }
        public PageLock(PageId pageId,Permissions perm){
            this.pageId = pageId;
            this.perm = perm;
        }

        public void lock() {
            this.lock.lock();
        }

        public void unlock(){
            this.lock.unlock();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PageLock pageLock = (PageLock) o;
            return Objects.equals(pageId, pageLock.pageId) &&
                    Objects.equals(perm, pageLock.perm);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pageId,perm);
        }

        @Override
        public String toString() {
            return "PageLock{" +
                    "pageId=" + pageId +
                    ", lock=" + lock +
                    ", perm=" + perm +
                    '}';
        }
    }

    public PageLock acquireLock(TransactionId tid, PageId pid,Permissions perm){
        xlock.lock();
//        System.out.println("pageIdToLock size: "+pageIdToLock.size());
//        System.out.println("pageIdToLock content: "+pageIdToLock);
        PageLock pageLock;
        Map<PageId,PageLock> pageIdToPageLock;

        if(!pageIdToLock.containsKey(pid)) {
            ReadWriteLock readWriteLock = new StampedLock().asReadWriteLock();
            pageIdToLock.put(pid,readWriteLock);
            pageIdToSLock.put(pid, new PageLock(pid,Permissions.READ_ONLY,readWriteLock));
            pageIdToXLock.put(pid, new PageLock(pid,Permissions.READ_WRITE,readWriteLock));
        }

        if(perm.equals(Permissions.READ_ONLY)){
            pageIdToPageLock = pageIdToSLock;
        }else {
            pageIdToPageLock = pageIdToXLock;
        }

        pageLock = pageIdToPageLock.get(pid);

        //check lock upgrade


        PageLock rlock = new PageLock(pid,Permissions.READ_ONLY);
        PageLock wlock = new PageLock(pid,Permissions.READ_WRITE);


        //r -> w
        Set<TransactionId> txIds = pageLockToTids.get(rlock);
        Set<TransactionId> expectedTxIds = pageLockToExpectedTid.get(rlock);
        if(txIds != null
                && perm.equals(Permissions.READ_WRITE)
                && txIds.contains(tid)
                && txIds.size() == 1
                && expectedTxIds == null)
        {
           // System.out.println("lock: "+pageLock);

            //upgrade cpy begin
           // System.out.println("pageIdToSLock: "+pageIdToSLock);

            Set<TransactionId> transactionIds;
            if(pageLockToTids.containsKey(pageLock)) {
                transactionIds = pageLockToTids.get(pageLock);
            }else {
                transactionIds = new HashSet<>();
                pageLockToTids.put(pageLock,transactionIds);
            }
            transactionIds.add(tid);


            Set<PageLock> pageLocks;
            if(tidToPageLocks.containsKey(tid)) {
                pageLocks = tidToPageLocks.get(tid);
            }else {
                pageLocks = new HashSet<>();
                tidToPageLocks.put(tid,pageLocks);
            }
            pageLocks.add(pageLock);

            //upgrade cpy end

            //remove rlock
            tidToPageLocks.get(tid).remove(rlock);
            pageLockToTids.get(rlock).remove(tid);

//            System.out.println("pageIdToSLock2: "+pageIdToSLock);
//            System.out.println("pageIdToSLock3: "+pageIdToSLock.get(pid));
            pageIdToSLock.get(pid).unlock();

            pageLock.lock();
            xlock.unlock();
            return pageLock;

        }else {
            //w -> w/r
            txIds = pageLockToTids.get(wlock);
            if(txIds != null
                    && txIds.contains(tid)
                    && txIds.size() == 1)
            {
//                System.out.println("here: ");
                xlock.unlock();
                return pageIdToXLock.get(pid);
            }


        }

        //multi thread
        if(pageLockToExpectedTid.containsKey(pageLock)) {
            expectedTxIds = pageLockToExpectedTid.get(pageLock);
        }else {
            expectedTxIds = new HashSet<>();
            pageLockToExpectedTid.put(pageLock,expectedTxIds);
        }
        expectedTxIds.add(tid);

        xlock.unlock();

        pageLock.lock();

        xlock.lock();

        //manage lock metadata

        //pageLockToTids

        Set<TransactionId> transactionIds;
        if(pageLockToTids.containsKey(pageLock)) {
            transactionIds = pageLockToTids.get(pageLock);
        }else {
            transactionIds = new HashSet<>();
            pageLockToTids.put(pageLock,transactionIds);
        }
        transactionIds.add(tid);


        Set<PageLock> pageLocks;
        if(tidToPageLocks.containsKey(tid)) {
            pageLocks = tidToPageLocks.get(tid);
        }else {
            pageLocks = new HashSet<>();
            tidToPageLocks.put(tid,pageLocks);
        }
        pageLocks.add(pageLock);


        //pageLockToTids
        pageLockToExpectedTid.get(pageLock).remove(tid);
        if(pageLockToExpectedTid.get(pageLock).size() == 0){
            pageLockToExpectedTid.remove(pageLock);
        }

        xlock.unlock();
        return pageLock;
    }

    public void releaseLock(TransactionId tid, PageId pid){
        xlock.lock();

        Set<PageLock> pageLocks = tidToPageLocks.get(tid);
        Set<PageLock> newPageLocks = new HashSet<>();
        for(PageLock pageLock : pageLocks){
            if(pageLock.pageId.equals(pid)){
                pageLock.unlock();
//                pageLocks.remove(pageLock);
                pageLockToTids.get(pageLock).remove(tid);
                if(pageLockToTids.get(pageLock).size() == 0){
                    pageLockToTids.remove(pageLock);
                }
            }else {
                newPageLocks.add(pageLock);
            }
        }

        if(newPageLocks.size() == 0){
            tidToPageLocks.remove(tid);
        }else {
            tidToPageLocks.put(tid, newPageLocks);
        }
        xlock.unlock();
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //xlock.lock();
        boolean flag = false;
        PageLock pageLock;
        pageLock = pageIdToSLock.get(pid);
        if(pageLock != null){
            if(pageLockToTids.get(pageLock).contains(tid)){
                flag = true;
            }
        }

        pageLock = pageIdToXLock.get(pid);
        if(pageLock != null){
            if(pageLockToTids.get(pageLock).contains(tid)){
                flag = true;
            }
        }
       // xlock.unlock();
        return flag;
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsPotentialLock(PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //xlock.lock();
        boolean flag = false;
        PageLock pageLock;
        pageLock = pageIdToSLock.get(pid);
        if(pageLock != null){
            if(pageLockToTids.containsKey(pageLock)){
                flag = true;
            }
        }

        pageLock = pageIdToXLock.get(pid);
        if(pageLock != null){
            if(pageLockToTids.containsKey(pageLock)){
                flag = true;
            }
        }

        if(pageLockToExpectedTid.containsKey(new PageLock(pid,Permissions.READ_WRITE)) || pageLockToExpectedTid.containsKey(new PageLock(pid,Permissions.READ_ONLY))){
            flag = true;
        }

       // xlock.unlock();
        return flag;
    }
}
