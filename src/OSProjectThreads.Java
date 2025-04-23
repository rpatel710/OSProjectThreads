import java.util.concurrent.Semaphore;

class ReadWriteLock {
    private int readers = 0;
    private final Semaphore mutex = new Semaphore(1);
    private final Semaphore writeLock = new Semaphore(1);

    public void startRead(int id) throws InterruptedException {
        mutex.acquire();
        readers++;
        if (readers == 1) {
            writeLock.acquire();
        }
        System.out.println("[Reader " + id + "] Started Reading. Readers = " + readers);

        mutex.release();
    }

    public void endRead(int id) throws InterruptedException {
        mutex.acquire();
        readers--;
        System.out.println("[Reader " + id + "] Finished Reading. Readers = " + readers);
        if (readers == 0) {
            writeLock.release();
        }
        
        mutex.release();
    }

    public void startWrite(int id) throws InterruptedException {
        System.out.println("[Writer " + id + "] Waiting to write...");
        writeLock.acquire();
        System.out.println("[Writer " + id + "] Started Writing.");
    }

    public void endWrite(int id) throws InterruptedException {
        System.out.println("[Writer " + id + "] Finished Writing.");
        writeLock.release();
    }
}

class Reader extends Thread {
    private final int id;
    private final ReadWriteLock rwLock;

    public Reader(int id, ReadWriteLock rwLock) {
        this.id = id;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startRead(id);
            Thread.sleep(1000); // Simulate reading
            rwLock.endRead(id);
        } catch (InterruptedException e) {
            System.err.println("Reader " + id + " interrupted.");
        }
    }
}

class Writer extends Thread {
    private final int id;
    private final ReadWriteLock rwLock;

    public Writer(int id, ReadWriteLock rwLock) {
        this.id = id;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startWrite(id);
            Thread.sleep(1000); // Simulate writing
            rwLock.endWrite(id);
        } catch (InterruptedException e) {
            System.err.println("Writer " + id + " interrupted.");
        }
    }
}

public class OSProjectThreads {
    public static void main(String[] args) {
        ReadWriteLock rwLock = new ReadWriteLock();

        Reader[] readers = {
            new Reader(1, rwLock),
            new Reader(2, rwLock),
            new Reader(3, rwLock)
        };

        Writer[] writers = {
            new Writer(1, rwLock),
            new Writer(2, rwLock)
        };

        readers[0].start();
        writers[0].start();
        readers[1].start();
        writers[1].start();
        readers[2].start();
    }
}