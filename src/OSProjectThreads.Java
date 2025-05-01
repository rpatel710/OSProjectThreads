import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;

class ReadWriteLock {
    private int readers = 0;
    private int watingWriters = 0;
    private final Semaphore mutex = new Semaphore(1);
    private final Semaphore writeLock = new Semaphore(1);
    private final Semaphore readBlock = new Semaphore(1);

    public void startRead(int id) throws InterruptedException {
        readBlock.acquire();
        mutex.acquire();
        readers++;
        if (readers == 1) {
            writeLock.acquire();
        }
        mutex.release();
        readBlock.release();

        System.out.println("[Reader " + id + "] Started Reading. Readers = " + readers);
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
        mutex.acquire();
        watingWriters++;
        if (watingWriters == 1) {
            readBlock.acquire();
        }
        mutex.release();

        System.out.println("[Writer " + id + "] Waiting to write...");
        writeLock.acquire();
        System.out.println("[Writer " + id + "] Started Writing.");
    }

    public void endWrite(int id) throws InterruptedException {
        System.out.println("[Writer " + id + "] Finished Writing.");
        writeLock.release();

        mutex.acquire();
        watingWriters--;
        if (watingWriters == 0) {
            readBlock.release();
        }
        mutex.release();
    }
}

class Reader extends Thread {
    private final int id;
    private final ReadWriteLock rwLock;

    public Reader(int id, int burstTime, ReadWriteLock rwLock) {
        this.id = id;
        this.burstTime = burstTime;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startRead(id);
            Thread.sleep(burstTime * 1000L); // Simulate reading
            rwLock.endRead(id);
        } catch (InterruptedException e) {
            System.err.println("Reader " + id + " interrupted.");
        }
    }
}

class Writer extends Thread {
    private final int id;
    private final ReadWriteLock rwLock;

    public Writer(int id, int burstTime, ReadWriteLock rwLock) {
        this.id = id;
        this.burstTime = burstTime;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startWrite(id);
            Thread.sleep(burstTime * 1000L); // Simulate writing
            rwLock.endWrite(id);
        } catch (InterruptedException e) {
            System.err.println("Writer " + id + " interrupted.");
        }
    }
}

public class OSProjectThreads {
    public static void main(String[] args) {
        ReadWriteLock rwLock = new ReadWriteLock();
        List<Thread> threads = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("lib/processes.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;

                String type = parts[0];
                int id = Integer.parseInt(parts[1]);
                int burst = Integer.parseInt(parts[2]);

                if (type.equalsIgnoreCase("R")) {
                    threads.add(new Reader(id, burst, rwLock));
                } else if (type.equalsIgnoreCase("W")) {
                    threads.add(new Writer(id, burst, rwLock));
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading processes file: " + e.getMessage());
            return;
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {}
        }

        System.out.println("All threads have finished execution.");
    }
}