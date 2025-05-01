import java.io.*;
import java.util.*;
import java.util.concurrent.Semaphore;

// Class implementing a Read-Write Lock
class ReadWriteLock {
    private int readers = 0; // Number of active readers
    private int watingWriters = 0; // Number of waiting writers
    private final Semaphore mutex = new Semaphore(1); // Mutex for reader count
    private final Semaphore writeLock = new Semaphore(1); // Semaphore for write access
    private final Semaphore readBlock = new Semaphore(1); // Semaphore to block readers when a writer is waiting

    // Method to start reading
    public void startRead(int id) throws InterruptedException {
        readBlock.acquire(); // Ensure no writers are waiting
        mutex.acquire(); // Lock to update reader count
        readers++; // Increment reader count

        if (readers == 1) {
            writeLock.acquire(); // Block writers if this is the first reader
        }
        mutex.release(); // Release mutex
        readBlock.release(); // Allow other readers to proceed

        System.out.println("[Reader " + id + "] Started Reading. Readers = " + readers);
    }

    // Method to end reading
    public void endRead(int id) throws InterruptedException {
        mutex.acquire(); // Lock to update reader count
        readers--; // Decrement reader count
        System.out.println("[Reader " + id + "] Finished Reading. Readers = " + readers);
        if (readers == 0) {
            writeLock.release(); // Release write lock if this was the last reader
        }
        
        mutex.release(); // Release mutex
    }

    // Method to start writing
    public void startWrite(int id) throws InterruptedException {
        mutex.acquire(); // Lock to update waiting writers count
        watingWriters++; // Increment waiting writers count
        if (watingWriters == 1) {
            readBlock.acquire(); // Block readers if this is the first waiting writer
        }
        mutex.release(); // Release mutex

        System.out.println("[Writer " + id + "] Waiting to write...");
        writeLock.acquire(); // Acquire write lock
        System.out.println("[Writer " + id + "] Started Writing.");
    }

    // Method to end writing
    public void endWrite(int id) throws InterruptedException {
        System.out.println("[Writer " + id + "] Finished Writing.");
        writeLock.release(); // Release write lock

        mutex.acquire(); // Lock to update waiting writers count
        watingWriters--; // Decrement waiting writers count
        if (watingWriters == 0) {
            readBlock.release(); // Release read block if this was the last waiting writer
        }
        mutex.release(); // Release mutex
    }
}

// Class representing a Reader thread
class Reader extends Thread {
    private final int id; // Reader ID
    private final int burstTime; // Time to read
    private final ReadWriteLock rwLock; // Read-Write Lock instance

    public Reader(int id, int burstTime, ReadWriteLock rwLock) {
        this.id = id;
        this.burstTime = burstTime;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startRead(id); // Start reading
            Thread.sleep(burstTime * 1000L); // Simulate reading
            rwLock.endRead(id); // End reading
        } catch (InterruptedException e) {
            System.err.println("Reader " + id + " interrupted.");
        }
    }
}

// Class representing a Writer thread
class Writer extends Thread {
    private final int id;  // Writer ID
    private final int burstTime; // Time to write
    private final ReadWriteLock rwLock; // Read-Write Lock instance

    public Writer(int id, int burstTime, ReadWriteLock rwLock) {
        this.id = id;
        this.burstTime = burstTime;
        this.rwLock = rwLock;
    }

    public void run() {
        try {
            rwLock.startWrite(id); // Start writing
            Thread.sleep(burstTime * 1000L); // Simulate writing
            rwLock.endWrite(id); // End writing
        } catch (InterruptedException e) {
            System.err.println("Writer " + id + " interrupted.");
        }
    }
}

public class OSProjectThreads {
    public static void main(String[] args) {
        ReadWriteLock rwLock = new ReadWriteLock(); // Create a new Read-Write Lock
        List<Thread> threads = new ArrayList<>(); // List to hold threads

        // Read process information from a file
        try (BufferedReader br = new BufferedReader(new FileReader("lib/processes.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.trim().split("\\s+"); // Split line into parts
                if (parts.length < 3) continue; // Skip invalid lines

                String type = parts[0]; // Type of thread (Reader or Writer)
                int id = Integer.parseInt(parts[1]); // Thread ID
                int burst = Integer.parseInt(parts[2]); // Burst time

                // Create Reader or Writer thread based on type
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

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {}
        }

        System.out.println("All threads have finished execution.");
    }
}