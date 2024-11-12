package be.panako.strategy.panako.storage;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

public class PanakoStorageScylla implements PanakoStorage {

    /**
     * The single instance of the storage.
     */
    private static final PanakoStorageScylla instance = new PanakoStorageScylla();
    private final CqlSession session;
    private final PreparedStatement storageStatement;
    private final PreparedStatement queryStatement;
    private final PreparedStatement deleteStatement;
    private final String statisticsStatement;

    private final BlockingQueue<List<Long>> storeQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Long> queryQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Integer> deleteQueue = new LinkedBlockingQueue<>();

    public static PanakoStorage getInstance() {
        return instance;
    }

    /**
     * Create a new storage instance
     */
    public PanakoStorageScylla() {
        session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("51.250.47.177", 9042))
                .withLocalDatacenter("scylla_data_center")
                .withAuthCredentials("cassandra", "Axg7na0w6HTL5yw")
                .build();

        storageStatement = session.prepare(
                "INSERT INTO test.metadata (resource_id, resource_path, duration, num_fingerprints) VALUES (?, ?, ?, ?)");
        queryStatement = session.prepare(
                "SELECT resource_id, resource_path, duration, num_fingerprints FROM test.metadata WHERE resource_id = ?");
        deleteStatement = session.prepare("DELETE FROM test.metadata WHERE resource_id = ?");
        statisticsStatement = "SELECT count(resource_id), sum(duration), sum(num_fingerprints) FROM test.metadata";
    }

    @Override
    public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprints) {
        session.execute(storageStatement.bind(resourceID, resourcePath, (double) duration, fingerprints));
    }

    @Override
    public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
        Long[] data = {fingerprintHash, (long) resourceIdentifier, (long) t1, (long) f1};
        storeQueue.add(Arrays.asList(data));
    }

    @Override
    public void processStoreQueue() {
        List<List<Long>> queueLocal = new ArrayList<>();
        storeQueue.drainTo(queueLocal);

        for (List<Long> data : queueLocal) {
            session.execute(storageStatement.bind(data.get(0),
                    data.get(1),
                    data.get(2).intValue(),
                    data.get(3).intValue()));
        }
    }

    @Override
    public PanakoResourceMetadata getMetadata(long identifier) {
        PanakoResourceMetadata metadata = null;
        ResultSet resultSet = session.execute(queryStatement.bind(identifier));

        Row row = resultSet.one();
        if (row != null) {
            metadata = new PanakoResourceMetadata();
            metadata.identifier = row.getLong("resource_id");
            metadata.path = row.getString("resource_path");
            metadata.duration = row.getDouble("duration");
            metadata.numFingerprints = row.getInt("num_fingerprints");
        }

        return metadata;
    }

    @Override
    public void printStatistics(boolean detailedStats) {
        ResultSet resultSet = session.execute(statisticsStatement);

        double totalDuration = 0;
        long totalPrints = 0;
        long totalResources = 0;

        Row row = resultSet.one();
        if (row != null) {
            totalResources = row.getInt(0);
            totalDuration = row.getDouble(1);
            totalPrints = row.getInt(2);
        }

        System.out.printf("Database statistics\n");
        System.out.printf("=========================\n");
        System.out.printf("> %d audio files \n", totalResources);
        System.out.printf("> %.3f seconds of audio\n", totalDuration);
        System.out.printf("> %d fingerprint hashes \n", totalPrints);
        System.out.printf("=========================\n\n");
    }

    @Override
    public void deleteMetadata(long resourceID) {
        session.execute(deleteStatement.bind(resourceID));
    }

    @Override
    public void addToQueryQueue(long queryHash) {
        queryQueue.add(queryHash);
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range) {
        processQueryQueue(matchAccumulator, range, new HashSet<>());
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range,
                                  Set<Integer> resourcesToAvoid) {
        List<Long> batch = new ArrayList<>();
        queryQueue.drainTo(batch);

        for (Long originalKey : batch)
        {
            String query = "SELECT fingerprintHash, resource_id, t1, f1 from test.fingerprints WHERE fingerprintHash IN (";

            String hashes = "";
            boolean first = true;

            //Generate string for IN statement
            for (long r = originalKey - range; r <= originalKey + range; r++) {
                if (first)
                    first = false;
                else
                    hashes += ", ";
                hashes += Long.toString(r);
            }

            query += hashes + ")";

            ResultSet resultSet = session.execute(
                SimpleStatement.newInstance(query)
            );

            for (Row row : resultSet) {
                if (row != null) {
                    long fingerprintHash = row.getLong("fingerprintHash");
                    long resourceID = row.getLong("resource_id");
                    long t = row.getInt("t1");
                    long f = row.getInt("f1");

                    if (!resourcesToAvoid.contains((int) resourceID)) {
                        if (!matchAccumulator.containsKey(originalKey)) {
                            matchAccumulator.put(originalKey, new ArrayList<>());
                        }
                        matchAccumulator.get(originalKey).add(new PanakoHit(originalKey, fingerprintHash, t, resourceID, f));
                    }
                }

            }
        }
    }

    @Override
    public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) {
        deleteQueue.add(resourceIdentifier);
    }

    @Override
    public void processDeleteQueue() {
        List<Integer> queueLocal = new ArrayList<>();
        deleteQueue.drainTo(queueLocal);

        for (long id : queueLocal) {
            session.execute(deleteStatement.bind(id));
        }
    }

    @Override
    public void clear() {
       session.close();
    }
}
