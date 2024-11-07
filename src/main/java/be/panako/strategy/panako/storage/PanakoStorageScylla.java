package be.panako.strategy.panako.storage;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import be.panako.cli.Application;
import be.panako.util.Config;
import be.panako.util.FileUtils;
import be.panako.util.Key;

public class PanakoStorageScylla implements PanakoStorage {

	/**
	 * The single instance of the storage.
	 */
	private static PanakoStorageScylla instance;

	/**
	 * A mutex for synchronization purposes
	 */
	private static final Object mutex = new Object();

	/**
	 * Uses a singleton pattern.
	 * @return Returns or creates a storage instance. This should be a thread
	 *         safe operation.
	 */
	public synchronized static PanakoStorageScylla getInstance() {
		if (instance == null) {
			synchronized (mutex) {
				if (instance == null) {
					instance = new PanakoStorageScylla();
				}
			}
		}
		return instance;
	}
	
	final Map<Long,List<long[]>> storeQueue;
	final Map<Long,List<long[]>> deleteQueue;
	final Map<Long,List<Long>> queryQueue;

    CqlSession session;

	/**
	 * Create a new storage instance
	 */
	public PanakoStorageScylla() 
    {		
       session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1") // Replace with your datacenter name
                .build();

		storeQueue = new HashMap<Long,List<long[]>>();
		deleteQueue = new HashMap<Long,List<long[]>>();
		queryQueue = new HashMap<Long,List<Long>>();
	}

    /**
	 * Closes the database environment.
	 */
	public void close() 
    {
		session.close();
	}

    @Override
    public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprints) 
    {
        String insertQuery = "INSERT INTO metadata (resource_id, resource_path, duration, num_fingerprints) VALUES (?, ?, ?, ?)";

        session.execute(
            SimpleStatement.newInstance(insertQuery, resourceID, resourcePath, duration, fingerprints)
        );
    }

    @Override
    public void addToStoreQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) 
    {
		long[] data = {fingerprintHash,resourceIdentifier,t1,f1};
		long threadID = Thread.currentThread().threadId();
		if(!storeQueue.containsKey(threadID))
			storeQueue.put(threadID, new ArrayList<long[]>());
		storeQueue.get(threadID).add(data);
    }

    @Override
    public void processStoreQueue() 
    {
		if (storeQueue.isEmpty())
			return;
		
		long threadID = Thread.currentThread().getId();
		if(!storeQueue.containsKey(threadID))
			return;
		
		List<long[]> queue = storeQueue.get(threadID);
		
		if (queue.isEmpty())
			return;

        
   }

    @Override
    public PanakoResourceMetadata getMetadata(long identifier) 
    {
        PanakoResourceMetadata metadata = null;
        String query = "SELECT resource_id, resource_path, duration, num_fingerprints FROM resource_metadata WHERE resource_id = ?";

        ResultSet resultSet = session.execute(
            SimpleStatement.newInstance(query, identifier)
        );

        Row row = resultSet.one();
        if (row != null) 
        {
            metadata = new PanakoResourceMetadata();
            metadata.identifier = row.getLong("resource_id");
            metadata.path = row.getString("resource_path");
            metadata.duration = row.getDouble("duration");
            metadata.numFingerprints = row.getInt("num_fingerprints");
        }
 
        return metadata;
    }

    @Override
    public void printStatistics(boolean detailedStats) 
    {
        ResultSet resultSet = session.execute(
            SimpleStatement.newInstance("SELECT count(resource_id), sum(duration), sum(num_fingerprints) FROM resource_metadata")
        );

        double totalDuration = 0;
        long totalPrints = 0;
        long totalResources = 0;

        Row row = resultSet.one();
        if (row != null) 
        { 
            totalResources = row.getLong(0);
            totalDuration = row.getDouble(1);
            totalPrints = row.getLong(2);
        }          
        		      
        System.out.printf("Database statistics\n");
        System.out.printf("=========================\n");
        System.out.printf("> %d audio files \n",totalResources);
        System.out.printf("> %.3f seconds of audio\n",totalDuration);
        System.out.printf("> %d fingerprint hashes \n",totalPrints);
        System.out.printf("=========================\n\n");
    }

    @Override
    public void deleteMetadata(long resourceID) 
    {
        String deleteQuery = "DELETE FROM metadata WHERE resource_id = ?";

        session.execute(
            SimpleStatement.newInstance(deleteQuery, resourceID)
        );
    }

    @Override
    public void addToQueryQueue(long queryHash) 
    {
		long threadID = Thread.currentThread().threadId();
		if(!queryQueue.containsKey(threadID))
			queryQueue.put(threadID, new ArrayList<Long>());
		queryQueue.get(threadID).add(queryHash);
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range) 
    {
        processQueryQueue(matchAccumulator, range, new HashSet<Integer>());
    }

    @Override
    public void processQueryQueue(Map<Long, List<PanakoHit>> matchAccumulator, int range,
            Set<Integer> resourcesToAvoid) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processQueryQueue'");
    }

    @Override
    public void addToDeleteQueue(long fingerprintHash, int resourceIdentifier, int t1, int f1) 
    {
		long[] data = {fingerprintHash,resourceIdentifier,t1,f1};
		long threadID = Thread.currentThread().threadId();
		if(!deleteQueue.containsKey(threadID))
			deleteQueue.put(threadID, new ArrayList<long[]>());
		deleteQueue.get(threadID).add(data);
    }

    @Override
    public void processDeleteQueue() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processDeleteQueue'");
    }

    @Override
    public void clear() {
       close();
    }

}
