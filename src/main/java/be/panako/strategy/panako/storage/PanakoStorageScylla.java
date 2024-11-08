package be.panako.strategy.panako.storage;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

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
                .addContactPoint(new InetSocketAddress("51.250.47.177", 9042))
                .withLocalDatacenter("scylla_data_center") 
                .withAuthCredentials("cassandra", "Axg7na0w6HTL5yw")
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
	}

    @Override
    public void storeMetadata(long resourceID, String resourcePath, float duration, int fingerprints) 
    {
        String insertQuery = "INSERT INTO test.metadata (resource_id, resource_path, duration, num_fingerprints) VALUES (?, ?, ?, ?)";

        session.execute(
            SimpleStatement.newInstance(insertQuery, resourceID, resourcePath, (double)duration, fingerprints)
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
		
		long threadID = Thread.currentThread().threadId();
		if(!storeQueue.containsKey(threadID))
			return;
		
		List<long[]> queue = storeQueue.get(threadID);
		
		if (queue.isEmpty())
			return;

        String insertQuery = "INSERT INTO test.fingerprints (fingerprintHash, resource_id, t1, f1) VALUES (?, ?, ?, ?)";

        for(long[] data : queue) 
        {
            session.execute(
                SimpleStatement.newInstance(insertQuery, data[0], data[1], (int)data[2], (int)data[3]));            
        }                        
   }

    @Override
    public PanakoResourceMetadata getMetadata(long identifier) 
    {
        PanakoResourceMetadata metadata = null;
        String query = "SELECT resource_id, resource_path, duration, num_fingerprints FROM test.metadata WHERE resource_id = ?";

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
            SimpleStatement.newInstance("SELECT count(resource_id), sum(duration), sum(num_fingerprints) FROM test.metadata")
        );

        double totalDuration = 0;
        long totalPrints = 0;
        long totalResources = 0;

        Row row = resultSet.one();
        if (row != null) 
        { 
            totalResources = row.getInt(0);
            totalDuration = row.getDouble(1);
            totalPrints = row.getInt(2);
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
        String deleteQuery = "DELETE FROM test.metadata WHERE resource_id = ?";

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
            Set<Integer> resourcesToAvoid) 
    {	
        if (queryQueue.isEmpty())
        return;
    
        long threadID = Thread.currentThread().threadId();
        if(!queryQueue.containsKey(threadID))
            return;
        
        List<Long> queue = queryQueue.get(threadID);
        
        if (queue.isEmpty())
            return;            

        for (Long originalKey : queue) 
        {
            String query = "SELECT fingerprintHash, resource_id, t1, f1 from test.fingerprints WHERE fingerprintHash IN (";

            String hashes = "";
            boolean first = true;

            //Generate string for IN statement
            for (long r = originalKey - range; r<= originalKey + range; r++)
            {
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

            for (Row row : resultSet) 
            {
                if (row != null) 
                {
                    long fingerprintHash = row.getLong("fingerprintHash");
                    long resourceID = row.getLong("resource_id");
                    long t = row.getInt("t1");
                    long f = row.getInt("f1");
    
                    if(!resourcesToAvoid.contains((int) resourceID)) 
                    {
                        if(!matchAccumulator.containsKey(originalKey))
                            matchAccumulator.put(originalKey,new ArrayList<PanakoHit>());
                        matchAccumulator.get(originalKey).add(new PanakoHit(originalKey, fingerprintHash, t, resourceID, f));
                    }
                }

            }
        }
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
    public void processDeleteQueue() 
    {
		if (storeQueue.isEmpty())
			return;
		
		long threadID = Thread.currentThread().threadId();
		if(!storeQueue.containsKey(threadID))
			return;
		
		List<long[]> queue = storeQueue.get(threadID);
		
		if (queue.isEmpty())
			return;
            
        String deleteQuery = "DELETE FROM test.fingerprints WHERE fingerprintHash = ?";

        for(long[] data : queue) 
        {
            session.execute(
                SimpleStatement.newInstance(deleteQuery, data[0]));            
        }
    }

    @Override
    public void clear() {
       close();
    }

}
