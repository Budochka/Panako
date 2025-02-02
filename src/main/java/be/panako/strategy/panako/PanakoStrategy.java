/***************************************************************************
*                                                                          *
* Panako - acoustic fingerprinting                                         *
* Copyright (C) 2014 - 2022 - Joren Six / IPEM                             *
*                                                                          *
* This program is free software: you can redistribute it and/or modify     *
* it under the terms of the GNU Affero General Public License as           *
* published by the Free Software Foundation, either version 3 of the       *
* License, or (at your option) any later version.                          *
*                                                                          *
* This program is distributed in the hope that it will be useful,          *
* but WITHOUT ANY WARRANTY; without even the implied warranty of           *
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            *
* GNU Affero General Public License for more details.                      *
*                                                                          *
* You should have received a copy of the GNU Affero General Public License *
* along with this program.  If not, see <http://www.gnu.org/licenses/>     *
*                                                                          *
****************************************************************************
*    ______   ________   ___   __    ________   ___   ___   ______         *
*   /_____/\ /_______/\ /__/\ /__/\ /_______/\ /___/\/__/\ /_____/\        *
*   \:::_ \ \\::: _  \ \\::\_\\  \ \\::: _  \ \\::.\ \\ \ \\:::_ \ \       *
*    \:(_) \ \\::(_)  \ \\:. `-\  \ \\::(_)  \ \\:: \/_) \ \\:\ \ \ \      *
*     \: ___\/ \:: __  \ \\:. _    \ \\:: __  \ \\:. __  ( ( \:\ \ \ \     *
*      \ \ \    \:.\ \  \ \\. \`-\  \ \\:.\ \  \ \\: \ )  \ \ \:\_\ \ \    *
*       \_\/     \__\/\__\/ \__\/ \__\/ \__\/\__\/ \__\/\__\/  \_____\/    *
*                                                                          *
****************************************************************************
*                                                                          *
*                              Panako                                      *
*                       Acoustic Fingerprinting                            *
*                                                                          *
****************************************************************************/


package be.panako.strategy.panako;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import be.panako.strategy.QueryResult;
import be.panako.strategy.QueryResultHandler;
import be.panako.strategy.Strategy;
import be.panako.strategy.panako.storage.*;
import be.panako.util.*;
import be.tarsos.dsp.AudioDispatcher;
import be.tarsos.dsp.io.jvm.AudioDispatcherFactory;
import be.tarsos.dsp.util.PitchConverter;

/**
 * The main interface to the Panako implementation
 */
public class PanakoStrategy extends Strategy {
	private static final int MAX_TIME = 5_000_000;
	
	private final static Logger LOG = Logger.getLogger(PanakoStrategy.class.getName());

	private final int latency;

	private final PanakoStorage db;

	/**
	 * Create a new instance
	 */
	public PanakoStrategy(){

		if(!Config.getBoolean(Key.PANAKO_USE_GPU_EP_EXTRACTOR)) {
			//determine the Gaborator latency to make sure the block time to seconds
			//conversion is always correct!
			int size = Config.getInt(Key.PANAKO_AUDIO_BLOCK_SIZE);
			PanakoEventPointProcessor eventPointProcessor = new PanakoEventPointProcessor(size);
			latency = eventPointProcessor.latency();
			LOG.info(String.format("Gaborator latency is %d samples", latency));
			eventPointProcessor.processingFinished();
		}else{
			latency=0;
		}

		PanakoStorage db;
		if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("LMDB")) {
			db = PanakoStorageKV.getInstance();
		}else if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("FILE")) {
			db = PanakoStorageFile.getInstance();
		}else if (Config.get(Key.PANAKO_STORAGE).equalsIgnoreCase("SCYLLA")) {
			db = PanakoStorageScylla.getInstance();
		}else {
			db = PanakoStorageMemory.getInstance();
		}
		if(Config.getBoolean(Key.PANAKO_CACHE_TO_FILE) && db != PanakoStorageFile.getInstance()) {
			LOG.info("Using "+ db.getClass().getSimpleName() + " storage with caching front.");
			db = new PanakoCachingStorage(PanakoStorageFile.getInstance(),db);
		}else {
			LOG.info("Using " + db.getClass().getSimpleName() + " as storage.");
		}
		this.db = db;
	}

	private PanakoStorage getStorage(){
		return db;
	}
	
	@Override
	public double store(String resource, String description) {
		PanakoStorage db = getStorage();
		List<PanakoFingerprint> prints = toFingerprints(resource);
		
		int resourceID = FileUtils.getIdentifier(resource);
		//store
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToStoreQueue(hash, resourceID, print.t1,print.f1);
		}
		db.processStoreQueue();

		//store meta-data as well
		float duration = 0;
		if(prints.size() != 0) {
			duration = blocksToSeconds(prints.get(prints.size()-1).t3);
			LOG.info(String.format("Stored %d fingerprints for '%s', id: %d", prints.size() , resource ,resourceID));
		}else {
			LOG.warning("Warning: no prints extracted for " + resource);
		}
		int numberOfPrints = prints.size();
		
		db.storeMetadata((long) resourceID,resource,duration,numberOfPrints);
		
		//storage is done: 
		//try to clear memory
		System.gc();
		
		return duration;
	}

	@Override
	public double delete(String resource) {

		PanakoStorage db = getStorage();
		
		List<PanakoFingerprint> prints = toFingerprints(resource);
		
		int resourceID = FileUtils.getIdentifier(resource);
		//delete
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToDeleteQueue(hash, resourceID, print.t1,print.f1);
		}
		db.processDeleteQueue();
		
		//delete meta-data as well
		float duration = 0;
		if(prints.size() != 0) {
			duration = blocksToSeconds(prints.get(prints.size()-1).t3);
		}else {
			LOG.warning("Warning: no prints extracted for " + resource);
		}

		db.deleteMetadata((long) resourceID);
		
		//storage is done: 
		//try to clear memory
		System.gc();
		
		return duration;
	}
	
	private List<PanakoFingerprint> toFingerprints(String resource){
		return toFingerprints(resource,0,MAX_TIME);
	}
	
	private List<PanakoFingerprint> toFingerprints(String resource,double startTimeOffset,double numberOfSeconds){

		if(Config.getBoolean(Key.PANAKO_USE_CACHED_PRINTS)) {
			String folder = Config.get(Key.PANAKO_CACHE_FOLDER);
			folder = FileUtils.expandHomeDir(folder);
			String tdbPath =  FileUtils.combine(folder,resolve(resource) + ".tdb");

			if(FileUtils.exists(tdbPath)) {
				List<PanakoFingerprint> prints = new ArrayList<>();
				List<long[]> printData = readFingerprintFile(tdbPath);
				for(long[] data : printData) {
					long fingerprintHash = data[0];
					int t1 = (int) data[2];
					int f1 = (int) data[3];
					float t1InSeconds = blocksToSeconds(t1);

					//skip all fingerprints after stop time
					if(t1InSeconds > startTimeOffset + numberOfSeconds)
						break;
					//only add prints if they are after the start time offset
					if(t1InSeconds >= startTimeOffset)
						prints.add(new PanakoFingerprint(fingerprintHash,t1,f1));
				}
				LOG.info(String.format("Read %d cached fingerprints from file '%s' (start: %.3f sec, stop: %.3f sec) for '%s'", prints.size(),tdbPath,startTimeOffset,startTimeOffset+numberOfSeconds,resource));
				return prints;
			}else{
				LOG.info(String.format("Could not read cached fingerprints from file '%s' for '%s'",tdbPath,resource));
			}
		} //else no cached prints are found

		if(Config.getBoolean(Key.PANAKO_USE_GPU_EP_EXTRACTOR)){
			List<PanakoFingerprint> prints = new PanakoGPUEventPointProcessor().extractFingerprints(resource);
			List<PanakoFingerprint> filteredPrints = new ArrayList<>();
			for(PanakoFingerprint print : prints) {
				float t1InSeconds = blocksToSeconds(print.t1);
				//skip all fingerprints after stop time
				if(t1InSeconds > startTimeOffset + numberOfSeconds)
					break;
				//only add prints if they are after the start time offset
				if(t1InSeconds >= startTimeOffset)
					filteredPrints.add(print);
			}
			return filteredPrints;
		}else{
			int samplerate, size, overlap;
			samplerate = Config.getInt(Key.PANAKO_SAMPLE_RATE);
			size = Config.getInt(Key.PANAKO_AUDIO_BLOCK_SIZE);
			overlap = Config.getInt(Key.PANAKO_AUDIO_BLOCK_OVERLAP);

			AudioDispatcher d;

			if(numberOfSeconds==MAX_TIME)
				d = AudioDispatcherFactory.fromPipe(resource, samplerate, size, overlap,startTimeOffset);
			else
				d = AudioDispatcherFactory.fromPipe(resource, samplerate, size, overlap,startTimeOffset,numberOfSeconds);

			PanakoEventPointProcessor eventPointProcessor = new PanakoEventPointProcessor(size);
			d.addAudioProcessor(eventPointProcessor);
			d.run();

			return eventPointProcessor.getFingerprints();
		}

	}

	private float blocksToSeconds(int t) {
		float timeResolution = Config.getFloat(Key.PANAKO_TRANSF_TIME_RESOLUTION);
		float sampleRate = Config.getFloat(Key.PANAKO_SAMPLE_RATE);
		
		return t * (timeResolution/sampleRate) + latency/(float) sampleRate;
	}
	
	private float binToHz(int f) {
		float minFreq = Config.getFloat(Key.PANAKO_TRANSF_MIN_FREQ);
		float binsPerOctave = Config.getFloat(Key.PANAKO_TRANSF_BANDS_PER_OCTAVE);
		float centsPerBin = 1200.0f / binsPerOctave;
		
		float diffFromMinFreqInCents = f * centsPerBin;
		float minFreqInAbsCents = (float) PitchConverter.hertzToAbsoluteCent(minFreq);
		float binInAbsCents = minFreqInAbsCents + diffFromMinFreqInCents;
		
		return (float) PitchConverter.absoluteCentToHertz(binInAbsCents);
	}

	private int mostCommonDeltaTforHitList(List<PanakoMatch> hitList) {
		Map<Integer,Integer> countPerDiff = new HashMap<>();
		hitList.forEach((hit)->{
			int deltaT = hit.deltaT();
			if(!countPerDiff.containsKey(deltaT))
				countPerDiff.put(deltaT, 0);
			countPerDiff.put(deltaT, countPerDiff.get(deltaT)+1);
		});
		
		int maxCount = 0;
		int mostCommonDeltaT = 0;
		for(Map.Entry<Integer,Integer> entry : countPerDiff.entrySet()) {
			int count = entry.getValue();
			if(count > maxCount) {
				maxCount = count;
				mostCommonDeltaT = entry.getKey();
			}
		}
		return mostCommonDeltaT;
	}
	@Override
	public void query(String query, int maxNumberOfResults, Set<Integer> avoid, QueryResultHandler handler) {
		query(query,maxNumberOfResults,avoid,handler,0,MAX_TIME);
	}


	private void query(String query, int maxNumberOfResults, Set<Integer> avoid, QueryResultHandler handler, double startTimeOffset,double numberOfSeconds ) {
		
		final String queryPath ;
		List<PanakoFingerprint> prints;
		if(numberOfSeconds != MAX_TIME) {
			queryPath = query + "-" + startTimeOffset + "_" + (startTimeOffset+numberOfSeconds);
			prints = toFingerprints(query,startTimeOffset,numberOfSeconds);
		}else {
			queryPath = query;
			prints = toFingerprints(query);
		}
		
		PanakoStorage db = getStorage();
		
		Map<Long,PanakoFingerprint> printMap = new HashMap<>();
		
		//query
		for(PanakoFingerprint print : prints) {
			long hash = print.hash();
			db.addToQueryQueue(hash);
			printMap.put(hash, print);
		}
		
		//fingerprint hash to info
		Map<Long,List<PanakoHit>> matchAccumulator = new HashMap<>();
		
		StopWatch w = new StopWatch();
		int queryRange = Config.getInt(Key.PANAKO_QUERY_RANGE);
		db.processQueryQueue(matchAccumulator,queryRange , avoid);
		

		 HashMap<Integer,List<PanakoMatch>> hitsPerIdentifer = new HashMap<>();
		 
		 final List<QueryResult> queryResults = new ArrayList<>();

		 matchAccumulator.forEach((fingerprintHash, dbHits) -> {

			 dbHits.forEach((dbHit)->{
				//long matchingHash  = data[0];
				 int identifier = dbHit.resourceID;
				 int matchTime = dbHit.t;
				 if(!hitsPerIdentifer.containsKey(identifier)){
					hitsPerIdentifer.put(identifier, new ArrayList<PanakoMatch>());
				 }
				 PanakoMatch hit = new PanakoMatch();
				 hit.identifier = identifier;
				 hit.matchTime = matchTime;
				 hit.originalHash = dbHit.originalHash;
				 hit.matchedNearHash = dbHit.matchedNearHash;
				 hit.queryTime = printMap.get(fingerprintHash).t1;
				 
				 hit.queryF1 = printMap.get(fingerprintHash).f1;
				 hit.matchF1 = dbHit.f;
				 
				 hitsPerIdentifer.get(identifier).add(hit);
			 });
		 });

		 LOG.info(String.format("Query for %d prints, %d matches in %s \n", printMap.size(),matchAccumulator.size(), w.formattedToString()));

		 int minimumUnfilteredHits = Config.getInt(Key.PANAKO_MIN_HITS_UNFILTERED);
		 int minimumFilteredHits = Config.getInt(Key.PANAKO_MIN_HITS_FILTERED);
		 
		 List<Integer> matchesToDelete = new ArrayList<>();
		 hitsPerIdentifer.forEach((identifier, hitlist) -> {
			 if(hitlist.size() < minimumUnfilteredHits) {
				 matchesToDelete.add(identifier);
			 }
		 });
		 
		 matchesToDelete.forEach( identifier ->{
			 hitsPerIdentifer.remove(identifier);
		 });
		 
		 hitsPerIdentifer.forEach((identifier, hitlist) -> {
			 
			 //sort by query time
			 Collections.sort(hitlist, (Comparator<? super PanakoMatch>) (PanakoMatch a, PanakoMatch b) -> Integer.valueOf(a.queryTime).compareTo(Integer.valueOf(b.queryTime)));
			
			 //view the first and last hits (max 250)
			 int maxPartListSize = Config.getInt(Key.PANAKO_HIT_PART_MAX_SIZE);
			 int partDivider = Config.getInt(Key.PANAKO_HIT_PART_DIVIDER);
			 int partListLength = Math.min(maxPartListSize,Math.max(minimumUnfilteredHits,hitlist.size()/partDivider));
			 List<PanakoMatch> firstHits = hitlist.subList(0, partListLength);
			 List<PanakoMatch> lastHits  = hitlist.subList(hitlist.size() - partListLength, hitlist.size());
			 
			//find the first x1 where delta t is equals to the median delta t
			 float y1 = mostCommonDeltaTforHitList(firstHits);
			 float x1 = 0;
			 float frequencyFactor = 0;
			 for(int i = 0 ; i < firstHits.size() ; i++) {
				 PanakoMatch hit = firstHits.get(i);
				 int diff = hit.deltaT();
				 if(diff == y1) {
					 x1 = hit.queryTime;
					 frequencyFactor = binToHz(hit.matchF1) / binToHz(hit.queryF1);
					 break;
				 }
			 }

			//find the first x2 where delta t is equals to the median delta t
			 float y2 = mostCommonDeltaTforHitList(lastHits);
			 float x2 = 0;
			 for(int i = lastHits.size() - 1 ; i >= 0 ; i--) {
				 PanakoMatch hit = lastHits.get(i);
				 int diff = hit.deltaT();
				 if(diff == y2) {
					 x2 = hit.queryTime;
					 break;
				 }
			 }
			 	 
			 float slope = (y2-y1)/(x2-x1);
			 float offset = -x1 * slope + y1;
			 float timeFactor = 1.0f/(1-slope);
			 	 
			 //System.out.printf("slope %f offset %f  timefactor %f  freqfactor %f \n",slope,offset,timeFactor,frequencyFactor);
			 
			 //threshold in time bins
			 double threshold = Config.getFloat(Key.PANAKO_QUERY_RANGE);
			 
			 //only continue processing when time factor is reasonable
			 if(timeFactor > Config.getFloat(Key.PANAKO_MIN_TIME_FACTOR) && timeFactor < Config.getFloat(Key.PANAKO_MAX_TIME_FACTOR) && 
					 frequencyFactor> Config.getFloat(Key.PANAKO_MIN_FREQ_FACTOR) &&  frequencyFactor < Config.getFloat(Key.PANAKO_MAX_FREQ_FACTOR)	 ) {
				 List<PanakoMatch> filteredHits = new ArrayList<>();
				 
				 hitlist.forEach( hit ->{				 
					 float yActual = hit.deltaT();
					 float x = hit.queryTime;
					 float yPredicted = slope * x + offset;
					 
					 //should be within an expected range
					 boolean yInExpectedRange = Math.abs(yActual-yPredicted) <= threshold ; 
					 
					 //if(hit.identifier!= FileUtils.getIdentifier(queryPath))
						 //System.out.printf("pred: %f  actual: %f   dif abs: %f  threshold %f \n",yPredicted, yActual,Math.abs(yActual-yPredicted), threshold);
					 
					 if(yInExpectedRange) {
						 filteredHits.add(hit);
					 }
				 });
				 
				 //ignore resources with too few filtered hits remaining
				 if(filteredHits.size() > minimumFilteredHits) {
					 //System.out.println("Matches " + identifier + " matches filtered hits: " + filteredHits.size());
					 
					 float minDuration = Config.getFloat(Key.PANAKO_MIN_MATCH_DURATION);
					 float queryStart = blocksToSeconds(filteredHits.get(0).queryTime);
					 float queryStop = blocksToSeconds(filteredHits.get(filteredHits.size()-1).queryTime);
					 float duration = queryStop - queryStart;
					 
					 if(duration >= minDuration) {
						 int score = filteredHits.size();
						
						 
						 float refStart = blocksToSeconds(filteredHits.get(0).matchTime);
						 float refStop =  blocksToSeconds(filteredHits.get(filteredHits.size()-1).matchTime);
						 
						 //retrieve meta-data
						 PanakoResourceMetadata metadata = db.getMetadata((long) identifier);
						 String refPath = "metadata unavailable!";
						 if(metadata != null )
							 refPath = metadata.path;
						 
						 //Construct a histogram with the number of matches for each second
						 //Ideally there is a more or less equal number of matches each second
						 // note that the last second might not be a full second
						 TreeMap<Integer,Integer> matchesPerSecondHistogram = new TreeMap<>();
						 for(PanakoMatch hit : filteredHits) {
							 //if(hit.identifier!= FileUtils.getIdentifier(queryPath))
								 //System.out.printf("%d %d %d %d %d\n", hit.identifier, hit.matchTime, hit.queryTime, hit.originalHash, hit.matchedNearHash);
							 float offsetInSec = blocksToSeconds(hit.matchTime) - refStart;
							 int secondBin = (int) offsetInSec;
							 if(!matchesPerSecondHistogram.containsKey(secondBin))
								 matchesPerSecondHistogram.put(secondBin, 0);
							 matchesPerSecondHistogram.put(secondBin, matchesPerSecondHistogram.get(secondBin)+1);
						 }
						
						 //number of seconds bins
						 float numberOfMatchingSeconds = (float) Math.ceil(refStop - refStart);
						 float emptySeconds = numberOfMatchingSeconds - matchesPerSecondHistogram.size();
						 float percentOfSecondsWithMatches = 1 - (emptySeconds / numberOfMatchingSeconds);

						 if(percentOfSecondsWithMatches >= Config.getFloat(Key.PANAKO_MIN_SEC_WITH_MATCH)){
						 	QueryResult r = new QueryResult(queryPath,queryStart, queryStop, refPath, "" + identifier, refStart, refStop,  score, timeFactor, frequencyFactor,percentOfSecondsWithMatches);
						 	queryResults.add(r);
						 }
					 }
				 }
			 }			 
		 });
		 
		 if (queryResults.isEmpty()) {
			 handler.handleEmptyResult(QueryResult.emptyQueryResult(queryPath,0,0));
		 }else {
			 //sort results by score desc
			 queryResults.sort((Comparator<? super QueryResult>) (QueryResult a, QueryResult b) -> Integer.valueOf((int) b.score).compareTo(Integer.valueOf((int) a.score)));
			 //limit results to max number of results, if needed
			 List<QueryResult> finalResults = queryResults;
			 if(queryResults.size()>maxNumberOfResults) {
				 finalResults = queryResults.subList(0, maxNumberOfResults);
			 }
			 //handle the results in order (best score first)
			 for(QueryResult r : finalResults) {
				 handler.handleQueryResult(r);
			 }
		 }
	}
	
	@Override
	public void monitor(String query, int maxNumberOfReqults, Set<Integer> avoid, QueryResultHandler handler) {
		int overlapInSeconds = Config.getInt(Key.MONITOR_OVERLAP); // 5
		int stepSizeInSeconds = Config.getInt(Key.MONITOR_STEP_SIZE); //25
		
		float totalDuration = AudioFileUtils.audioFileDurationInSeconds(new File(query));
		
		//Steps: 0-25s ; 20-45s ; 40-65s ...
		int actualStep = stepSizeInSeconds - overlapInSeconds;//20s
		for(int t = 0 ; t + stepSizeInSeconds < totalDuration; t += actualStep ) {			
			query(query,maxNumberOfReqults,avoid,handler,t,stepSizeInSeconds);
		}
	}

	@Override
	public boolean hasResource(String resource) {
		int identifier = FileUtils.getIdentifier(resource);
		PanakoStorage db = getStorage();

		return db.getMetadata(identifier) != null;
	}

	@Override
	public boolean isStorageAvailable() {
		return true;
	}

	@Override
	public void printStorageStatistics() {
		getStorage().printStatistics(true);
	}

	@Override
	public String resolve(String filename) {
		return "" + FileUtils.getIdentifier(filename);
	}
	
	private List<long[]> readFingerprintFile(String fingerprintFilePath) {
		List<long[]> prints = new ArrayList<>();
		try {
			PanakoStorageFile fileDb = PanakoStorageFile.getInstance();
			final File file = new File(fingerprintFilePath);
			FileReader fileReader = new FileReader(file);
			final BufferedReader reader = new BufferedReader(fileReader);
			String inputLine = reader.readLine();
			while (inputLine != null) {
				long[] data = fileDb.dataFromLine(inputLine);
				prints.add(data);
				inputLine = reader.readLine();
			}
			reader.close();
		} catch (final IOException i1) {
			System.err.println("Can't open file:" + fingerprintFilePath);
			i1.printStackTrace();
		}
		return prints;
	}
	
	
	private void addToMap(TreeMap<Integer,float[]> map,int t,int f,float m) {
		if(!map.containsKey(t)) {
			map.put(t, new float[Config.getInt(Key.OLAF_SIZE)/2]);
		}
		map.get(t)[f]=m;
	}

	@Override
	public void print(String path, boolean sonicVisualizerOutput, boolean printOnlyEPs) {
		List<PanakoFingerprint> prints = toFingerprints(path);

		TreeMap<Integer,float[]> timeIndexedSpectralPeaks = new TreeMap<>();
		for(PanakoFingerprint print : prints) {
			addToMap(timeIndexedSpectralPeaks,print.t1,print.f1,print.m1);
			addToMap(timeIndexedSpectralPeaks,print.t2,print.f2,print.m2);
			addToMap(timeIndexedSpectralPeaks,print.t2,print.f2,print.m3);
		}
		float[] emptySpectrum = new float[Config.getInt(Key.OLAF_SIZE)/2];
		StringBuilder sb = new StringBuilder();
		for(int t = 0 ; t <= timeIndexedSpectralPeaks.lastKey();t++) {
			float[] spectrum = emptySpectrum;
			if(timeIndexedSpectralPeaks.containsKey(t))
				spectrum = timeIndexedSpectralPeaks.get(t);
			
			sb.append(blocksToSeconds(t)).append(",");
			for(int i = 0 ; i < spectrum.length ; i++) {
				sb.append(spectrum[i]).append(",");
			}
			sb.append("\n");
		}
		System.out.println(sb);
	}

	@Override
	public void clear() {
		getStorage().clear();
	}

	public String metadata(String path) {
		final PanakoStorage db = getStorage();
		long identifier = FileUtils.getIdentifier(path);
		PanakoResourceMetadata metaData = db.getMetadata(identifier);

		return String.format("%d ; %s ; %.3f (s) ; %d (#) ; %.3f (#/s)",metaData.identifier,metaData.path,metaData.duration,metaData.numFingerprints,metaData.printsPerSecond());
	}
}
