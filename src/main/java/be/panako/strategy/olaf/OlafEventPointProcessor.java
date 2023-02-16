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


package be.panako.strategy.olaf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import be.panako.util.Config;
import be.panako.util.Key;
import be.panako.util.LemireMinMaxFilter;
import be.tarsos.dsp.AudioEvent;
import be.tarsos.dsp.AudioProcessor;
import be.tarsos.dsp.util.PitchConverter;
import be.tarsos.dsp.util.fft.FFT;
import be.tarsos.dsp.util.fft.HammingWindow;

/**
 * A TarsosDSP AudioProcessor which steps through the audio and returns peaks in the spectrogram.
 */
public class OlafEventPointProcessor implements AudioProcessor {

	private final FFT fft;
		
	/**
	 * 
	 * Use a 2D float array to prevent creation of new
	 * objects in the processing loop, at the expense of a bit of
	 * complexity
	 */
	private final float[][] magnitudes;
	private final float[][] maxMagnitudes;
	
	/**
	 * A counter used in the 2D float arrays
	 */
	private int magnitudesIndex=0;
	
	//Each map maps a frame index to the data represented.
	private final Map<Integer,float[]> previousMaxMagnitudes;
	private final Map<Integer,float[]> previousMagnitudes;
	
	private final List<OlafEventPoint> eventPoints = new ArrayList<>();
	private final List<OlafFingerprint> fingerprints = new ArrayList<>();

	private int analysisFrameIndex = 0;
	
	private final LemireMinMaxFilter maxFilterVertical;

	private final int maxFilterWindowSizeFrequency = Config.getInt(Key.OLAF_FREQ_MAX_FILTER_SIZE);
	private final int maxFilterWindowSizeTime = Config.getInt(Key.OLAF_TIME_MAX_FILTER_SIZE);
	
	private final float[] maxHorizontal;

	private final int[] frequencyFilterMinIndexes;
	private final int[] frequencyFilterMaxIndexes;
	
	//private final int maxFingerprintsPerEventPoint = 10;

	/**
	 * Create a new event point processor for this fft size
	 * @param fftSize The size of the fft
	 */
	public OlafEventPointProcessor(final int fftSize){
		fft = new FFT(fftSize, new HammingWindow());		
		
		magnitudesIndex=0;
		magnitudes = new float[maxFilterWindowSizeTime][fftSize/2];
		maxMagnitudes = new float[maxFilterWindowSizeTime][fftSize/2];

		previousMaxMagnitudes = new HashMap<>();
		previousMagnitudes = new HashMap<>();

		maxFilterVertical = new LemireMinMaxFilter(maxFilterWindowSizeFrequency+1, fftSize/2,true);
		
		maxHorizontal = new float[fftSize/2];

		int startFrequencyBinIndex = Config.getInt(Key.OLAF_EP_MIN_FREQ_BIN);
		frequencyFilterMinIndexes = new int[fftSize/2];
		frequencyFilterMaxIndexes = new int[fftSize/2];

		int frequencyMidi[] = new int[fftSize/2];
		for(int f = startFrequencyBinIndex ; f < fftSize/2 ; f++){
			frequencyMidi[f] = PitchConverter.hertzToMidiKey(Double.valueOf(binToHz(f)));
		}

		for(int f = startFrequencyBinIndex ; f < fftSize/2 ; f++){
			int centerBinMidiKey = frequencyMidi[f];
			int startBinMidiKey = centerBinMidiKey - Config.getInt(Key.OLAF_FREQ_MAX_FILTER_SIZE) / 2;
			int stopBinMidiKey = centerBinMidiKey + Config.getInt(Key.OLAF_FREQ_MAX_FILTER_SIZE) / 2;
			int startIndex = Arrays.binarySearch(frequencyMidi,startBinMidiKey);
			startIndex = startIndex >= 0 ? startIndex : Math.abs(startIndex + 1);
			int stopIndex = Math.abs(Arrays.binarySearch(frequencyMidi,stopBinMidiKey));
			stopIndex = stopIndex >= 0 ? stopIndex : Math.abs(stopIndex + 1);
			frequencyFilterMinIndexes[f] = Math.max(startFrequencyBinIndex,startIndex);
			frequencyFilterMaxIndexes[f] = Math.min(stopIndex,frequencyFilterMaxIndexes.length);
		}
	}

	void verticalFilter(float[] data, float[]  max){
		int startFrequencyBinIndex = Config.getInt(Key.OLAF_EP_MIN_FREQ_BIN);
		for(int f = startFrequencyBinIndex ; f < data.length ; f++){
			int startIndex = frequencyFilterMinIndexes[f];
			int  stopIndex = frequencyFilterMaxIndexes[f];
			float maxValue = -1000000;
			for(int j = startIndex ; j < stopIndex; j++){
				if(maxValue < data[j]){
					maxValue = data[j];
				}
			}
			max[f] = maxValue;
		}
	}
	void naive_max_filter(float[] data, float[]  max, int  half_filter_size , boolean clamp){

		for(int i = 0 ; i < data.length ; i++){
			int startIndex = Math.max(i - half_filter_size,0);
			int  stopIndex = Math.min(data.length-1, i + half_filter_size);
			float maxValue = -1000000;
			for(int j = startIndex ; j <= stopIndex; j++){
				if(maxValue < data[j]){
	                maxValue = data[j];
	            }
			}
	        max[i] = maxValue;
		}
	   
	}
	
	@Override	
	public boolean process(AudioEvent audioEvent) {
		//clone since the buffer is reused to slide
		float[] buffer = audioEvent.getFloatBuffer().clone();
		
		//calculate the fft
		fft.forwardTransform(buffer);

		//
		int startFrequencyBinIndex = Config.getInt(Key.OLAF_EP_MIN_FREQ_BIN);

		//calculate the magnitudes		
		for (int i = startFrequencyBinIndex; i < magnitudes[magnitudesIndex].length; i++) {
			int realIndex = 2 * i;
			int imgIndex  = 2 * i + 1;
			magnitudes[magnitudesIndex][i] =  buffer[realIndex] * buffer[realIndex] + buffer[imgIndex] * buffer[imgIndex];
		}
		
		//store the frame magnitudes
		previousMagnitudes.put(analysisFrameIndex, magnitudes[magnitudesIndex]);
		
		//run a max filter over frequency bins
		verticalFilter(magnitudes[magnitudesIndex],maxMagnitudes[magnitudesIndex]);

		//store the max filtered frequency bins
		previousMaxMagnitudes.put(analysisFrameIndex,maxMagnitudes[magnitudesIndex]);
		
		//find the horziontal maxima
		if(previousMaxMagnitudes.size()==maxFilterWindowSizeTime){
			
			int t = analysisFrameIndex - maxFilterWindowSizeTime /2;
			
			float[] maxFrame = previousMaxMagnitudes.get(t);
			float[] frameMagnitudes = previousMagnitudes.get(t);
			
			for(int f = startFrequencyBinIndex ; f < frameMagnitudes.length - 1 ; f++){
				float maxVal = maxFrame[f];
				float currentVal = frameMagnitudes[f];
				
				if(maxVal == currentVal) {
					horizontalFilter(f);
					maxVal = maxHorizontal[f];
					if(currentVal == maxVal && currentVal !=0 ){
						
						float[] prevFrameMagnitudes = previousMagnitudes.get(t-1);
						float[] nextFrameMagnitudes = previousMagnitudes.get(t+1);
						
						//add the magnitude of surrounding bins for magnitude estimates more robust against discretization effects 
						float totalMagnitude = frameMagnitudes[f] + prevFrameMagnitudes[f] + nextFrameMagnitudes[f]
								+ frameMagnitudes[f+1] + prevFrameMagnitudes[f+1] + nextFrameMagnitudes[f+1]
								+ frameMagnitudes[f-1] + prevFrameMagnitudes[f-1] + nextFrameMagnitudes[f-1];
						
						eventPoints.add(new OlafEventPoint(t, f,totalMagnitude));
					}
				}
			}
			
			//Remove analysis frames that are not needed any more:
			previousMaxMagnitudes.remove(analysisFrameIndex-maxFilterWindowSizeTime+1);
			previousMagnitudes.remove(analysisFrameIndex-maxFilterWindowSizeTime+1);
		}
				
		//magnitude index counter
		magnitudesIndex++;
		if(magnitudesIndex == magnitudes.length){
			magnitudesIndex=0;
		}
		
		//Increment analysis frame counter
		analysisFrameIndex++;
		
		return true;
	}

	/**
	 * Return the extracted magnitudes
	 * @return Return the extracted magnitudes
	 */
	public float[] getMagnitudes(){
		return magnitudes[magnitudesIndex];
	}
	
	private void horizontalFilter(int j) {
 		Arrays.fill(maxHorizontal, -1000);
		
		// The frame index of the frame under analysis:
 		int centerFrameIndex = analysisFrameIndex - maxFilterWindowSizeTime /2;
		int startFrameIndex =  centerFrameIndex - maxFilterWindowSizeTime/2;
		int stopFrameIndex =  centerFrameIndex + maxFilterWindowSizeTime/2;
		
		// Run a horizontal max filter
		for(int i = startFrameIndex ; i < stopFrameIndex ; i++){
			float[] maxFrame = previousMaxMagnitudes.get(i);
			maxHorizontal[j] = Math.max(maxHorizontal[j], maxFrame[j]);
		}
	}
	
	
	@Override
	public void processingFinished() {
		packEventPointsIntoFingerprints();
	}

	/**
	 * Returns the extracted fingerprints.
	 * @return Returns the extracted fingerprints.
	 */
	public List<OlafFingerprint> getFingerprints(){
		return fingerprints;
	}

	/**
	 * Returns the extracted event points.
	 * @return Returns the extracted event points.
	 */
	public List<OlafEventPoint> getEventPoints() {
		return eventPoints;
	}

	private void packEventPointsIntoFingerprintsTwo(){

		int minFreqDistance = Config.getInt(Key.OLAF_FP_MIN_FREQ_DIST);
		int maxFreqDistance = Config.getInt(Key.OLAF_FP_MAX_FREQ_DIST);

		int minTimeDistance = Config.getInt(Key.OLAF_FP_MIN_TIME_DIST);
		int maxTimeDistance = Config.getInt(Key.OLAF_FP_MAX_TIME_DIST);

		for(int i = 0; i < eventPoints.size();i++){
			int t1 = eventPoints.get(i).t;
			int f1 = eventPoints.get(i).f;

			for(int j = i + 1; j < eventPoints.size() ;j++){
				int t2 = eventPoints.get(j).t;
				int f2 = eventPoints.get(j).f;

				int fDiff = Math.abs(f1 - f2);
				int tDiff = t2-t1;

				if(tDiff > maxTimeDistance) break;
				if(tDiff < minTimeDistance) continue;

				if(fDiff < minFreqDistance) continue;
				if(fDiff > maxFreqDistance ) continue;

				OlafFingerprint fingerprint;
				fingerprint = new OlafFingerprint(eventPoints.get(i),eventPoints.get(j),eventPoints.get(j));
				fingerprints.add(fingerprint);
			}
		}
	}

	private void packEventPointsIntoFingerprints(){
		if(Config.getInt(Key.OLAF_EPS_PER_FP) == 2){
			packEventPointsIntoFingerprintsTwo();
		}else if(Config.getInt(Key.OLAF_EPS_PER_FP) == 3){
			packEventPointsIntoFingerprintsThree();
		}else {
			throw new RuntimeException("Key.OLAF_EPS_PER_FP should be either 2 or 3");
		}
	}

	private void packEventPointsIntoFingerprintsThree(){
		
		int minFreqDistance = Config.getInt(Key.OLAF_FP_MIN_FREQ_DIST);
		int maxFreqDistance = Config.getInt(Key.OLAF_FP_MAX_FREQ_DIST);
		
		int minTimeDistance = Config.getInt(Key.OLAF_FP_MIN_TIME_DIST);
		int maxTimeDistance = Config.getInt(Key.OLAF_FP_MAX_TIME_DIST);
		
		for(int i = 0; i < eventPoints.size();i++){
			int t1 = eventPoints.get(i).t;
			int f1 = eventPoints.get(i).f;
				
			for(int j = i + 1; j < eventPoints.size() ;j++){
				int t2 = eventPoints.get(j).t;
				int f2 = eventPoints.get(j).f;
				
				int fDiff = Math.abs(f1 - f2);
				int tDiff = t2-t1;
				
				if(tDiff > maxTimeDistance) break;
				if(tDiff < minTimeDistance) continue;
				
				if(fDiff < minFreqDistance) continue;
				if(fDiff > maxFreqDistance ) continue;
				
				for(int k = j + 1 ; k <eventPoints.size(); k++) {
					int t3 = eventPoints.get(k).t;
					int f3 = eventPoints.get(k).f;
					
					fDiff = Math.abs(f2 - f3);
					tDiff = t3-t2;
					
					if(tDiff > maxTimeDistance) break;
					if(tDiff < minTimeDistance) continue;
					
					if(fDiff < minFreqDistance) continue;
					if(fDiff > maxFreqDistance ) continue;
					
					OlafFingerprint fingerprint;
					fingerprint = new OlafFingerprint(eventPoints.get(i),eventPoints.get(j),eventPoints.get(k));
					fingerprints.add(fingerprint);
				}
			}
		}
	}

	private float binToHz(int f) {
		double sampleRate = Config.getFloat(Key.OLAF_SAMPLE_RATE);
		double fftSize = Config.getFloat(Key.OLAF_SIZE);
		double binSizeInHz = sampleRate / fftSize;
		double centerBinFrequencyInHz = f * binSizeInHz + binSizeInHz / 2.0;
		return (float) centerBinFrequencyInHz;
	}

	/**
	 * Reset the state of the extractor to the initial state.
	 * Forgets any information of before.
	 */
	public void reset() {
		eventPoints.clear();
		fingerprints.clear();
		analysisFrameIndex=0;
		magnitudesIndex=0;
		previousMagnitudes.clear();
		previousMaxMagnitudes.clear();
	}
	
}
