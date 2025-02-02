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


package be.panako.cli;

import java.io.File;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import be.panako.strategy.Strategy;
import be.panako.util.*;

/**
 * Store audio fingerptings in the storage.
 *
 * @author Joren Six
 */
class Store extends Application {
    private final static Logger LOG = Logger.getLogger(Store.class.getName());

    @Override
    public void run(final String... args) {
        int processors = availableProcessors();
        int counter = 0;

        final ExecutorService executor = Executors.newFixedThreadPool(processors);

        final List<File> files = this.getFilesFromArguments(args);
        if (files.size() > 1) {
            String msg = "Processing " + files.size() + " files on " + processors + " seperate threads.";
            LOG.info("Store task started. " + msg);
        }
        System.out.println("index; length; audiofile; audio duration; fingerprinting duration; ratio");
        for (File file : files) {
            counter++;

            StoreTask task = new StoreTask(file, counter, files.size());
            if (processors == 1) {
                // Only one thread available:
                // run on the main thread
                task.run();
            } else {
                // run on thread managed by pool
                executor.submit(task);
            }
        }

        try {
            //do not accept more tasks.
            executor.shutdown();
            //wait for tasks to finish
            executor.awaitTermination(300, java.util.concurrent.TimeUnit.DAYS);
            //System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            Strategy.getInstance().clear();
        }
    }

    @Override
    public String description() {
        return "Stores audio fingerprints in the storage.";
    }

    @Override
    public String synopsis() {
        return "store [audiofilelist.txt... audio_files...]";
    }

    private static class StoreTask implements Runnable {
        private final File file;
        private final int taskID;
        private final int totalTasks;


        public StoreTask(File file, int taskID, int totalTasks) {
            this.file = file;
            this.taskID = taskID;
            this.totalTasks = totalTasks;
        }

        @Override
        public void run() {
            StopWatch w = new StopWatch();
            Strategy strategy = Strategy.getInstance();
            boolean isDouble = false;
            if (Config.getBoolean(Key.CHECK_DUPLICATE_FILE_NAMES)) {
                isDouble = strategy.hasResource(file.getAbsolutePath());
            }

            String message = null;
            if (isDouble) {
                message = String.format("%d; %d; %s; %s; 0; 0", taskID, totalTasks, file.getName(), "Skipped: resource already stored;");
            } else {
                double durationInSeconds = strategy.store(file.getAbsolutePath(), file.getName());
                double cpuSecondsPassed = w.timePassed(TimeUnit.SECONDS);
                String audioDuration = StopWatch.toTime("", (int) Math.round(durationInSeconds));
                String cpuTimeDuration = w.formattedToString();
                double timeRatio = durationInSeconds / cpuSecondsPassed;
                message = String.format("%d; %d; %s; %s; %s; %.2f", taskID, totalTasks, file.getName(), audioDuration, cpuTimeDuration, timeRatio);
            }
            LOG.info(message);
            System.out.println(message);
        }
    }

    @Override
    public boolean needsStorage() {
        return true;
    }

    @Override
    public boolean writesToStorage() {
        return true;
    }
}
