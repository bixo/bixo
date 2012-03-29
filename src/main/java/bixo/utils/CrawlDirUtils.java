/*
 * Copyright 2009-2012 Scale Unlimited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package bixo.utils;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class CrawlDirUtils {
	private static final Pattern LOOP_DIRNAME_PATTERN = Pattern
			.compile("(\\d+)-([^/]+)");

	/**
	 * Protect against earlier versions of Hadoop returning null if there
	 * are no sub-directories in <path>
	 * 
	 * @param fs
	 * @param path
	 * @return
	 * @throws IOException
	 */
	private static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException {
	    FileStatus[] result = fs.listStatus(path);
	    if (result == null) {
	        result = new FileStatus[0];
	    }
	    
	    return result;
	}
	
	
	/**
	 * 
	 * @param fs
	 * @param outputDir
	 * @param loopNumber
	 * @return Directory path <loopNumber>-<timestamp>
	 * @throws IOException
	 */
	public static Path makeLoopDir(FileSystem fs, Path outputDir, int loopNumber)
			throws IOException {
		String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
				.format(new Date());
		Path loopDir = new Path(outputDir, "" + loopNumber + "-" + timestamp);
		fs.mkdirs(loopDir);
		return loopDir;
	}

	/**
	 * 
	 * @param fs
	 * @param outputPath
	 * @return Path to the latest loop directory (based on the loop number); 
	 *         null in the case of an error.
	 * @throws IOException
	 */
	public static Path findLatestLoopDir(FileSystem fs, Path outputPath)
			throws IOException {
		int bestLoop = -1;
		Path result = null;

		FileStatus[] subdirs = listStatus(fs, outputPath);
		for (FileStatus status : subdirs) {
			if (!status.isDir()) {
				continue;
			}

			try {
				int curLoop = extractLoopNumber(status.getPath());
				if (curLoop > bestLoop) {
					bestLoop = curLoop;
					result = status.getPath();
				}
			} catch (InvalidParameterException e) {
				// ignore, though we shouldn't have random sub-dirs in the
				// output directory.
			}
		}

		return result;
	}

	/**
	 * Given a loopNumber, returns the name of the next loop directory. 
	 * 
	 * @param fs
	 * @param outputPath
	 * @param loopNumber
	 * @return Name of the next loop directory if one is present; null otherwise. 
	 * @throws IOException
	 */
	public static Path findNextLoopDir(FileSystem fs, Path outputPath,
			int loopNumber) throws IOException {
		int bestLoop = Integer.MAX_VALUE;
		Path result = null;

		FileStatus[] subdirs = listStatus(fs, outputPath);
		for (FileStatus status : subdirs) {
			if (!status.isDir()) {
				continue;
			}

			try {
				int curLoop = extractLoopNumber(status.getPath());
				if ((curLoop > loopNumber) && (curLoop < bestLoop)) {
					bestLoop = curLoop;
					result = status.getPath();
				}
			} catch (InvalidParameterException e) {
				// ignore, though we shouldn't have random sub-dirs in the
				// output directory.
			}
		}

		return result;
	}

	/**
	 * Given a "crawl dir" style input path, extract the loop number from the path.
	 * 
	 * @param inputPath
	 * @return Loop number for the directory
	 * @throws InvalidParameterException
	 */
	public static int extractLoopNumber(Path inputPath)
			throws InvalidParameterException {
		String dirName = inputPath.getName();
		Matcher dirNameMatcher = LOOP_DIRNAME_PATTERN.matcher(dirName);
		if (dirNameMatcher.matches()) {
			return Integer.parseInt(dirNameMatcher.group(1));
		} else {
			throw new InvalidParameterException(String.format(
					"%s is not a valid loop directory name", dirName));
		}
	}
	
	/**
	 * Return an array of paths to all of the subdirs in crawl dirs found
     * inside of <crawlPath>, where the subdir name == <subdirName>
	 * @param fs
	 * @param outputPath
	 * @param subdirName
	 * @return Array of directory paths that contain <subdirName>
	 * @throws IOException
	 */
    public static Path[] findAllSubdirs(FileSystem fs, Path outputPath,
			String subdirName) throws IOException {
		ArrayList<Path> result = new ArrayList<Path>();

		FileStatus[] crawldirs = listStatus(fs, outputPath);
		for (FileStatus status : crawldirs) {
			if (!status.isDir()) {
				continue;
			}

			try {
				// Verify crawl dir name is valid.
				extractLoopNumber(status.getPath());
				
				Path subdirPath = new Path(status.getPath(), subdirName);
				// isDirectory has been un-depracated in 0.21...
	            if (fs.exists(subdirPath) && fs.isDirectory(subdirPath)) {
	                result.add(subdirPath);
	            }
			} catch (InvalidParameterException e) {
				// ignore, though we shouldn't have random sub-dirs in the
				// output directory.
			}
		}

		return result.toArray(new Path[result.size()]);
	}
}
