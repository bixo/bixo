/*
 * Copyright 2009-2015 Scale Unlimited
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

import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;


public class CrawlDirUtils {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CrawlDirUtils.class);
    
	private static final Pattern LOOP_DIRNAME_PATTERN = Pattern
			.compile("(\\d+)-([^/]+)");

	private CrawlDirUtils() {
        // Enforce class isn't instantiated
    }
	
	
	/**
	 * 
	 * @param platform
	 * @param outputDir
	 * @param loopNumber
	 * @return Directory path <loopNumber>-<timestamp>
	 * @throws Exception 
	 */
	public static BasePath makeLoopDir(BasePlatform platform, BasePath outputDir, int loopNumber)
			throws Exception {
		String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
				.format(new Date());
		BasePath loopDir = platform.makePath(outputDir, "" + loopNumber + "-" + timestamp);
		loopDir.mkdirs();
		return loopDir;
	}

	/**
	 * 
     * @param platform
	 * @param outputPath
	 * @return Path to the latest loop directory (based on the loop number); 
	 *         null in the case of an error.
	 * @throws Exception 
	 */
	public static BasePath findLatestLoopDir(BasePlatform platform, BasePath outputPath)
			throws Exception {
		int bestLoop = -1;
		BasePath result = null;

		BasePath[] paths = outputPath.list();
		for (BasePath path : paths) {
			if (!path.isDirectory()) {
				continue;
			}
			try {
			int curLoop = extractLoopNumber(path);
			if (curLoop > bestLoop) {
				bestLoop = curLoop;
				result = path;
			}
			} catch (InvalidParameterException e) {
			    // Ignore - we could have non loop-dir dirs
			}
		}

		return result;
	}

	/**
	 * Given a loopNumber, returns the name of the next loop directory. 
	 * 
	 * @param platform
	 * @param outputPath
	 * @param loopNumber
	 * @return Name of the next loop directory if one is present; null otherwise. 
	 * @throws Exception 
	 */
	public static BasePath findNextLoopDir(BasePlatform platform, BasePath outputPath,
			int loopNumber) throws Exception {
		int bestLoop = Integer.MAX_VALUE;
		BasePath result = null;

		BasePath[] paths = outputPath.list();
		for (BasePath path : paths) {
			if (!path.isDirectory()) {
				continue;
			}

			int curLoop = extractLoopNumber(path);
			if ((curLoop > loopNumber) && (curLoop < bestLoop)) {
				bestLoop = curLoop;
				result = path;
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
	public static int extractLoopNumber(BasePath inputPath)
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
	 * @param platform
	 * @param outputPath
	 * @param subdirName
	 * @return Array of directory paths that contain <subdirName>
	 * @throws Exception 
	 */
    public static BasePath[] findAllSubdirs(BasePlatform platform, BasePath outputPath,
			String subdirName) throws Exception {
		ArrayList<BasePath> result = new ArrayList<BasePath>();

		BasePath[] paths = outputPath.list();
		for (BasePath path : paths) {
			if (!path.isDirectory()) {
				continue;
			}

            try {
                // Verify crawl dir name is valid.
                extractLoopNumber(path);

                BasePath subdirPath = platform.makePath(path, subdirName);
                if (subdirPath.exists() && subdirPath.isDirectory()) {
                    result.add(subdirPath);
                }
            } catch (InvalidParameterException e) {
                LOGGER.debug("Ignoring directory :" + path.getName());
            }
		}

		return result.toArray(new BasePath[result.size()]);
	}
}
