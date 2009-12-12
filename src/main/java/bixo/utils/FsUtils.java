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

public class FsUtils {
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
	
	public static Path makeLoopDir(FileSystem fs, Path outputDir, int loopNumber)
			throws IOException {
		String timestamp = new SimpleDateFormat("yyyyMMdd'T'HHmmss")
				.format(new Date());
		Path loopDir = new Path(outputDir, "" + loopNumber + "-" + timestamp);
		fs.mkdirs(loopDir);
		return loopDir;
	}

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
	
	// Return an array of paths to all of the subdirs in crawl dirs found
	// inside of <crawlPath>, where the subdir name == <subdirName>.
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
				FileStatus[] subdirStatus = listStatus(fs, subdirPath);
				if ((subdirStatus.length == 1) && (subdirStatus[0].isDir())) {
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
