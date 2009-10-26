package bixo.utils;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FsUtils {
	private static final Pattern LOOP_DIRNAME_PATTERN = Pattern
			.compile("(\\d+)-([^/]+)");

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

		FileStatus[] subdirs = fs.listStatus(outputPath);
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

		FileStatus[] subdirs = fs.listStatus(outputPath);
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
}
