package com.finderbots.utilities;

import com.bixolabs.cascading.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterator;

import java.io.IOException;
import java.util.ArrayList;

/**
 * User: pat
 * Date: 10/8/12
 * Time: 12:40 PM
 * To change this template use File | Settings | File Templates.
 */

public class MahoutUtilities {
    public static SequenceFileDirIterator<?, ?> getMultiFileIterator(Path p, String namePattern ) throws IOException, InterruptedException {
        Configuration conf = HadoopUtils.getDefaultJobConf();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] dirStats = fs.listStatus(p);
        ArrayList<Path>filePaths = new ArrayList<Path>();
        SequenceFileDirIterator<?, ?> it = null;
        for( FileStatus f : dirStats ){
            if(f.getPath().toString().contains(namePattern)){//namePattern has some part of the name we are looking for
                // like "art-" for a "part-x-xxxxx" file
                filePaths.add(f.getPath());
            } //otherwise not of interest, maybe a status or temp file
        }
        Path[] fileList = filePaths.toArray(new Path[filePaths.size()]);
        if( fileList.length > 0 ){
            it = new SequenceFileDirIterator<Writable, Writable>(fileList, true, conf);
        }
        return it;
    }

}

