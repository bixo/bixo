package bixo.fetcher.simulation;

import it.unimi.dsi.fastutil.io.BinIO;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

/**
 * Creates a list of urls from a webgraph. Urls can be duplicated.
 * 
 */
public class ExtractWebgraphUrls {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        String usage = "ExtractWebgraphUrls <path to webgraph.flc> <pathToOutFile>";
        try {

            if (args.length != 2) {
                System.out.println(usage);
                System.exit(-1);
            }
            List<CharSequence> node2url = (List<CharSequence>) BinIO.loadObject(args[0]);
            int count = node2url.size();
            System.out.println(String.format("Writing %d urls.", count));

            File file = new File(args[1]);
            file.getParentFile().mkdirs();
            FileOutputStream outputStream = new FileOutputStream(file);

            for (int i = 0; i < count; i++) {
                CharSequence url = node2url.get(i);
                outputStream.write((url.toString() + "\n").getBytes());
            }
        } catch (Exception e) {
            System.err.println(usage);
            e.printStackTrace();
        }
    }
}
