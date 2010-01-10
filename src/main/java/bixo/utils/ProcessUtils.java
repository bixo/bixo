package bixo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.log4j.Logger;

public class ProcessUtils {

    private static final Logger LOGGER = Logger.getLogger(ProcessUtils.class);

    public static int execute(String command, StringBuffer infoBuffer, StringBuffer errorBuffer,
            String[] environmentVariables) throws InterruptedException, IOException {
        LOGGER.debug("executing: " + command);
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command, environmentVariables);
        return processStream(infoBuffer, errorBuffer, process);
    }

    public static int execute(String[] command, StringBuffer infoBuffer, StringBuffer errorBuffer,
            String[] environmentVariables) throws IOException, InterruptedException {
        LOGGER.debug("executing: " + Arrays.asList(command));
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command, environmentVariables);
        return processStream(infoBuffer, errorBuffer, process);
    }

    private static int processStream(StringBuffer infoBuffer, StringBuffer errorBuffer,
            Process process) throws InterruptedException {
        Thread inThread = readStreamToBuff("in", process.getInputStream(), infoBuffer);
        Thread errThread = readStreamToBuff("err", process.getErrorStream(), errorBuffer);
        inThread.join();
        errThread.join();
        int result = process.waitFor();
        return result;
    }

    private static Thread readStreamToBuff(final String name, final InputStream inputStream,
            final StringBuffer buffer) {
        Thread readThread = new Thread(new Runnable() {
            public void run() {
                String line = null;
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                try {
                    while ((line = reader.readLine()) != null) {
                        buffer.append(line + '\n');
                    }
                    
                    reader.close();
                } catch (IOException e) {
                    LOGGER.error("error on process '" + name + "' stream", e);
                }
            }
        });
        
        readThread.setName("read-on-" + name);
        readThread.setDaemon(true);
        readThread.start();
        return readThread;
    }
    

}
