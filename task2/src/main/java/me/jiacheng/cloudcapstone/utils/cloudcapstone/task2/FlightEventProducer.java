package me.jiacheng.cloudcapstone.utils.cloudcapstone.task2;

import org.apache.log4j.Logger;
import org.apache.tools.ant.DirectoryScanner;

/**
 * Created by jiacheng on 12/02/16.
 */
public class FlightEventProducer {
    public static final Logger LOG = Logger.getLogger(FlightEventProducer.class);

    public static void produceFromFile(String filename) {
        LOG.debug("reading from file " + filename);
    }

    public static void main(String[] argv) {
        if (argv.length != 3) {
            System.out.println("Usage: FlightEventProducer <broker list> <zookeeper> <file pattern>");
            System.exit(-1);
        }

        LOG.debug("Using broker: " + argv[0] + ", zookeeper: " + argv[1]);

        DirectoryScanner scanner = new DirectoryScanner();
        scanner.setIncludes(new String[] {argv[2]});
        scanner.scan();

        for (String filename: scanner.getIncludedFiles()) {
            produceFromFile(filename);
        }
    }
}
