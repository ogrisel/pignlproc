package pignlproc.storage;

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.pig.PigServer;
import org.junit.Test;

public class TestUriUriNTriplesStorer {

    protected File outputFile;

    public void cleanupOutputFile() {
        FileUtils.deleteQuietly(outputFile);
    }

    @Test
    public void testUriUriStorer() throws Exception {
        PigServer pig = new PigServer(LOCAL);
        URL dbpediaDump = Thread.currentThread().getContextClassLoader().getResource("followership.tsv");
        String inputFilename = dbpediaDump.getPath();
        inputFilename = inputFilename.replace("\\", "\\\\");
        outputFile = File.createTempFile("testUriUriStorer-", ".nt");
        FileUtils.deleteQuietly(outputFile);
        String outputFilename = outputFile.getAbsolutePath().replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + inputFilename + "' AS (s: chararray, o: chararray);";
        pig.registerQuery(query);
        pig.store("A", outputFilename,
            "pignlproc.storage.UriUriNTriplesStorer('http://mynamespace#followerOf')");
        assertTrue(outputFile.exists());
        File outPart = new File(outputFile, "part-m-00000");
        assertTrue(outPart.exists());
        List<String> lines = FileUtils.readLines(outPart);
        assertEquals(5, lines.size());
        assertEquals("<a> <http://mynamespace#followerOf> <b> .", lines.get(0));
        assertEquals("<a> <http://mynamespace#followerOf> <c> .", lines.get(1));
        assertEquals("<d> <http://mynamespace#followerOf> <a> .", lines.get(2));
    }

}
