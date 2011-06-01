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

public class TestUriStringLiteralNTriplesStorer {

    protected File outputFile;

    public void cleanupOutputFile() {
        FileUtils.deleteQuietly(outputFile);
    }

    @Test
    public void testUriStringLiteralStorer() throws Exception {
        PigServer pig = new PigServer(LOCAL);
        URL dbpediaDump = Thread.currentThread().getContextClassLoader().getResource("graph.tsv");
        String inputFilename = dbpediaDump.getPath();
        inputFilename = inputFilename.replace("\\", "\\\\");
        outputFile = File.createTempFile("testUriUriStorer-", ".nt");
        FileUtils.deleteQuietly(outputFile);
        String outputFilename = outputFile.getAbsolutePath().replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + inputFilename + "' AS (s: chararray, o: chararray);";
        pig.registerQuery(query);
        pig.store("A", outputFilename,
            "pignlproc.storage.UriStringLiteralNTriplesStorer('http://mynamespace#hasValue')");
        assertTrue(outputFile.exists());
        File outPart = new File(outputFile, "part-m-00000");
        assertTrue(outPart.exists());
        List<String> lines = FileUtils.readLines(outPart);
        assertEquals(5, lines.size());
        assertEquals("<a> <http://mynamespace#hasValue> \"b\" .", lines.get(0));
        assertEquals("<a> <http://mynamespace#hasValue> \"c\\u00E9\" .", lines.get(1));
        assertEquals("<d> <http://mynamespace#hasValue> \"a\" .", lines.get(2));
    }

    @Test
    public void testUriStringLiteralStorerWithNamespaceAndLang() throws Exception {
        PigServer pig = new PigServer(LOCAL);
        URL dbpediaDump = Thread.currentThread().getContextClassLoader().getResource("graph.tsv");
        String inputFilename = dbpediaDump.getPath();
        inputFilename = inputFilename.replace("\\", "\\\\");
        outputFile = File.createTempFile("testUriUriStorer-", ".nt");
        FileUtils.deleteQuietly(outputFile);
        String outputFilename = outputFile.getAbsolutePath().replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + inputFilename + "' AS (s: chararray, o: chararray);";
        pig.registerQuery(query);
        pig.store("A", outputFilename,
            "pignlproc.storage.UriStringLiteralNTriplesStorer(" +
            "'http://mynamespace#hasValue', 'http://example.org/source#', 'en')");
        assertTrue(outputFile.exists());
        File outPart = new File(outputFile, "part-m-00000");
        assertTrue(outPart.exists());
        List<String> lines = FileUtils.readLines(outPart);
        assertEquals(5, lines.size());
        assertEquals("<http://example.org/source#a> <http://mynamespace#hasValue> \"b\"@en .", lines.get(0));
        assertEquals("<http://example.org/source#a> <http://mynamespace#hasValue> \"c\\u00E9\"@en .", lines.get(1));
        assertEquals("<http://example.org/source#d> <http://mynamespace#hasValue> \"a\"@en .", lines.get(2));
    }
}
