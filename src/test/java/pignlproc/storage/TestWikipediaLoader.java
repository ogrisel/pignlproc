package pignlproc.storage;

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestWikipediaLoader {

    @Test
    public void testLoadXMLLoader() throws Exception {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "enwiki-20090902-pages-articles-sample.xml");
        String filename = wikiDump.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file:"
                + filename
                + "' USING pignlproc.storage.WikipediaLoader() as (title:chararray, markup:chararray);";
        pig.registerQuery(query);
        Iterator<?> it = pig.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = (Tuple) it.next();
            if (tuple == null)
                break;
            else {
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(4, tupleCount);
    }
}
