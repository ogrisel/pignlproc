package pignlproc.storage;

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.net.URL;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Test;

public class TestUriStringLiteralNTriplesLoader {

    @Test
    public void testUriStringLiteralNTriplesLoader() throws Exception {
        URL dbpediaDump = Thread.currentThread().getContextClassLoader().getResource(
                "dbpedia_3.4_longabstract_en.nt");
        String filename = dbpediaDump.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file:" + filename
                + "' USING pignlproc.storage.UriStringLiteralNTriplesLoader()"
                + " as (s: chararray, o: chararray, l: chararray);";
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        int tupleCount = 0;
        Tuple tuple = null;
        while (it.hasNext()) {
            tuple = it.next();
            assertNotNull(tuple);
            assertEquals(3, tuple.size());
            if (tuple.size() > 0) {
                tupleCount++;
            }
        }
        assertEquals(22, tupleCount);
        // introspect last tuple
        assertEquals("http://dbpedia.org/resource/%22C%22_Is_for_Corpse",
                tuple.get(0));
        assertEquals(
                "\"C\" Is for Corpse is the third novel in Sue Grafton's \"Alphabet\""
                        + " series of mystery novels and features Kinsey Millhone, a private eye"
                        + " based in Santa Teresa, California.", tuple.get(1));
        assertEquals("en", tuple.get(2));
    }
}
