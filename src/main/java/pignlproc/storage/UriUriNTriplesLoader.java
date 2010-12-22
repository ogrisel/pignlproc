package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * Pig loader for NTriples formatted RDF files assuming that subject, predicate
 * and object are all valid URIs (e.g. clear of any whitespace character).
 * 
 * Returns the subject and the object. The property value can be given as a
 * filter.
 */
public class UriUriNTriplesLoader extends LoadFunc {

    protected RecordReader<Long, Text> reader;

    protected TupleFactory tupleFactory = TupleFactory.getInstance();

    protected String propertyUri;

    public UriUriNTriplesLoader() {
    }

    public UriUriNTriplesLoader(String propertyUri) {
        this.propertyUri = propertyUri;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public InputFormat getInputFormat() throws IOException {
        return new TextInputFormat();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {
        this.reader = reader;
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            while (reader.nextKeyValue()) {
                String line = reader.getCurrentValue().toString();
                String[] split = line.split(" ");
                if (split.length != 4) {
                    // unexpected spaces, the object must be a literal, skip
                    continue;
                }
                if (propertyUri != null
                        && !propertyUri.equals(stripBrackets(split[1]))) {
                    // property value is not matching the filter
                    continue;
                }
                return tupleFactory.newTupleNoCopy(Arrays.asList(
                        stripBrackets(split[0]), stripBrackets(split[2])));
            }
            return null;
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected String stripBrackets(String bracketedURI) {
        return bracketedURI.substring(1, bracketedURI.length() - 1);
    }
}
