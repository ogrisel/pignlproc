package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pig loader for NTriples formatted RDF files assuming that subject, predicate
 * and object are all valid URIs (e.g. clear of any whitespace character).
 * 
 * Returns the subject and the object. The property value can be given as a
 * filter.
 */
public class UriUriNTriplesLoader extends LoadFunc implements LoadMetadata {

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
            String line = null;
            while (reader.nextKeyValue()) {
                line = reader.getCurrentValue().toString();
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
            if (line != null) {
                // we reached the end of the file split with an non matching
                // line: return the (null, null) tuple instead of just null
                // since pig was not expecting this split to be fully consumed
                // by introspecting the InputFormat progress data.
                return tupleFactory.newTupleNoCopy(Arrays.asList(null, null));
            }
            return null;
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
            throw new IOException(e);
        }
    }

    protected String stripBrackets(String bracketedURI) {
        return bracketedURI.substring(1, bracketedURI.length() - 1);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Schema schema = new Schema();
        schema.add(new FieldSchema("subject", DataType.CHARARRAY));
        schema.add(new FieldSchema("object", DataType.CHARARRAY));
        return new ResourceSchema(schema);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
    }
}
