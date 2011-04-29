package pignlproc.storage;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Pig loader for NTriples formatted RDF files assuming that subject, predicate
 * and object are all valid URIs (e.g. clear of any whitespace character).
 * 
 * Returns the subject and the object. The property value can be given as a
 * filter.
 */
public class UriUriNTriplesLoader extends AbstractNTriplesLoader {

    public UriUriNTriplesLoader() {
        this(null, null, null);
    }

    public UriUriNTriplesLoader(String propertyUri, String subjectPrefix, String objectPrefix) {
        super();
        this.propertyUri = propertyUri;
        this.subjectPrefix = checkColons(subjectPrefix);
        this.objectPrefix = checkColons(objectPrefix);
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
                        stripBrackets(split[0], subjectPrefix),
                        stripBrackets(split[2], objectPrefix)));
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

    @Override
    public ResourceSchema getSchema(String location, Job job)
            throws IOException {
        Schema schema = new Schema();
        schema.add(new FieldSchema("subject", DataType.CHARARRAY));
        schema.add(new FieldSchema("object", DataType.CHARARRAY));
        return new ResourceSchema(schema);
    }

}
