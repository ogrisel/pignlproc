package pignlproc.storage;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class UriUriNTriplesStorer extends AbstractNTriplesStorer {

    protected final String pattern;

    public UriUriNTriplesStorer(String propertyUri) {
        this(propertyUri, null, null);
    }

    public UriUriNTriplesStorer(String propertyUri, String subjectNamespace, String objectNamespace) {
        super();
        if (subjectNamespace == null) {
            subjectNamespace = "";
        }
        if (objectNamespace == null) {
            objectNamespace = "";
        }
        pattern = "<" + subjectNamespace + "%s> " + String.format("<%s> <%s", propertyUri, objectNamespace)
                  + "%s> .";
    }

    @Override
    public void putNext(Tuple fields) throws IOException {
        if (fields.isNull()) {
            // skip null entries
            return;
        }
        if (fields.size() != 2) {
            throw new ExecException("Expected 2 chararray fields in input tuple: subject and object");
        }
        if (fields.isNull(0) || fields.isNull(1)) {
            // skip null entries
            return;
        }
        try {
            writer.write(null, String.format(pattern, fields.get(0), fields.get(1)));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}
