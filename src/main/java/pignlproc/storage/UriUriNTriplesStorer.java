package pignlproc.storage;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Serialize key value pairs as NTriples by providing a fixed property URI and optional namespace info the
 * subject and object of the triples.
 */
public class UriUriNTriplesStorer extends AbstractNTriplesStorer {

    protected final String head;

    protected final String middle;

    protected final String tail;

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
        head = "<" + escapeUtf8ToUsAsciiUri(subjectNamespace);
        middle = String.format("> <%s> <%s", escapeUtf8ToUsAsciiUri(propertyUri),
            escapeUtf8ToUsAsciiUri(objectNamespace));
        tail = "> .";
    }

    @Override
    public void putNext(Tuple fields) throws IOException {
        if (fields.size() != 2) {
            throw new ExecException("Expected 2 chararray fields in input tuple: subject and object");
        }
        if (fields.isNull(0) || fields.isNull(1)) {
            // skip null entries
            return;
        }
        try {
            StringBuilder sb = new StringBuilder(head);
            escapeUtf8ToUsAscii(fields.get(0).toString(), sb, true);
            sb.append(middle);
            escapeUtf8ToUsAscii(fields.get(1).toString(), sb, true);
            sb.append(tail);
            writer.write(null, sb.toString());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

}
