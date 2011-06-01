package pignlproc.storage;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Serialize key value pairs as NTriples by providing a fixed property URI and optional namespace info the
 * subject. The object is expected to be a string literal with a fixed optional language declaration.
 */
public class UriStringLiteralNTriplesStorer extends AbstractNTriplesStorer {

    protected final String head;

    protected final String middle;

    protected final String tail;

    public UriStringLiteralNTriplesStorer(String propertyUri) {
        this(propertyUri, null, null);
    }

    public UriStringLiteralNTriplesStorer(String propertyUri, String subjectNamespace, String lang) {
        super();
        if (subjectNamespace == null) {
            subjectNamespace = "";
        }
        head = "<" + escapeUtf8ToUsAsciiUri(subjectNamespace);
        middle = "> <" + escapeUtf8ToUsAsciiUri(propertyUri) + "> \"";
        String tail = "\"";
        if (lang != null && !lang.isEmpty()) {
            tail += "@" + lang;
        }
        this.tail = tail + " .";
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
            StringBuilder sb = new StringBuilder(head);
            escapeUtf8ToUsAscii(fields.get(0).toString(), sb, true);
            sb.append(middle);
            escapeUtf8ToUsAscii(fields.get(1).toString(), sb, false);
            sb.append(tail);
            writer.write(null, sb.toString());
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}
