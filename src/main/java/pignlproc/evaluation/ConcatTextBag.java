package pignlproc.evaluation;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Merge a bag of single column tuples of texts using a separator.
 */
public class ConcatTextBag extends EvalFunc<String> {

    protected final String separator;

    protected final boolean trim;

    public ConcatTextBag() throws IOException {
        this(" ", true);
    }

    public ConcatTextBag(String separator) throws IOException {
        this(separator, true);
    }

    public ConcatTextBag(String separator, boolean trim) throws IOException {
        super();
        this.separator = separator;
        this.trim = trim;
    }

    /**
     * Tuple element is expected to be a bag.
     */
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            if (input == null) {
                return "";
            }
            Object bag = input.get(0);
            String text = "";
            if (bag instanceof String) {
                text = (String) bag;
            } else if (bag instanceof DataBag) {
                DataBag textBag = (DataBag) bag;
                if (textBag.size() == 0) {
                    return text;
                }
                StringBuilder sb = new StringBuilder();
                Iterator<Tuple> it = textBag.iterator();
                while (it.hasNext()) {
                    String nextString = (String) it.next().get(0);
                    if (nextString == null) {
                        continue;
                    }
                    if (trim) {
                        nextString = nextString.trim();
                    }
                    if (!nextString.isEmpty()) {
                        if (sb.length() > 0) {
                            sb.append(separator);
                        }
                        sb.append(nextString);
                    }
                }
                text = sb.toString();
            } else {
                throw new ExecException(String.format("Illegal value for text field: %s."
                                                      + " Expected instance of charray or bag", bag));
            }
            return text;
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing aggregate text of bag in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
