package pignlproc.evaluation;

import java.io.IOException;
import java.util.Iterator;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Merge a bag of single column tuples of texts into a single column tuple of
 * the aggregate text with a limit in size.
 * 
 * The sizeLimit is no a strong limit to avoid cutting text in the middle of an
 * abstract or worst, in the middle of a word.
 */
public class AggregateTextBag extends EvalFunc<String> {

    protected final Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

    protected final int sizeLimit;

    public AggregateTextBag() throws IOException {
        this(10000);
    }

    public AggregateTextBag(int sizeLimit) throws IOException {
        super();
        this.sizeLimit = sizeLimit;
    }

    /**
     * Tuple element is expected to be a bag.
     */
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            Object bag = input.get(0);
            String text;
            if (bag instanceof String) {
                text = (String) bag;
            } else if (bag instanceof DataBag) {
                DataBag textBag = (DataBag) bag;
                int remaining = sizeLimit;
                StringBuilder sb = new StringBuilder();
                Iterator<Tuple> it = textBag.iterator();
                while (remaining > 0 && it.hasNext()) {
                    // assume that all the element of the textField bag are the
                    // same
                    // sentence grouped several times.
                    String nextString = (String) it.next().get(0);
                    if (sb.length() > 0) {
                        sb.append(" ");
                    }
                    sb.append(nextString);
                    remaining -= nextString.length() + 1;
                }
                text = sb.toString();
            } else {
                throw new ExecException(String.format(
                        "Illegal value for text field: %s."
                                + " Expected instance of charray or bag", bag));
            }
            return text;
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing aggregate text of bag in "
                    + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
