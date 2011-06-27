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
 *
 * Also replace new lines and tabs in occurrences by simple whitespace to make
 * the field save for Tab Separated Values format storage using PigStorage.
 */
public class AggregateTextBag extends EvalFunc<String> {

    protected final Tokenizer tokenizer = SimpleTokenizer.INSTANCE;

    protected final int sizeLimit;

    protected boolean tsvSafe;

    public AggregateTextBag() throws IOException {
        this(100000);
    }

    public AggregateTextBag(int sizeLimit) throws IOException {
        this(sizeLimit, false);
    }

    public AggregateTextBag(int sizeLimit, boolean tsvSafe) throws IOException {
        super();
        this.sizeLimit = sizeLimit;
        this.tsvSafe = tsvSafe;
    }

    /**
     * Tuple element is expected to be a bag.
     */
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            Object bag = input.get(0);
            String text = "";
            if (bag instanceof String) {
                text = (String) bag;
            } else if (bag instanceof DataBag) {
                DataBag textBag = (DataBag) bag;
                if (textBag.size() == 0) {
                    return text;
                }
                int individualLimit = sizeLimit / (int) textBag.size();
                // take at least 200 chars for each item
                individualLimit = Math.max(individualLimit, 200);
                int remaining = sizeLimit;
                StringBuilder sb = new StringBuilder();
                Iterator<Tuple> it = textBag.iterator();
                while (remaining > 0 && it.hasNext()) {
                    // assume that all the elements of the tuples in the textField bag are the
                    // same sentence grouped several times, hence only consider the first element
                    String nextString = (String) it.next().get(0);

                    // approximate shortening the text so that samples from the same bag contribute in equal
                    // ways to the aggregate rather that dropping the end of the bag in case of limit
                    if (nextString.length() > individualLimit) {
                        int nextWhitespace = nextString.indexOf(" ", individualLimit - 1);
                        if (nextWhitespace != -1) {
                            nextString = nextString.substring(0, nextWhitespace);
                        } else {
                            nextString = nextString.substring(0, individualLimit);
                        }
                    }
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
            if (tsvSafe) {
                return SafeTsvText.escape(text);
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
