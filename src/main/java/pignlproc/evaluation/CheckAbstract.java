package pignlproc.evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Check if the abstract of an article looks interesting or not. - must be
 * longer than a minimum number of words - must not contain some hardcoded
 * blacklisted patterns
 */
public class CheckAbstract extends EvalFunc<Boolean> {

    protected final int minWords;

    protected final List<Pattern> blackListedPatterns = new ArrayList<Pattern>();

    public CheckAbstract() throws IOException {
        this(80);
    }

    public CheckAbstract(int minWords) throws IOException {
        super();
        this.minWords = minWords;

        // list of articles or other category index
        blackListedPatterns.add(Pattern.compile("A B C D E F G H I J K L M N O P Q R S T U V W X Y Z"));
    }

    /**
     * Tuple element is expected to be a single string.
     */
    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            if (input == null) {
                return false;
            }
            if (input.size() < 1) {
                return false;
            }
            Object ob = input.get(0);
            if (ob == null) {
                return false;
            } else if (ob instanceof String) {
                String content = (String) ob;
                if (content.isEmpty()) {
                    return false;
                }
                if (content.split(" ").length < minWords) {
                    return false;
                }
                for (Pattern pattern : blackListedPatterns) {
                    if (pattern.matcher(content).find()) {
                        return false;
                    }
                }
                return true;
            } else {
                throw new ExecException(String.format(
                        "Illegal value for field: %s."
                                + " Expected instance of charray", ob));
            }
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
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }

}
