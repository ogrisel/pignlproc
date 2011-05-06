package pignlproc.evaluation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Ensure that the given string can be used as a field in a TSV file.
 */
public class SafeTsvText extends EvalFunc<String> {

    /**
     * Tuple is expected to hold a single String field.
     */
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            Object field = input.get(0);
            String text;
            if (field instanceof String) {
                text = (String) field;
            } else {
                throw new ExecException(String.format(
                        "Illegal value for text field: %s."
                                + " Expected instance of charray", field));
            }
            return escape(text);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing aggregate text of bag in "
                    + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    public static String escape(String text) {
        text = text.replaceAll("[\t\n]", " ");
        text = text.replaceAll("\"", "\"\"");
        return "\"" + text + "\"";
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
