package pignlproc.evaluation;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Utility to filter path values to ensure that the last element of the path is not introducing a loop.
 */
public class NoLoopInPath extends EvalFunc<Boolean> {

    protected final String separator;

    public NoLoopInPath() throws IOException {
        this(" ");
    }

    public NoLoopInPath(String separator) throws IOException {
        super();
        this.separator = separator;
    }

    /**
     * Tuple element is expected to be a single string.
     */
    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() == 0) {
                return true;
            }
            Object element = input.get(0);
            if (element instanceof String) {
                String text = (String) element;
                text = text.trim();
                if (text.isEmpty()) {
                    return true;
                }
                String[] pathElements = text.split(separator);
                if (pathElements.length < 2) {
                    return true;
                }
                String lastElement = pathElements[pathElements.length - 1];
                for (int i = 0; i < pathElements.length - 1; i++) {
                    if (lastElement.equals(pathElements[i])) {
                        // last element is introducing a loop
                        return false;
                    }
                }
                return true;
            } else {
                throw new ExecException(String.format("Illegal value for text field: %s."
                                                      + " Expected instance of charray", element));
            }
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while checking loop in " + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }

}
