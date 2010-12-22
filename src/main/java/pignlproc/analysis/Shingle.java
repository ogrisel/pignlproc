package pignlproc.analysis;

import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;


/**
 * Parse the text using a n-gram tokenizer and emit a tuple with the n-gram.
 */
public class Shingle extends EvalFunc<DataBag> {

    TupleFactory tupleFactory = TupleFactory.getInstance();

    BagFactory bagFactory = BagFactory.getInstance();


    public Shingle() throws IOException {
        // TODO set up parameters for ShingleFilter here if possible.

    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag output = bagFactory.newDefaultBag();

        Object t0 = input.get(0);
        if (!(t0 instanceof Integer)) {
            throw new IOException("Expected input to be Integer, but got "
                    + t0.getClass().getName());
        }

        Object t1 = input.get(1);
        if (!(t1 instanceof String)) {
            throw new IOException(
                    "Expected input to be bag of String, but got "
                            + t1.getClass().getName());
        }
        Integer maxShingles = (Integer) t0;
        String text = (String) t1;

        Reader r = new StringReader(text);
        TokenStream ts = new LowerCaseTokenizer(r);
        TokenStream stream = new ShingleFilter(ts, maxShingles);
        // Convert the output into a bag
        TermAttribute termAttribute = stream.getAttribute(TermAttribute.class);

        while (stream.incrementToken()) {
            String term = termAttribute.toString();
            output.add(tupleFactory.newTuple(term));
        }

        return output;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("max shingle size", DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema("text", DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

}
