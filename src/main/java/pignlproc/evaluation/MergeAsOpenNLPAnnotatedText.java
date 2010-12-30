package pignlproc.evaluation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import opennlp.tools.namefind.NameSampleDataStream;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.util.Span;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import pignlproc.helpers.SpanHelper;

/**
 * Merge a bag of external annotation tuples and text (e.g. sentences) as a
 * single text with inline tag annotations using a format suitable for OpenNLP
 * NameFinderME model training for instance.
 */
public class MergeAsOpenNLPAnnotatedText extends EvalFunc<String> {

    public static final String ENGLISH_TOKENMODEL_PATH = "opennlp/en-token.bin";

    protected final TokenizerModel model;

    // TODO: make it possible to configure a global type in the constructor

    public MergeAsOpenNLPAnnotatedText() throws IOException {
        ClassLoader loader = this.getClass().getClassLoader();
        // TODO: make it possible to path the language code and or midel path as
        // argument
        String path = ENGLISH_TOKENMODEL_PATH;
        InputStream in = loader.getResourceAsStream(path);
        if (in == null) {
            String message = String.format("Failed to find resource for model"
                    + " tokenizer model: %s", path);
            log.error(message);
            throw new IOException(message);
        }
        model = new TokenizerModel(in);
    }

    /**
     * If tuple elements are bags, aggregate the annotations into the same text
     * element.
     * 
     * The first field is expected to be the text of the sentence to merge the
     * annotation into.
     * 
     * The second and third fields are expected to be the (bag of) integer
     * locations of the begin and end of each annotation.
     * 
     * The optional fourth field is the type value of the annotation type (bag
     * of) String.
     */
    @Override
    public String exec(Tuple input) throws IOException {

        if (input.size() != 3 && input.size() != 4) {
            throw new ExecException(String.format(
                    "invalid number of fields: %d."
                            + " Expected 3 or 4 fields with text content,"
                            + " begin and end int locations"
                            + " and optional type String", input.size()));
        }
        try {
            // TODO: use global type info as default instead of null if
            // available
            Object textField = input.get(0);
            String text;
            if (textField instanceof String) {
                text = (String) textField;
            } else if (textField instanceof DataBag) {
                DataBag textBag = (DataBag) textField;
                if (textBag.size() == 0) {
                    // if we were handed an empty bag, return NULL
                    // this is in compliance with SQL standard
                    return null;
                }
                // assume that all the element of the textField bag are the same
                // sentence grouped several times.
                text = (String) textBag.iterator().next().get(0);
            } else {
                throw new ExecException(String.format(
                        "Illegal value for text field: %s."
                                + " Expected instance of charray or bag",
                        textField));
            }

            Object type = input.size() == 4 ? input.get(3) : null;
            List<Span> links = SpanHelper.tupleFieldsToSpans(input.get(1),
                    input.get(2), type);
            return merge(text, links);
        } catch (ExecException ee) {
            throw ee;
        } catch (Exception e) {
            int errCode = 2106;
            String msg = "Error while computing merged annotations in "
                    + this.getClass().getSimpleName();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    public String merge(String text, List<Span> links) throws ExecException {
        Collections.sort(links);
        TokenizerME tokenizer = new TokenizerME(model);

        List<Span> tokens = Arrays.asList(tokenizer.tokenizePos(text));
        Iterator<Span> tokensIterator = tokens.iterator();
        Iterator<Span> linksIterator = links.iterator();

        Span nextToken = null;
        Span activeLink = null;
        Span nextLink = null;

        StringBuilder sb = new StringBuilder();
        while (linksIterator.hasNext()) {
            // peek at the next link
            nextLink = linksIterator.next();
            while (nextLink != null
                    && (nextToken != null || tokensIterator.hasNext())) {
                nextToken = nextToken == null ? tokensIterator.next()
                        : nextToken;
                if (nextLink.contains(nextToken)) {
                    activeLink = nextLink;
                    nextLink = null;
                    if (activeLink.getType() != null) {
                        sb.append(NameSampleDataStream.START_TAG_PREFIX);
                        sb.append(activeLink.getType());
                        sb.append('>');
                    } else {
                        sb.append(NameSampleDataStream.START_TAG);
                    }
                    sb.append(' ');
                    do {
                        // consume tokens inside an active link
                        sb.append(text.substring(nextToken.getStart(),
                                nextToken.getEnd()));
                        sb.append(' ');
                        nextToken = tokensIterator.hasNext() ? tokensIterator.next()
                                : null;
                    } while (nextToken != null
                            && activeLink.contains(nextToken));
                    sb.append(NameSampleDataStream.END_TAG);
                    sb.append(' ');
                } else {
                    // consume tokens outside of any active link
                    sb.append(text.substring(nextToken.getStart(),
                            nextToken.getEnd()));
                    sb.append(' ');
                    nextToken = null;
                }
            }
        }
        // consume the remaining tokens outside of the last link
        while (nextToken != null || tokensIterator.hasNext()) {
            nextToken = nextToken == null ? tokensIterator.next() : nextToken;
            sb.append(text.substring(nextToken.getStart(), nextToken.getEnd()));
            sb.append(' ');
            nextToken = null;
        }
        return sb.toString().trim();
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
