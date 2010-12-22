package pignlproc.evaluation;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.Span;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Parse the text using a sentence tokenizer and for each link found emit a
 * tuple with link info contextualized to the enclosing sentence.
 * 
 * Paragraph location is used to ensure that the sentences do not span paragraph
 * boundaries.
 * 
 * Sentences are also given a sequential number that can be useful for
 * reordering purposes.
 * 
 * If a given sentence hold several link, the output bag will include as one
 * sentence tuple for each link.
 */
public class SentencesWithLink extends EvalFunc<DataBag> {

    public static final String ENGLISH_SENTMODEL_PATH = "opennlp/en-sent.bin";

    TupleFactory tupleFactory = TupleFactory.getInstance();

    BagFactory bagFactory = BagFactory.getInstance();

    protected final SentenceModel model;

    public SentencesWithLink() throws IOException {
        ClassLoader loader = this.getClass().getClassLoader();
        // TODO: un-hardcode the model language
        String path = ENGLISH_SENTMODEL_PATH;
        InputStream in = loader.getResourceAsStream(path);
        if (in == null) {
            String message = String.format("Failed to find resource for model"
                    + " sentence detection model: %s", path);
            log.error(message);
            throw new IOException(message);
        }
        model = new SentenceModel(in);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);
        DataBag output = bagFactory.newDefaultBag();
        Object t0 = input.get(0);
        if (!(t0 instanceof String)) {
            throw new IOException("Expected input to be chararray, but got "
                    + t0.getClass().getName());
        }
        Object t1 = input.get(1);
        if (!(t1 instanceof DataBag)) {
            throw new IOException("Expected input to be bag of links, but got "
                    + t1.getClass().getName());
        }
        Object t2 = input.get(2);
        if (!(t1 instanceof DataBag)) {
            throw new IOException(
                    "Expected input to be bag of paragraphs, but got "
                            + t2.getClass().getName());
        }
        String text = (String) t0;
        DataBag links = (DataBag) t1;
        DataBag paragraphBag = (DataBag) t2;

        // convert the bag of links as absolute spans over the text
        List<Span> linkSpans = new ArrayList<Span>();
        for (Tuple l : links) {
            linkSpans.add(new Span((Integer) l.get(1), (Integer) l.get(2),
                    (String) l.get(0)));
        }
        Collections.sort(linkSpans);

        // iterate of the paragraph and extract sentence locations
        int order = 0;
        for (Tuple p : paragraphBag) {
            Integer beginParagraph = (Integer) p.get(1);
            Integer endParagraph = (Integer) p.get(2);
            Span[] spans = sentenceDetector.sentPosDetect(text.substring(
                    beginParagraph, endParagraph));
            for (Span sentenceRelative : spans) {
                // for each sentence found in that paragraph, compute the
                // absolute span of the text
                order++;
                Span absoluteSentence = new Span(beginParagraph
                        + sentenceRelative.getStart(), beginParagraph
                        + sentenceRelative.getEnd(), sentenceRelative.getType());

                String sentence = text.substring(absoluteSentence.getStart(),
                        absoluteSentence.getEnd());
                // replace some formatting white-spaces without changing the
                // number of chars not to break the annotations
                sentence = sentence.replaceAll("\n", " ");
                sentence = sentence.replaceAll("\t", " ");

                // for each link in that sentence, emit a tuple
                for (Span link : linkSpans) {
                    // TODO: optimize me by leveraging the link ordering
                    if (absoluteSentence.contains(link)) {
                        int begin = link.getStart()
                                - absoluteSentence.getStart();
                        int end = link.getEnd() - absoluteSentence.getStart();
                        output.add(tupleFactory.newTupleNoCopy(Arrays.asList(
                                order, sentence, link.getType(), begin, end)));
                    } else if (link.compareTo(absoluteSentence) > 1) {
                        break;
                    }
                }
            }
        }
        return output;

    }

    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new FieldSchema("sentenceOrder", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("sentence", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("linkTarget", DataType.CHARARRAY));
            tupleSchema.add(new FieldSchema("linkBegin", DataType.INTEGER));
            tupleSchema.add(new FieldSchema("linkEnd", DataType.INTEGER));
            return new Schema(new Schema.FieldSchema(getSchemaName(
                    this.getClass().getName().toLowerCase(), input),
                    tupleSchema, DataType.BAG));
        } catch (Exception e) {
            return null;
        }
    }

}
