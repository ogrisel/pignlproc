package pignlproc.evaluation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import opennlp.tools.util.Span;

import org.apache.pig.backend.executionengine.ExecException;
import org.junit.Before;
import org.junit.Test;

public class TestMergeAsOpenNLPAnnotatedText {

    protected MergeAsOpenNLPAnnotatedText merger;

    @Before
    public void setUp() throws IOException {
        merger = new MergeAsOpenNLPAnnotatedText();
    }

    @Test
    public void testSimpleSentenceMerge() throws ExecException {
        String sentence = "John Smith works at Smith Consulting.";
        List<Span> names = Arrays.asList(new Span(0, 10, "person"), new Span(
                19, 36, "organization"));
        String merged = merger.merge(sentence, names);
        assertEquals("<START:person> John Smith <END> works"
                + " at <START:organization> Smith Consulting <END> .", merged);
    }

}
