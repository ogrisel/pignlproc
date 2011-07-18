package pignlproc.evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import opennlp.tools.util.Span;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Before;
import org.junit.Test;

public class TestEvalFunctions {

    public static final String JOHN_SENTENCE = "John Smith works at Smith Consulting.";

    public static final String JOHN_SENTENCE_WITH_TABS = "John\tSmith\tworks\nat Smith Consulting.";

    protected MergeAsOpenNLPAnnotatedText merger;

    protected AggregateTextBag aggregator;

    protected ConcatTextBag concatTextBag;

    @Before
    public void setUp() throws IOException {
        merger = new MergeAsOpenNLPAnnotatedText();
        aggregator = new AggregateTextBag(40, true);
        concatTextBag = new ConcatTextBag(" _ ", true);
    }

    @Test
    public void testSimpleSentenceMerge() throws ExecException {
        String sentence = JOHN_SENTENCE;
        List<Span> names = Arrays.asList(new Span(0, 10, "person"), new Span(
                19, 36, "organization"));
        String merged = merger.merge(sentence, names);
        assertEquals("<START:person> John Smith <END> works"
                + " at <START:organization> Smith Consulting <END> .", merged);

        names = Arrays.asList(new Span(0, 10), new Span(19, 36));
        merged = merger.merge(sentence, names);
        assertEquals("<START> John Smith <END> works"
                + " at <START> Smith Consulting <END> .", merged);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBagOfAnnotationMerge() throws IOException {
        TupleFactory tf = TupleFactory.getInstance();
        DefaultDataBag textBag = new DefaultDataBag();
        DefaultDataBag beginBag = new DefaultDataBag();
        DefaultDataBag endBag = new DefaultDataBag();
        DefaultDataBag typeBag = new DefaultDataBag();

        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        beginBag.add(tf.newTupleNoCopy(Arrays.asList(0)));
        endBag.add(tf.newTupleNoCopy(Arrays.asList(10)));
        typeBag.add(tf.newTupleNoCopy(Arrays.asList("person")));

        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        beginBag.add(tf.newTupleNoCopy(Arrays.asList(19)));
        endBag.add(tf.newTupleNoCopy(Arrays.asList(36)));
        typeBag.add(tf.newTupleNoCopy(Arrays.asList("organization")));

        // all bags
        Tuple input = tf.newTupleNoCopy(Arrays.asList(textBag, beginBag,
                endBag, typeBag));
        String merged = merger.exec(input);
        assertEquals("<START:person> John Smith <END> works"
                + " at <START:organization> Smith Consulting <END> .", merged);

        // all literals
        input = tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE, 0, 10, "person"));
        merged = merger.exec(input);
        assertEquals(
                "<START:person> John Smith <END> works at Smith Consulting .",
                merged);

        // bags without types
        input = tf.newTupleNoCopy(Arrays.asList(textBag, beginBag, endBag));
        merged = merger.exec(input);
        assertEquals(
                "<START> John Smith <END> works at <START> Smith Consulting <END> .",
                merged);

        // bags with fixed type
        input = tf.newTupleNoCopy(Arrays.asList(textBag, beginBag, endBag,
                "entity"));
        merged = merger.exec(input);
        assertEquals("<START:entity> John Smith <END> works at"
                + " <START:entity> Smith Consulting <END> .", merged);
    }

    @Test
    public void testAggregateBagOfText() throws IOException {
        TupleFactory tf = TupleFactory.getInstance();
        DefaultDataBag textBag = new DefaultDataBag();

        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE)));

        // all bags
        Tuple input = tf.newTupleNoCopy(Arrays.asList(textBag));
        String merged = aggregator.exec(input);
        String expected = StringUtils.join(
                Arrays.asList(JOHN_SENTENCE, JOHN_SENTENCE), " ");
        expected = "\"" + expected + "\"";
        assertEquals(expected, merged);
    }

    @Test
    public void testAggregateBagOfTextWithTabs() throws IOException {
        TupleFactory tf = TupleFactory.getInstance();
        DefaultDataBag textBag = new DefaultDataBag();

        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(JOHN_SENTENCE_WITH_TABS)));

        // all bags
        Tuple input = tf.newTupleNoCopy(Arrays.asList(textBag));
        String merged = aggregator.exec(input);
        String expected = StringUtils.join(
                Arrays.asList(JOHN_SENTENCE, JOHN_SENTENCE), " ");
        expected = "\"" + expected + "\"";
        assertEquals(expected, merged);
    }

    @Test
    public void testConcatTextBag() throws IOException {
        TupleFactory tf = TupleFactory.getInstance();
        DefaultDataBag textBag = new DefaultDataBag();

        textBag.add(tf.newTupleNoCopy(Arrays.asList(" foo1")));
        textBag.add(tf.newTupleNoCopy(Arrays.asList("foo2")));
        textBag.add(tf.newTupleNoCopy(Arrays.asList(" foo3 ")));
        textBag.add(tf.newTupleNoCopy(Arrays.asList("foo1")));
        textBag.add(tf.newTupleNoCopy(Arrays.asList("foo4 ")));

        // all bags
        Tuple input = tf.newTupleNoCopy(Arrays.asList(textBag));
        String merged = concatTextBag.exec(input);
        String expected = "foo1 _ foo2 _ foo3 _ foo4";
        assertEquals(expected, merged);
    }

    @Test
    public void testNoLoopInPath() throws IOException {
        NoLoopInPath noloopInPathFunc = new NoLoopInPath(" ");
        TupleFactory tf = TupleFactory.getInstance();

        assertTrue(noloopInPathFunc.exec(null));
        assertTrue(noloopInPathFunc.exec(tf.newTupleNoCopy(Collections.emptyList())));
        assertTrue(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList(""))));
        assertTrue(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a"))));
        assertTrue(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a b c d"))));

        assertFalse(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a a"))));
        assertFalse(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a b c a"))));
        assertFalse(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a b c b"))));
        assertFalse(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a b c c"))));

        // for efficiency reason, this function only check for loops introduced
        // by the last element:
        assertTrue(noloopInPathFunc.exec(tf.newTupleNoCopy(Arrays.asList("a b b d"))));
    }

    @Test
    public void testCheckAbstract() throws IOException {
        CheckAbstract abstractChecker = new CheckAbstract();
        TupleFactory tf = TupleFactory.getInstance();
        assertFalse(abstractChecker.exec(null));
        assertFalse(abstractChecker.exec(tf.newTupleNoCopy(Collections.emptyList())));
        assertFalse(abstractChecker.exec(tf.newTupleNoCopy(Arrays.asList((String) null))));
        assertFalse(abstractChecker.exec(tf.newTupleNoCopy(Arrays.asList(""))));
        assertFalse(abstractChecker.exec(tf.newTupleNoCopy(Arrays.asList("Some short boring abstract."))));
        String longBoringAbstract = "Elections by country gives information on elections."
                + " For each de jure and de facto sovereign state and dependent territory"
                + " an article on elections in that entity has been included and "
                + "information on the way the head of state and the parliament or "
                + "legislature is elected. The articles include the results of the "
                + "elections. For a chronological order, see the electoral calendar."
                + " Contents Top · 0–9 · A B C D E F G H I J K L M N O P Q R S T U V W X Y Z";
        assertFalse(abstractChecker.exec(tf.newTupleNoCopy(Arrays.asList(longBoringAbstract))));
        String interestingAbstract = "Mathematics is the study of quantity, structure,"
                + " space, and change. Mathematicians seek out patterns,"
                + " formulate new conjectures, and establish truth by rigorous"
                + " deduction from appropriately chosen axioms and definitions."
                + " There is debate over whether mathematical objects such as"
                + " numbers and points exist naturally or are human creations."
                + " The mathematician Benjamin Peirce called mathematics \"the"
                + " science that draws necessary conclusions\". Albert Einstein,"
                + " on the other hand, stated that \"as far as the laws of"
                + " mathematics refer to reality, they are not certain; and as"
                + " far as they are certain, they do not refer to reality. \""
                + " Through the use of abstraction and logical reasoning,"
                + " mathematics evolved from counting, calculation, measurement,"
                + " and the systematic study of the shapes and motions of"
                + " physical objects.";
        assertTrue(abstractChecker.exec(tf.newTupleNoCopy(Arrays.asList(interestingAbstract))));
    }
}
