package pignlproc.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import pignlproc.format.WikipediaPageInputFormat.WikipediaRecordReader;
import pignlproc.markup.Annotation;
import pignlproc.markup.LinkAnnotationTextConverter;

public class TestWikipediaParsing {

    @Test
    public void testEnWikipediaParsingFromReader() throws IOException,
            InterruptedException {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "enwiki-20090902-pages-articles-sample.xml");
        assertNotNull(wikiDump);
        WikipediaRecordReader reader = new WikipediaPageInputFormat.WikipediaRecordReader(
                wikiDump, 0, 100000);

        // first article
        assertTrue(reader.nextKeyValue());
        assertEquals(new Text("AccessibleComputing"), reader.getCurrentKey());
        String markup = reader.getCurrentValue().toString();
        assertEquals(
                "#REDIRECT [[Computer accessibility]] {{R from CamelCase}}",
                markup);

        LinkAnnotationTextConverter converter = new LinkAnnotationTextConverter();
        String simpleText = converter.convert(markup);
        assertEquals("", simpleText);
        assertTrue(converter.getWikiLinks().isEmpty());
        assertEquals("http://en.wikipedia.org/wiki/Computer_accessibility",
                converter.getRedirect());

        // second article
        assertTrue(reader.nextKeyValue());
        assertEquals("Anarchism", reader.getCurrentKey().toString());
        markup = reader.getCurrentValue().toString();
        converter = new LinkAnnotationTextConverter();
        simpleText = converter.convert(markup);
        assertTrue(simpleText.startsWith("\nAnarchism is a political philosophy"
                + " encompassing theories and attitudes"));
        assertEquals(465, converter.getWikiLinks().size());
        Annotation firstLink = converter.getWikiLinks().get(0);
        assertEquals("political philosophy", firstLink.label);
        assertEquals("http://en.wikipedia.org/wiki/Political_philosophy",
                firstLink.value);
        assertEquals(16, firstLink.begin);
        assertEquals(36, firstLink.end);
        assertEquals("political philosophy",
                simpleText.substring(firstLink.begin, firstLink.end));

        // third article
        assertTrue(reader.nextKeyValue());
        assertEquals("AfghanistanHistory", reader.getCurrentKey().toString());
        markup = reader.getCurrentValue().toString();
        converter = new LinkAnnotationTextConverter();
        simpleText = converter.convert(markup);
        assertEquals("", simpleText);
        assertEquals(0, converter.getWikiLinks().size());
        assertEquals("http://en.wikipedia.org/wiki/History_of_Afghanistan",
                converter.getRedirect());

        // fourth article
        assertTrue(reader.nextKeyValue());
        assertEquals("Autism", reader.getCurrentKey().toString());
        markup = reader.getCurrentValue().toString();
        converter = new LinkAnnotationTextConverter();
        simpleText = converter.convert(markup);
        assertTrue(simpleText.contains("Autism is a brain development disorder"
                + " characterized by impaired social interaction and communication"));
        assertEquals(236, converter.getWikiLinks().size());
        firstLink = converter.getWikiLinks().get(0);
        assertEquals("Neurodevelopmental disorder", firstLink.label);
        assertEquals(
                "http://en.wikipedia.org/wiki/Neurodevelopmental_disorder",
                firstLink.value);
        assertEquals(15, firstLink.begin);
        assertEquals(41, firstLink.end);
        assertEquals("brain development disorder",
                simpleText.substring(firstLink.begin, firstLink.end));

        // there is no fifth article in this test file
        assertFalse(reader.nextKeyValue());
    }

    @Test
    public void testFrWikipediaParsingFromReader() throws IOException,
            InterruptedException {
        URL wikiDump = Thread.currentThread().getContextClassLoader().getResource(
                "frwiki-20101103-pages-articles-sample.xml");
        assertNotNull(wikiDump);
        WikipediaRecordReader reader = new WikipediaPageInputFormat.WikipediaRecordReader(
                wikiDump, 0, 100000);

        // first article
        assertTrue(reader.nextKeyValue());
        assertEquals("Antoine Meillet", reader.getCurrentKey().toString());
        String markup = reader.getCurrentValue().toString();
        LinkAnnotationTextConverter converter = new LinkAnnotationTextConverter();
        String simpleText = converter.convert(markup);
        // TODO: handle date templates
        assertTrue(simpleText.startsWith("Paul Jules Antoine Meillet, né le  à Moulins,"
                + " Allier, mort le  à Châteaumeillant"));
        assertEquals(48, converter.getWikiLinks().size());
        assertNull(converter.getRedirect());

        // go to the last article wich is a redirect
        assertTrue(reader.nextKeyValue());
        assertTrue(reader.nextKeyValue());
        assertTrue(reader.nextKeyValue());
        assertTrue(reader.nextKeyValue());
        assertTrue(reader.nextKeyValue());

        assertEquals("Amenophis IV", reader.getCurrentKey().toString());
        markup = reader.getCurrentValue().toString();
        converter = new LinkAnnotationTextConverter();
        assertEquals("", converter.convert(markup));
        assertEquals(0, converter.getWikiLinks().size());
        assertEquals("http://en.wikipedia.org/wiki/Akh%C3%A9naton", converter.getRedirect());

        // this was the last article
        assertFalse(reader.nextKeyValue());
    }
}
