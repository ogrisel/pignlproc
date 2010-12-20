package pignlproc.markup;

import info.bliki.htmlcleaner.ContentToken;
import info.bliki.htmlcleaner.TagNode;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.filter.WPList;
import info.bliki.wiki.filter.WPTable;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.model.ImageFormat;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.tags.WPATag;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Parse mediawiki markup to strip the formatting info and extract a simple text
 * version suitable for NLP along with link position annotations.
 * 
 * Use the {@code #convert(String)} and {@code #getWikiLinks()} methods.
 */
public class LinkAnnotationTextConverter implements ITextConverter {

    public static final String HREF_ATTR_KEY = "href";

    public static final String WIKILINK_TITLE_ATTR_KEY = "title";

    public static final String WIKILINK_TARGET_ATTR_KEY = "href";

    public static final String WIKIOBJECT_ATTR_KEY = "wikiobject";

    public static final Set<String> TAGS_WITH_1_NEWLINE = new HashSet<String>(
            Arrays.asList("p"));

    public static final Set<String> TAGS_WITH_2_NEWLINES = new HashSet<String>(
            Arrays.asList("h1", "h2", "h3", "h4", "h5", "h6"));

    public static final Pattern INTERWIKI_PATTERN = Pattern.compile("http://[\\w-]+\\.wikipedia\\.org/wiki/.*");

    protected final List<Annotation> wikilinks = new ArrayList<Annotation>();

    protected String languageCode = "en";

    protected final WikiModel model;

    public LinkAnnotationTextConverter() {
        model = makeWikiModel(languageCode);
    }

    public LinkAnnotationTextConverter(String languageCode) {
        this.languageCode = languageCode;
        model = makeWikiModel(languageCode);
    }

    public WikiModel makeWikiModel(String languageCode) {
        return new WikiModel(String.format(
                "http:/%s.wikipedia.org/wiki/${image}", languageCode),
                String.format("http://%s.wikipedia.org/wiki/${title}",
                        languageCode)) {
            @Override
            public String getRawWikiContent(String namespace,
                    String articleName, Map<String, String> templateParameters) {
                // disable template support
                return "";
            }
        };
    }

    /**
     * Convert WikiMarkup to a simple text representation suitable for NLP
     * analysis. The links encountered during the extraction are then available
     * by calling {@code #getWikiLinks()}.
     * 
     * @param rawWikiMarkup
     * @return the simple text without the markup
     */
    public String convert(String rawWikiMarkup) {
        return model.render(this, rawWikiMarkup);
    }

    public void nodesToText(List<? extends Object> nodes, Appendable buffer,
            IWikiModel model) throws IOException {
        CountingAppendable countingBuffer;
        if (buffer instanceof CountingAppendable) {
            countingBuffer = (CountingAppendable) buffer;
        } else {
            // wrap
            countingBuffer = new CountingAppendable(buffer);
        }

        if (nodes != null && !nodes.isEmpty()) {
            try {
                int level = model.incrementRecursionLevel();
                if (level > Configuration.RENDERER_RECURSION_LIMIT) {
                    countingBuffer.append("Error - recursion limit exceeded"
                            + " rendering tags in PlainTextConverter#nodesToText().");
                    return;
                }
                for (Object node : nodes) {
                    if (node instanceof WPATag) {
                        // extract wikilink annotations
                        WPATag tag = (WPATag) node;
                        String wikilinkLabel = (String) tag.getAttributes().get(
                                WIKILINK_TITLE_ATTR_KEY);
                        String wikilinkTarget = (String) tag.getAttributes().get(
                                WIKILINK_TARGET_ATTR_KEY);
                        if (wikilinkLabel != null) {
                            int colonIdx = wikilinkLabel.indexOf(':');
                            if (colonIdx == -1) {
                                // do not serialize non-topic wiki-links such as
                                // translation links missing from the
                                // INTERWIKI_LINK map
                                int start = countingBuffer.currentPosition;
                                tag.getBodyString(countingBuffer);
                                int end = countingBuffer.currentPosition;
                                wikilinks.add(new Annotation(start, end,
                                        wikilinkLabel, wikilinkTarget));
                            }
                        } else {
                            tag.getBodyString(countingBuffer);
                        }

                    } else if (node instanceof ContentToken) {
                        ContentToken contentToken = (ContentToken) node;
                        countingBuffer.append(contentToken.getContent());
                    } else if (node instanceof List) {
                    } else if (node instanceof WPList) {
                    } else if (node instanceof WPTable) {
                        // ignore lists and tables since they most of the time
                        // do not hold grammatically correct
                        // interesting sentences that are representative of the
                        // language.
                    } else if (node instanceof TagNode) {
                        TagNode tagNode = (TagNode) node;
                        Map<String, String> attributes = tagNode.getAttributes();
                        Map<String, Object> oAttributes = tagNode.getObjectAttributes();
                        boolean hasSpecialHandling = false;
                        String tagName = tagNode.getName();
                        // countingBuffer.append("<[" + tagName + "]>");
                        if ("a".equals(tagName)) {
                            String href = attributes.get(HREF_ATTR_KEY);
                            if (href != null
                                    && INTERWIKI_PATTERN.matcher(href).matches()) {
                                // ignore the interwiki links since they are
                                // mostly used for translation purpose.
                                hasSpecialHandling = true;
                            }
                        } else if ("ref".equals(tagName)) {
                            // ignore the references since they do not hold
                            // interesting text content
                            hasSpecialHandling = true;
                        } else if (oAttributes != null
                                && oAttributes.get(WIKIOBJECT_ATTR_KEY) instanceof ImageFormat) {
                            // the caption of images often hold well formed
                            // sentences with links to entites
                            hasSpecialHandling = true;
                            ImageFormat iformat = (ImageFormat) oAttributes.get(WIKIOBJECT_ATTR_KEY);
                            imageNodeToText(tagNode, iformat, countingBuffer,
                                    model);
                        }
                        if (!hasSpecialHandling) {
                            nodesToText(tagNode.getChildren(), countingBuffer,
                                    model);
                        }
                        if (TAGS_WITH_1_NEWLINE.contains(tagName)) {
                            countingBuffer.append("\n");
                        } else if (TAGS_WITH_2_NEWLINES.contains(tagName)) {
                            countingBuffer.append("\n\n");
                        }
                        // countingBuffer.append("<[/" + tagName + "]>");
                    }
                }
            } finally {
                model.decrementRecursionLevel();
            }
        }
    }

    public void imageNodeToText(TagNode tagNode, ImageFormat imageFormat,
            Appendable buffer, IWikiModel model) throws IOException {
        nodesToText(tagNode.getChildren(), buffer, model);
    }

    public boolean noLinks() {
        return true;
    }

    public List<Annotation> getWikiLinks() {
        return wikilinks;
    }

    public class CountingAppendable implements Appendable {

        public int currentPosition = 0;

        final protected Appendable wrappedBuffer;

        public CountingAppendable(Appendable wrappedBuffer) {
            this.wrappedBuffer = wrappedBuffer;
        }

        public Appendable append(CharSequence charSeq) throws IOException {
            currentPosition += charSeq.length();
            return wrappedBuffer.append(charSeq);
        }

        public Appendable append(char aChar) throws IOException {
            currentPosition += 1;
            return wrappedBuffer.append(aChar);
        }

        public Appendable append(CharSequence charSeq, int start, int end)
                throws IOException {
            currentPosition += end - start;
            return wrappedBuffer.append(charSeq, start, end);
        }

    }

}
