/**
 * Parse wikipedia and extract lines with links to other wikipedia articles
 * along with position information to build NER training corpus.
 *
 * The schema of the result tuples is:
 * (title: chararray, sentenceOrder: int, linkTarget: chararray,
 *  linkBegin: int, linkEnd: int, sentence: chararray)
 */


-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

parsed = LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, wikiuri, text, redirect, links, headers, paragraphs);

-- filter and project as early as possible
noredirect = FILTER parsed by redirect IS NULL;
projected = FOREACH noredirect GENERATE title, text, links, paragraphs;

-- Extract the sentence contexts of the links respecting the paragraph
-- boundaries
sentences = FOREACH projected
  GENERATE title, flatten(pignlproc.evaluation.SentencesWithLink(
    text, links, paragraphs));

stored = FOREACH sentences
  GENERATE title, sentenceOrder, linkTarget, linkBegin, linkEnd, sentence;

-- Ensure ordering for fast merge with type info later
ordered = ORDER stored BY linkTarget ASC, title ASC, sentenceOrder ASC;
STORE ordered INTO '$OUTPUT/$LANG/sentences_with_links';
