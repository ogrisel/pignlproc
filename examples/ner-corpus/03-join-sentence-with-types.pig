/**
 * Join the wikipedia page URI to the type of the linked entity.
 */

REGISTER $PIGNLPROC_JAR

sentences = LOAD '$INPUT/sentences_with_links'
  AS (title: chararray, sentenceOrder: int, linkTarget: chararray,
      linkBegin: int, linkEnd: int, sentence: chararray);

wikiuri_types = LOAD '$INPUT/wikiuri_to_types'
  AS (wikiuri: chararray, type: chararray);

-- Perform successive joins to find the type of the linkTarget assuming
-- both bags are previously ordered by wikiuri / linkTarget
joined = JOIN wikiuri_types BY wikiuri, sentences BY linkTarget USING "merge";

result = FOREACH joined GENERATE type, linkTarget, title, sentenceOrder,
  linkBegin, linkEnd, sentence;

-- Reorder by type and sentence order
ordered = ORDER result BY type ASC, title ASC, sentenceOrder ASC;

-- TODO: group links in same sentence together and use OpenNLP syntax
STORE ordered INTO '$OUTPUT/sentences_with_types';
