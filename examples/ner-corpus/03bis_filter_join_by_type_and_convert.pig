/**
 * Join the wikipedia page URI to the type of the linked entity.
 */

SET default_parallel 20

REGISTER $PIGNLPROC_JAR
-- use the default OpenNLP tokenizer (should work for most european languages)
DEFINE merge pignlproc.evaluation.MergeAsOpenNLPAnnotatedText('$TYPE_NAME');

sentences = LOAD '$INPUT/$LANG/sentences_with_links'
  AS (title: chararray, sentenceOrder: int, linkTarget: chararray,
      linkBegin: int, linkEnd: int, sentence: chararray);

wikiuri_types = LOAD '$INPUT/$LANG/wikiuri_to_types'
  AS (wikiuri: chararray, type: chararray);

filtered_types = FILTER wikiuri_types BY type == '$TYPE_URI';

-- Perform successive joins to find the type of the linkTarget assuming
-- both bags are previously ordered by wikiuri / linkTarget
joined =
  JOIN filtered_types
  BY wikiuri, sentences BY linkTarget; -- USING "merge";

result =
  FOREACH joined
  GENERATE title, sentenceOrder, linkBegin, linkEnd, sentence;

-- Reorder and group by article title and sentence order
ordered = ORDER result BY title ASC, sentenceOrder ASC;

grouped = GROUP ordered BY (title, sentenceOrder);

-- Convert to the OpenNLP training format

opennlp_corpus =
 FOREACH grouped
 GENERATE merge(ordered.sentence, ordered.linkBegin, ordered.linkEnd);

STORE opennlp_corpus INTO '$OUTPUT/$LANG/opennlp_$TYPE_NAME';
