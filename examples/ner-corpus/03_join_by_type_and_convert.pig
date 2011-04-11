/**
 * Join the wikipedia page URI to the type of the linked entity.
 */

SET default_parallel 40

REGISTER $PIGNLPROC_JAR
-- use the default OpenNLP tokenizer (should work for most european languages)
DEFINE opennlp_merge pignlproc.evaluation.MergeAsOpenNLPAnnotatedText();

sentences = LOAD '$INPUT/$LANG/sentences_with_links'
  AS (title: chararray, sentenceOrder: int, linkTarget: chararray,
      linkBegin: int, linkEnd: int, sentence: chararray);

wikiuri_types = LOAD '$INPUT/$LANG/wikiuri_to_types'
  AS (wikiuri: chararray, typeuri: chararray);

-- load the type mapping from DBpedia type uri to opennlp type name
type_names = LOAD '$TYPE_NAMES' AS (typeuri: chararray, typename: chararray);

-- Perform successive joins to find the opennlp typename of the linkTarget
joined = JOIN wikiuri_types BY typeuri, type_names BY typeuri USING 'replicated';
joined_projected = FOREACH joined GENERATE wikiuri, typename;
joined2 = JOIN joined_projected BY wikiuri, sentences BY linkTarget;

result = FOREACH joined2
  GENERATE title, sentenceOrder, typename, linkBegin, linkEnd, sentence;

-- Reorder and group by article title and sentence order
ordered = ORDER result BY title ASC, sentenceOrder ASC;
grouped = GROUP ordered BY (title, sentenceOrder);

-- Convert to the OpenNLP training format
opennlp_corpus =
 FOREACH grouped
 GENERATE opennlp_merge(
   ordered.sentence, ordered.linkBegin, ordered.linkEnd, ordered.typename);

STORE opennlp_corpus INTO '$OUTPUT/$LANG/opennlp';
