/**
 * Find the descendants and ancestors of topics up to level 3.
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

topic_parents = LOAD 'workspace/skos_categories_en.nt.bz2'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader',
    'http://dbpedia.org/resource/',
    'http://dbpedia.org/resource/')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);

-- Apparently it's not possible to do self joins, hence create a new alias
-- relation to be able to do a self join on the topic hierarchy in the
-- grouped_topics relation
topic_children = FOREACH topic_parents
  GENERATE broaderTopicUri, narrowerTopicUri;

topic_children_deduped = FILTER topic_children
  BY broaderTopicUri != narrowerTopicUri;

topic_children_distinct = DISTINCT topic_children_deduped;

linked_topics = LOAD 'workspace/linked_topics.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, narrowerTopicCount:long, broaderTopicCount: long);

topic_descendants_1_joined = JOIN linked_topics BY topicUri LEFT OUTER,
                                  topic_children_distinct BY broaderTopicUri;
topic_descendants_1_projected = FOREACH topic_descendants_1_joined GENERATE
  topicUri, primaryArticleUri, articleCount, narrowerTopicUri AS uriLevel1;
topic_descendants_1_distinct = DISTINCT topic_descendants_1_projected;
  
SPLIT topic_descendants_1_distinct INTO
  topic_descendants_1_filtered IF uriLevel1 IS NOT NULL,
  topic_no_descendants_1 IF uriLevel1 IS NULL;

topic_descendants_2_joined = JOIN topic_descendants_1_filtered BY uriLevel1 LEFT OUTER,
                                  topic_children_distinct BY broaderTopicUri;
topic_descendants_2_projected = FOREACH topic_descendants_2_joined GENERATE
  topicUri, primaryArticleUri, articleCount, uriLevel1, narrowerTopicUri AS uriLevel2;
topic_descendants_2_distinct = DISTINCT topic_descendants_2_projected;

topic_descendants_1_padded = FOREACH topic_no_descendants_1 GENERATE
  topicUri, primaryArticleUri, articleCount, uriLevel1, NULL AS uriLevel2;

topic_descendants = UNION
 topic_descendants_1_padded,
 topic_descendants_2_distinct;

STORE topic_descendants INTO 'workspace/topic_descendants.tsv';
