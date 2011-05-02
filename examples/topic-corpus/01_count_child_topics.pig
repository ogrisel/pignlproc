/**
 * Count the topics in articles occurrences and including
 * occurrences of descendants topics
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

-- Defined available sources to join
article_topics = LOAD 'workspace/article_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject', 'db:', 'db:')
  AS (articleUri: chararray, topicUri: chararray);

topic_hierarchy = LOAD 'workspace/skos_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader', 'db:', 'db:')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);

-- Count the number of articles categorized for each topic
grouped_topics = COGROUP
  article_topics BY topicUri,
  topic_hierarchy BY broaderTopicUri;

counted_topics = FOREACH grouped_topics
  GENERATE
    group AS topicUri,
    COUNT(article_topics.articleUri) AS articleCount,
    COUNT(topic_hierarchy.narrowerTopicUri) AS childTopicCount;

ordered_topics = ORDER counted_topics
   BY childTopicCount ASC, articleCount DESC, topicUri ASC;

STORE ordered_topics INTO 'workspace/topics_counts.tsv';
