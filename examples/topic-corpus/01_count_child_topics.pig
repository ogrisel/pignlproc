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

topic_parents = LOAD 'workspace/skos_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader', 'db:', 'db:')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);

-- Apparently it's not possible to do self joins, hence create a new alias
-- relation to be able to do a self join on the topic hierarchy in the
-- grouped_topics relation
topic_children = FOREACH topic_parents
  GENERATE broaderTopicUri, narrowerTopicUri;

-- Count the number of articles categorized for each topic
grouped_topics = COGROUP
  article_topics BY topicUri,
  topic_parents BY narrowerTopicUri,
  topic_children BY broaderTopicUri;

counted_topics = FOREACH grouped_topics
  GENERATE
    group AS topicUri,
    COUNT(article_topics.articleUri) AS articleCount,
    COUNT(topic_children.narrowerTopicUri) AS narrowerTopicCount,
    COUNT(topic_parents.broaderTopicUri) AS broaderTopicCount;

-- Filter topics that are not part of any taxonomic tree as they
-- are usually not that interesting (1983_births, Living_people, ...)
filtered_topics = FILTER counted_topics
  BY narrowerTopicCount != 0 OR broaderTopicCount != 0;

ordered_topics = ORDER filtered_topics BY articleCount DESC, topicUri ASC;

STORE ordered_topics INTO 'workspace/topics_counts.tsv';
