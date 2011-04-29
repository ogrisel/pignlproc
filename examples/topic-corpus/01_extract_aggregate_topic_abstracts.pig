/**
 * Join the top skos topics with article abstracts and order
 * by most popular topics.
 *
 * The schema of the result tuples is:
 * (topicUri: chararray, topicCount: int, articleUri: chararray,
 *  articleAbstract: chararray)
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar
DEFINE aggregate pignlproc.evaluation.AggregateTextBag();

-- Defined available sources to join
article_topics = LOAD 'workspace/article_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject', 'db:', 'db:')
  AS (articleUri: chararray, topicUri: chararray);

article_abstracts = LOAD 'workspace/long_abstracts_en.nt.gz'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract', 'db:')
  AS (articleUri: chararray, articleAbstract: chararray);

-- Count the number of articles categorized for each topic
grouped_topics = GROUP article_topics BY topicUri;

counted_topics = FOREACH grouped_topics
  GENERATE
    group AS topicUri,
    article_topics.articleUri AS bagOfarticleUris,
    COUNT(article_topics.articleUri) AS topicCount;

-- Only keep the topics of the most popular articles while also
-- filtering out overly broad topics such as living people
filtered_topics =
  FILTER counted_topics
    BY topicCount > 10 AND topicCount < 10000;

-- Flatten the list of articles for each topic so as to be able
-- to join with the abstracts
flattened_topics = FOREACH filtered_topics
  GENERATE topicUri, topicCount,
    flatten(bagOfarticleUris) AS articleUri;

joined_topics_abstracts = JOIN
  flattened_topics BY articleUri,
  article_abstracts BY articleUri;

topics_abstracts = FOREACH joined_topics_abstracts
  GENERATE
   flattened_topics::topicUri AS topicUri,
   flattened_topics::topicCount AS topicCount,
   flattened_topics::articleUri AS articleUri,
   article_abstracts::articleAbstract AS articleAbstract;

grouped_topics2 = GROUP topics_abstracts BY topicUri;

bagged_abstracts = FOREACH grouped_topics2
  GENERATE
    group AS topicUri,
    COUNT(topics_abstracts.articleUri) AS topicCount,
    aggregate(topics_abstracts.articleAbstract) AS aggregateTopicAbstract;

ordered_topics = ORDER bagged_abstracts BY topicCount DESC, topicUri ASC;

STORE ordered_topics INTO 'workspace/topics.tsv';
