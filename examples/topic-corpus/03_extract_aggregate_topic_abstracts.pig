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

topic_counts = LOAD 'workspace/topics_counts.tsv'
  AS (topicUri: chararray, articleCount: long, narrowerTopicCount:long,
      broaderTopicCount);

-- Defined available sources to join
article_topics = LOAD 'workspace/article_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject', 'db:', 'db:')
  AS (articleUri: chararray, topicUri: chararray);

article_abstracts = LOAD 'workspace/long_abstracts_en.nt.gz'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract', 'db:')
  AS (articleUri: chararray, articleAbstract: chararray);

-- Only keep the topics of the most popular articles while also
-- filtering out overly broad topics such as "living people"
filtered_topics =
  FILTER topic_counts
    BY articleCount > 50
       AND articleCount < 30000
       AND NOT topicUri MATCHES '.*_births'
       AND NOT topicUri MATCHES '.*_deaths';

-- Join the filtered topics with the article URI information by topicUri
filtered_topics_articles = JOIN
  filtered_topics BY topicUri,
  article_topics BY topicUri;

-- Join with the abstract by articleUri
joined_topics_abstracts = JOIN
  filtered_topics_articles BY articleUri,
  article_abstracts BY articleUri;

topics_abstracts = FOREACH joined_topics_abstracts
  GENERATE
   filtered_topics_articles::filtered_topics::topicUri AS topicUri,
   filtered_topics_articles::article_topics::articleUri AS articleUri,
   article_abstracts::articleAbstract AS articleAbstract;

grouped_topics2 = GROUP topics_abstracts BY topicUri;

bagged_abstracts = FOREACH grouped_topics2
  GENERATE
    group AS topicUri,
    COUNT(topics_abstracts.articleUri) AS abstractCount,
    aggregate(topics_abstracts.articleAbstract) AS aggregateTopicAbstract;

-- filter again after abstract joins in case of missing abstract
-- because we do not resolve redirect yet
filtered_topics2 = FILTER bagged_abstracts BY abstractCount > 10;

ordered_topics = ORDER filtered_topics2 BY abstractCount DESC, topicUri ASC;

STORE ordered_topics INTO 'workspace/topics_abstracts.tsv';
