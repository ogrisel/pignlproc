/**
 * Join the top skos topics with categorized article abstracts
 * of close descendants of grounded topics (computed in previous step).
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar
DEFINE AggregateTextBag pignlproc.evaluation.AggregateTextBag();
DEFINE JoinPaths pignlproc.evaluation.ConcatTextBag(' ');
DEFINE SafeTsvText pignlproc.evaluation.SafeTsvText();
DEFINE CheckAbstract pignlproc.evaluation.CheckAbstract();
DEFINE NTriplesAbstractsStorage pignlproc.storage.UriStringLiteralNTriplesStorer(
  'http://pignlproc.org/merged-abstracts', 'http://dbpedia.org/resource/', 'en');

-- Defined available sources to join

article_abstracts = LOAD 'workspace/long_abstracts_en.nt.bz2'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract',
    'http://dbpedia.org/resource/')
  AS (articleUri: chararray, articleAbstract: chararray);

grounded_topics_articles = LOAD 'workspace/grounded_topics_articles.tsv'
  AS (topicUri: chararray, articleCount: long, articleUri: chararray);

topic_ancestry = LOAD 'workspace/grounded_ancestry.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, fullPath: chararray,
      groundedPath: chararray, groundedPathLength: long);

grounded_topic_ancestry = FILTER topic_ancestry
  BY primaryArticleUri IS NOT NULL;

grouped_topic_ancestry = GROUP grounded_topic_ancestry BY topicUri;

topic_paths = FOREACH grouped_topic_ancestry
  GENERATE
    group AS topicUri,
    JoinPaths(grounded_topic_ancestry.groundedPath) AS paths;

-- gather the list of articles that match the subset of topic with path
joined_topic_articles = JOIN
  topic_paths BY topicUri,
  grounded_topics_articles BY topicUri;

grounded_topics_articles_with_paths = FOREACH joined_topic_articles
  GENERATE topic_paths::topicUri AS topicUri, articleUri, paths;

-- filter out abstracts that are boring
filtered_article_abstracts = FILTER article_abstracts
  BY CheckAbstract(articleAbstract);

-- Join with the abstracts by articleUri
joined_topics_abstracts = JOIN
  grounded_topics_articles_with_paths BY articleUri,
  filtered_article_abstracts BY articleUri;

topics_abstracts = FOREACH joined_topics_abstracts
  GENERATE
   grounded_topics_articles_with_paths::topicUri AS topicUri,
   grounded_topics_articles_with_paths::paths AS paths,
   article_abstracts::articleAbstract AS articleAbstract;

grouped_topics2 = GROUP topics_abstracts BY (topicUri, paths);

bagged_abstracts = FOREACH grouped_topics2
  GENERATE
    group.$0 AS topicUri,
    COUNT(topics_abstracts.articleUri) AS abstractCount,
	group.$1 AS paths,
    AggregateTextBag(topics_abstracts.articleAbstract) AS aggregateTopicAbstract;

-- filter again after abstract joins in case of missing abstract
-- because we do not resolve redirect yet
filtered_topics2 = FILTER bagged_abstracts BY abstractCount > 10;

ordered_topics = ORDER filtered_topics2 BY abstractCount DESC, topicUri ASC;

-- TSV export suitable for direct Solr indexing
tsv_topics_abstracts = FOREACH ordered_topics
  GENERATE
    topicUri, abstractCount, paths,
    SafeTsvText(aggregateTopicAbstract) AS aggregateTopicAbstract;

STORE tsv_topics_abstracts
  INTO 'workspace/topics_abstracts.tsv';

-- NTriples export suitable for Stanbol EntityHub import
ntriples_topics_abstracts = FOREACH ordered_topics
  GENERATE topicUri, aggregateTopicAbstract;

STORE ntriples_topics_abstracts
  INTO 'workspace/topics_abstracts.nt'
  USING NTriplesAbstractsStorage;
