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

-- Join with the abstract by articleUri
joined_topics_abstracts = JOIN
  grounded_topics_articles BY articleUri,
  article_abstracts BY articleUri;

topics_abstracts = FOREACH joined_topics_abstracts
  GENERATE
   grounded_topics_articles::topicUri AS topicUri,
   grounded_topics_articles::articleUri AS articleUri,
   article_abstracts::articleAbstract AS articleAbstract;

grouped_topics2 = GROUP
  topics_abstracts BY topicUri,
  grounded_topic_ancestry BY topicUri;

bagged_abstracts = FOREACH grouped_topics2
  GENERATE
    group AS topicUri,
    COUNT(topics_abstracts.articleUri) AS abstractCount,
    AggregateTextBag(topics_abstracts.articleAbstract) AS aggregateTopicAbstract,
    COUNT(grounded_topic_ancestry.groundedPath) AS pathsCount,
    JoinPaths(grounded_topic_ancestry.groundedPath) AS paths;

-- filter again after abstract joins in case of missing abstract
-- because we do not resolve redirect yet
filtered_topics2 = FILTER bagged_abstracts BY abstractCount > 10 AND pathsCount != 0;

ordered_topics = ORDER filtered_topics2 BY abstractCount DESC, topicUri ASC;

-- TSV export suitable for direct Solr indexing
tsv_topics_abstracts = FOREACH ordered_topics
  GENERATE
    topicUri, abstractCount, paths,
    SafeTsvText(aggregateTopicAbstract) AS primaryArticleAbstract;

STORE tsv_topics_abstracts
  INTO 'workspace/topics_abstracts.tsv';

-- NTriples export suitable for Stanbol EntityHub import
ntriples_topics_abstracts = FOREACH ordered_topics
  GENERATE topicUri, aggregateTopicAbstract;

STORE ntriples_topics_abstracts
  INTO 'workspace/topics_abstracts.nt'
  USING NTriplesAbstractsStorage;
