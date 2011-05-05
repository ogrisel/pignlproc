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

grounded_topics = LOAD 'workspace/grounded_topics.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, narrowerTopicCount:long, broaderTopicCount: long);

nongrounded_topics = LOAD 'workspace/nongrounded_topics.tsv'
  AS (topicUri: chararray, articleCount: long, narrowerTopicCount:long,
      broaderTopicCount: long);

-- Defined available sources to join
article_topics = LOAD 'workspace/article_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject', 'db:', 'db:')
  AS (articleUri: chararray, topicUri: chararray);

article_abstracts = LOAD 'workspace/long_abstracts_en.nt.gz'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract', 'db:')
  AS (articleUri: chararray, articleAbstract: chararray);
  
topic_parents = LOAD 'workspace/skos_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader', 'db:', 'db:')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);

-- Join the filtered topics with the article URI information by topicUri
grounded_topics_articles = JOIN
  grounded_topics BY topicUri,
  article_topics BY topicUri;
  
projected_grounded_topics_articles = FOREACH grounded_topics_articles
  GENERATE
    grounded_topics::topicUri AS topicUri,
    grounded_topics::topicCount AS topicCount,
    article_topics::articleUri AS articleUri;
  
nongrounded_topics_articles = JOIN
  nongrounded_topics BY topicUri,
  article_topics BY topicUri;
  
-- Join the nongrounded children to aggregate their articles
children_of_grounded_topics = JOIN 
  grounded_topics BY topicUri,
  topic_parents BY broaderTopicUri;
  
nongrounded_children_topics = JOIN 
  children_of_grounded_topics BY topic_parents::narrowerTopicUri,
  nongrounded_topics BY topicUri;

projected_nongrounded_children_topics =
  FOREACH nongrounded_children_topics
  GENERATE
    nongrounded_children_topics::grounded_topics::topicUri AS topicUri,
    nongrounded_children_topics::grounded_topics::topicCount AS topicCount,
    nongrounded_topics::topicUri AS nongroundedChildTopicUri;

nongrounded_children_topics_articles = JOIN
  projected_nongrounded_children_topics BY nongroundedChildTopicUri,
  article_topics BY topicUri;

projected_nongrounded_children_topics_articles =
  FOREACH nongrounded_children_topics_articles
  GENERATE
    projected_nongrounded_children_topics::topicUri AS topicUri,
    projected_nongrounded_children_topics::topicCount AS topicCount,
    article_topics::articleUri AS articleUri;
    
compound_topics_articles = UNION
  projected_grounded_topics_articles,
  projected_nongrounded_children_topics_articles;

-- Join with the abstract by articleUri
joined_topics_abstracts = JOIN
  compound_topics_articles BY articleUri,
  article_abstracts BY articleUri;

topics_abstracts = FOREACH joined_topics_abstracts
  GENERATE
   compound_topics_articles::filtered_topics::topicUri AS topicUri,
   compound_topics_articles::article_topics::articleUri AS articleUri,
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
