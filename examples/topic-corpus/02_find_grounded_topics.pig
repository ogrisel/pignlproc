/**
 * This scripts is used to identify the categories that have matching
 * dbpedia resource (with label, abstract...). We call such categories
 * "grounded topics".
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

topic_counts = LOAD 'workspace/topics_counts.tsv'
  AS (topicUri: chararray, articleCount: long, narrowerTopicCount:long,
      broaderTopicCount);

topic_parents = LOAD 'workspace/skos_categories_en.nt.gz'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader', 'db:', 'db:')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);
  
article_abstracts = LOAD 'workspace/long_abstracts_en.nt.gz'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract', 'db:')
  AS (articleUri: chararray, articleAbstract: chararray);

-- Build are candidate matching article URI by removing the 'Category:'
-- part of the topic URI
candidate_grounded_topics = FOREACH topic_counts GENERATE
  topicUri, REPLACE(topicUri, 'Category:', '') AS primaryArticleUri,
  articleCount, narrowerTopicCount, broaderTopicCount;

-- Join on article abstracts to identify grounded topics
-- (topics that have a matching article with an abstract)
joined_candidate_grounded_topics = JOIN
  candidate_grounded_topics BY primaryArticleUri LEFT OUTER,
  article_abstracts BY articleUri;

projected_candidate_grounded_topics = FOREACH joined_candidate_grounded_topics
  GENERATE
    candidate_grounded_topics::topicUri AS topicUri,
    article_abstracts::articleUri AS primaryArticleUri,
    candidate_grounded_topics::articleCount AS articleCount,
    candidate_grounded_topics::narrowerTopicCount AS narrowerTopicCount,
    candidate_grounded_topics::broaderTopicCount AS broaderTopicCount;

SPLIT projected_candidate_grounded_topics INTO
   grounded_topics IF primaryArticleUri IS NOT NULL,
   nongrounded_topics IF primaryArticleUri IS NULL;
   
ordered_grounded_topics = ORDER grounded_topics
  BY articleCount DESC, topicUri;
   
projected_nongrounded_topics = FOREACH nongrounded_topics
  GENERATE topicUri, articleCount, narrowerTopicCount, broaderTopicCount;
  
ordered_nongrounded_topics = ORDER projected_nongrounded_topics
  BY articleCount DESC, topicUri;

STORE ordered_grounded_topics INTO 'workspace/grounded_topics.tsv';
STORE ordered_nongrounded_topics INTO 'workspace/nongrounded_topics.tsv';