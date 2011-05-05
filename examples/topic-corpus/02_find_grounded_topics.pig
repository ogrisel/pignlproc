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
candidate_ground_topics = FOREACH topic_counts GENERATE
  topicUri, REPLACE(topicUri, 'Category:', '') AS primaryArticleUri,
  articleCount, narrowerTopicCount, broaderTopicCount;

-- Join on article abstracts to identify ground topics
joined_candidate_ground_topics = JOIN
  candidate_ground_topics BY primaryArticleUri LEFT OUTER,
  article_abstracts BY articleUri;

projected_candidate_ground_topics = FOREACH joined_candidate_ground_topics
  GENERATE
    candidate_ground_topics::topicUri AS topicUri,
    article_abstracts::articleUri AS primaryArticleUri,
    candidate_ground_topics::articleCount AS articleCount,
    candidate_ground_topics::narrowerTopicCount AS narrowerTopicCount,
    candidate_ground_topics::broaderTopicCount AS broaderTopicCount;

SPLIT projected_candidate_ground_topics INTO
   ground_topics IF primaryArticleUri IS NOT NULL,
   nonground_topics IF primaryArticleUri IS NULL;
   
ordered_ground_topics = ORDER ground_topics
  BY articleCount DESC, topicUri;
   
projected_nonground_topics = FOREACH nonground_topics
  GENERATE topicUri, articleCount, narrowerTopicCount, broaderTopicCount;
  
ordered_nonground_topics = ORDER projected_nonground_topics
  BY articleCount DESC, topicUri;

STORE ordered_ground_topics INTO 'workspace/group_topics.tsv';
STORE ordered_nonground_topics INTO 'workspace/nonground_topics.tsv';