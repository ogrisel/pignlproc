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

topic_parents = LOAD 'workspace/skos_categories_en.nt.bz2'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader',
    'http://dbpedia.org/resource/',
    'http://dbpedia.org/resource/')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);
  
article_abstracts = LOAD 'workspace/long_abstracts_en.nt.bz2'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract',
    'http://dbpedia.org/resource/')
  AS (articleUri: chararray, articleAbstract: chararray);

-- Build are candidate matching article URI by removing the 'Category:'
-- part of the topic URI
candidate_grounded_topics = FOREACH topic_counts GENERATE
  topicUri, REPLACE(topicUri, 'Category:', '') AS candidatePrimaryArticleUri,
  articleCount, narrowerTopicCount, broaderTopicCount;

-- Join on article abstracts to identify grounded topics
-- (topics that have a matching article with an abstract)
joined_candidate_grounded_topics = JOIN
  candidate_grounded_topics BY candidatePrimaryArticleUri LEFT OUTER,
  article_abstracts BY articleUri;

projected_candidate_grounded_topics = FOREACH joined_candidate_grounded_topics
  GENERATE
    candidate_grounded_topics::topicUri AS topicUri,
    (article_abstracts::articleAbstract IS NOT NULL ?
      article_abstracts::articleUri : NULL) AS primaryArticleUri,
    candidate_grounded_topics::articleCount AS articleCount,
    candidate_grounded_topics::narrowerTopicCount AS narrowerTopicCount,
    candidate_grounded_topics::broaderTopicCount AS broaderTopicCount;

distinct_candidate_grounded_topics =
  DISTINCT projected_candidate_grounded_topics;

ordered_candidate_grounded_topics = ORDER distinct_candidate_grounded_topics
  BY articleCount DESC, topicUri;

SPLIT ordered_candidate_grounded_topics INTO
   grounded_topics IF primaryArticleUri IS NOT NULL,
   nongrounded_topics IF primaryArticleUri IS NULL;

projected_nongrounded_topics = FOREACH nongrounded_topics
  GENERATE topicUri, articleCount, narrowerTopicCount, broaderTopicCount;

-- all topics, grounded and non grounded (primaryArticleUri can be NULL)
STORE distinct_candidate_grounded_topics INTO 'workspace/linked_topics.tsv';

-- only grounded topics (primaryArticleUri is not NULL)
STORE grounded_topics INTO 'workspace/grounded_topics.tsv';

-- only non-grounded topics (hence no primaryArticleUri either)
STORE projected_nongrounded_topics INTO 'workspace/nongrounded_topics.tsv';