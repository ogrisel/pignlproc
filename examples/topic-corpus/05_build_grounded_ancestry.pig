/**
 * Build the ancestry path of topics up to grounded root topics
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

topic_parents = LOAD 'workspace/skos_categories_en.nt.bz2'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/2004/02/skos/core#broader',
    'http://dbpedia.org/resource/',
    'http://dbpedia.org/resource/')
  AS (narrowerTopicUri: chararray, broaderTopicUri: chararray);

linked_topics = LOAD 'workspace/linked_topics.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, narrowerTopicCount:long, broaderTopicCount: long);

-- Apparently it's not possible to do self joins, hence create a new alias
-- relation to be able to do a self join on the topic hierarchy in the
-- grouped_topics relation
topic_children = FOREACH topic_parents
  GENERATE broaderTopicUri, narrowerTopicUri;

topic_children_deduped = FILTER topic_children
  BY broaderTopicUri != narrowerTopicUri;
  
topic_children_distinct = DISTINCT topic_children_deduped;

topic_children_joined = JOIN
  topic_children_distinct BY narrowerTopicUri,
  linked_topics BY topicUri;
  
topic_children_with_info = FOREACH topic_children_joined GENERATE
  topicUri AS topicUri, primaryArticleUri AS primaryArticleUri,
  articleCount AS articleCount, broaderTopicUri AS broaderTopicUri;

topic_children_with_info_distinct = DISTINCT topic_children_with_info;

-- interesting roots
roots = FILTER topic_children_with_info_distinct
  BY broaderTopicUri == 'Category:Main_topic_classifications';

grounded_ancestry_1 = FOREACH roots GENERATE
  topicUri, topicUri AS rootUri,
  primaryArticleUri AS primaryArticleUri,
  articleCount AS articleCount,
  (primaryArticleUri IS NULL ? '' : topicUri) AS groundedPath,
  (primaryArticleUri IS NULL ? 0 : 1) AS groundedPathLength;

joined_children_2 = JOIN
  grounded_ancestry_1 BY topicUri,  
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_2 = FOREACH joined_children_2 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  grounded_ancestry_1::rootUri AS rootUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

joined_children_3 = JOIN
  grounded_ancestry_2 BY topicUri,  
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_3 = FOREACH joined_children_3 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  grounded_ancestry_2::rootUri AS rootUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;
  
joined_children_4 = JOIN
  grounded_ancestry_3 BY topicUri,  
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_4 = FOREACH joined_children_4 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  grounded_ancestry_3::rootUri AS rootUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

joined_children_5 = JOIN
  grounded_ancestry_4 BY topicUri,  
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_5 = FOREACH joined_children_5 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  grounded_ancestry_4::rootUri AS rootUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

joined_children_6 = JOIN
  grounded_ancestry_5 BY topicUri,  
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_6 = FOREACH joined_children_6 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  grounded_ancestry_5::rootUri AS rootUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

--joined_children_7 = JOIN
--  grounded_ancestry_6 BY topicUri,  
--  topic_children_with_info BY broaderTopicUri;
--
--grounded_ancestry_7 = FOREACH joined_children_7 GENERATE
--  topic_children_with_info::topicUri AS topicUri,
--  grounded_ancestry_6::rootUri AS rootUri,
--  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
--  topic_children_with_info::articleCount AS articleCount,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPath :
--     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPathLength :
--     groundedPathLength + 1) AS groundedPathLength;
--
--joined_children_8 = JOIN
--  grounded_ancestry_7 BY topicUri,  
--  topic_children_with_info BY broaderTopicUri;
--
--grounded_ancestry_8 = FOREACH joined_children_8 GENERATE
--  topic_children_with_info::topicUri AS topicUri,
--  grounded_ancestry_7::rootUri AS rootUri,
--  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
--  topic_children_with_info::articleCount AS articleCount,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPath :
--     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPathLength :
--     groundedPathLength + 1) AS groundedPathLength;
--
--joined_children_9 = JOIN
--  grounded_ancestry_8 BY topicUri,  
--  topic_children_with_info BY broaderTopicUri;
--
--grounded_ancestry_9 = FOREACH joined_children_9 GENERATE
--  topic_children_with_info::topicUri AS topicUri,
--  grounded_ancestry_8::rootUri AS rootUri,
--  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
--  topic_children_with_info::articleCount AS articleCount,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPath :
--     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPathLength :
--     groundedPathLength + 1) AS groundedPathLength;
--
--joined_children_10 = JOIN
--  grounded_ancestry_9 BY topicUri,  
--  topic_children_with_info BY broaderTopicUri;
--
--grounded_ancestry_10 = FOREACH joined_children_10 GENERATE
--  topic_children_with_info::topicUri AS topicUri,
--  grounded_ancestry_9::rootUri AS rootUri,
--  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
--  topic_children_with_info::articleCount AS articleCount,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPath :
--     CONCAT(CONCAT(groundedPath, ' '), topic_children_with_info::topicUri)) AS groundedPath,
--  (topic_children_with_info::primaryArticleUri IS NULL ?
--     groundedPathLength :
--     groundedPathLength + 1) AS groundedPathLength;

grounded_ancestry = UNION
  grounded_ancestry_1,
  grounded_ancestry_2,
  grounded_ancestry_3,
  grounded_ancestry_4,
  grounded_ancestry_5,
  grounded_ancestry_6;
--  grounded_ancestry_7,
--  grounded_ancestry_8,
--  grounded_ancestry_9,
--  grounded_ancestry_10;

ordered_grounded_ancestry = ORDER grounded_ancestry BY articleCount DESC, topicUri;

STORE ordered_grounded_ancestry INTO 'workspace/grounded_ancestry.tsv';