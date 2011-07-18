/**
 * Build the ancestry path of topics up to grounded root topics
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar
DEFINE NoLoopInPath pignlproc.evaluation.NoLoopInPath('/');

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

-- TODO: rewrite the following using a python script + loop when pig 0.9 is out

grounded_ancestry_1 = FOREACH roots GENERATE
  topicUri,
  primaryArticleUri AS primaryArticleUri,
  articleCount AS articleCount,
  topicUri AS fullPath,
  (primaryArticleUri IS NULL ? '' : topicUri) AS groundedPath,
  (primaryArticleUri IS NULL ? 0 : 1) AS groundedPathLength;

grounded_ancestry_1_noloop = FILTER grounded_ancestry_1 BY NoLoopInPath(fullPath);

joined_children_2 = JOIN
  grounded_ancestry_1_noloop BY topicUri,
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_2 = FOREACH joined_children_2 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  CONCAT(CONCAT(fullPath, '/'), topic_children_with_info::topicUri) AS fullPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, '/'), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

grounded_ancestry_2_noloop = FILTER grounded_ancestry_2 BY NoLoopInPath(fullPath);

joined_children_3 = JOIN
  grounded_ancestry_2_noloop BY topicUri,
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_3 = FOREACH joined_children_3 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  CONCAT(CONCAT(fullPath, '/'), topic_children_with_info::topicUri) AS fullPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, '/'), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

grounded_ancestry_3_noloop = FILTER grounded_ancestry_3 BY NoLoopInPath(fullPath);

joined_children_4 = JOIN
  grounded_ancestry_3_noloop BY topicUri,
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_4 = FOREACH joined_children_4 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  CONCAT(CONCAT(fullPath, '/'), topic_children_with_info::topicUri) AS fullPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, '/'), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

grounded_ancestry_4_noloop = FILTER grounded_ancestry_4 BY NoLoopInPath(fullPath);

joined_children_5 = JOIN
  grounded_ancestry_4_noloop BY topicUri,
  topic_children_with_info BY broaderTopicUri;

grounded_ancestry_5 = FOREACH joined_children_5 GENERATE
  topic_children_with_info::topicUri AS topicUri,
  topic_children_with_info::primaryArticleUri AS primaryArticleUri,
  topic_children_with_info::articleCount AS articleCount,
  CONCAT(CONCAT(fullPath, '/'), topic_children_with_info::topicUri) AS fullPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPath :
     CONCAT(CONCAT(groundedPath, '/'), topic_children_with_info::topicUri)) AS groundedPath,
  (topic_children_with_info::primaryArticleUri IS NULL ?
     groundedPathLength :
     groundedPathLength + 1) AS groundedPathLength;

grounded_ancestry_5_noloop = FILTER grounded_ancestry_5 BY NoLoopInPath(fullPath);

grounded_ancestry = UNION
  grounded_ancestry_1_noloop,
  grounded_ancestry_2_noloop,
  grounded_ancestry_3_noloop,
  grounded_ancestry_4_noloop,
  grounded_ancestry_5_noloop;

ordered_grounded_ancestry = ORDER grounded_ancestry BY articleCount DESC, topicUri;

STORE ordered_grounded_ancestry INTO 'workspace/grounded_ancestry.tsv';
