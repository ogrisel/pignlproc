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

article_templates = LOAD 'workspace/infobox_properties_en.nt.bz2'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://dbpedia.org/property/wikiPageUsesTemplate',
    'http://dbpedia.org/resource/',
    'http://dbpedia.org/resource/')
  AS (articleUri: chararray, templateUri: chararray);

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

joined_candidate_grounded_topics2 = JOIN
  joined_candidate_grounded_topics BY primaryArticleUri LEFT OUTER,
  article_templates BY articleUri;

projected_candidate_grounded_topics = FOREACH joined_candidate_grounded_topics2
  GENERATE
    topicUri, primaryArticleUri, articleCount,
    narrowerTopicCount, broaderTopicCount, templateUri;

-- Filter out Years categories which are not interesting
filtered_candidate_grounded_topics = FILTER projected_candidate_grounded_topics
  BY templateUri != 'Template:Yearbox';

ordered_candidate_grounded_topics = ORDER filtered_candidate_grounded_topics
  BY articleCount DESC, topicUri;

projected_ordered_candidate_grounded_topics =
  FOREACH ordered_candidate_grounded_topics
  GENERATE topicUri, primaryArticleUri, articleCount,
    narrowerTopicCount, broaderTopicCount;

distinct_candidate_grounded_topics =
  DISTINCT projected_ordered_candidate_grounded_topics;

SPLIT distinct_candidate_grounded_topics INTO
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