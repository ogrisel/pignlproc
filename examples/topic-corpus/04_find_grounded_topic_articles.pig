/**
 * Find all the articles uri of topics that are descendants up
 * to level 3 of grounded topics.
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

topic_descendants = LOAD 'workspace/topic_descendants.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, uriLevel1: chararray,
      uriLevel2: chararray);

article_topics = LOAD 'workspace/article_categories_en.nt.bz2'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject',
    'http://dbpedia.org/resource/',
    'http://dbpedia.org/resource/')
  AS (articleUri: chararray, topicUri: chararray);

-- Perform the joins on the grounded topics only

grounded_topics_descendants = FILTER topic_descendants
  BY primaryArticleUri IS NOT NULL;

-- Do not forget the main article
-- TODO: optim: reuse the grounded_topic table directly

grounded_topics_primary_articles = FOREACH grounded_topics_descendants
  GENERATE topicUri, articleCount, primaryArticleUri AS articleUri;

grounded_topics_primary_articles_distinct = DISTINCT grounded_topics_primary_articles;

-- Follow the descendants
   
grounded_topics_descendants_1_filtered = FILTER topic_descendants
  BY uriLevel1 IS NOT NULL;
grounded_topics_descendants_2_filtered = FILTER topic_descendants
  BY uriLevel2 IS NOT NULL;

grounded_topics_articles_0_joined = JOIN
  grounded_topics_descendants BY topicUri,
  article_topics BY topicUri;
grounded_topics_articles_0_projected = FOREACH grounded_topics_articles_0_joined
  GENERATE grounded_topics_descendants::topicUri AS topicUri,
    articleCount, articleUri;
  
grounded_topics_articles_1_joined = JOIN
  grounded_topics_descendants_1_filtered BY uriLevel1,
  article_topics BY topicUri;
grounded_topics_articles_1_projected = FOREACH grounded_topics_articles_1_joined
  GENERATE grounded_topics_descendants_1_filtered::topicUri AS topicUri,
    articleCount, articleUri;

grounded_topics_articles_2_joined = JOIN
  grounded_topics_descendants_2_filtered BY uriLevel2,
  article_topics BY topicUri;
grounded_topics_articles_2_projected = FOREACH grounded_topics_articles_2_joined
  GENERATE grounded_topics_descendants_2_filtered::topicUri AS topicUri,
    articleCount, articleUri;

grounded_topics_articles = UNION
  grounded_topics_primary_articles_distinct,
  grounded_topics_articles_0_projected,
  grounded_topics_articles_1_projected;
  --grounded_topics_articles_2_projected;

grounded_topics_articles_distinct = DISTINCT grounded_topics_articles;

grounded_topics_articles_ordered =  ORDER grounded_topics_articles_distinct BY
  articleCount DESC, topicUri, articleUri;

STORE grounded_topics_articles_ordered INTO 'workspace/grounded_topics_articles.tsv';
