/**
 * Join the top skos topics with article abstracts and order
 * by most popular topics.
 *
 * Warning: this naive method gives poor results when the output is
 * indexed as this is on solr server to perform similarity queries
 * for automated topic assignement. Better use the 03bis alternative
 * script instead. The ouput is just 58M which is probably far to small
 * for a good semantic coverage.
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar
DEFINE SafeTsvText pignlproc.evaluation.SafeTsvText();

grounded_topics = LOAD 'workspace/grounded_topics.tsv'
  AS (topicUri: chararray, primaryArticleUri: chararray,
      articleCount: long, narrowerTopicCount:long, broaderTopicCount: long);

article_abstracts = LOAD 'workspace/long_abstracts_en.nt.gz'
  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
    'http://dbpedia.org/ontology/abstract', 'db:')
  AS (articleUri: chararray, articleAbstract: chararray);
  
-- Join the filtered topics with the article URI information by topicUri
topics_abstracts = JOIN
  grounded_topics BY primaryArticleUri,
  article_abstracts BY articleUri;
  
projected_topics_abstracts = FOREACH topics_abstracts
  GENERATE
    grounded_topics::topicUri AS topicUri,
    grounded_topics::articleCount AS articleCount,
    SafeTsvText(article_abstracts::articleAbstract) AS primaryArticleAbstract;
  
ordered_topics = ORDER projected_topics_abstracts BY
  articleCount DESC, topicUri ASC;

STORE ordered_topics INTO 'workspace/topics_primary_abstracts.tsv';
