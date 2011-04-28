/**
 * Join the top skos categories with article abstracts and order
 * by most popular categories.
 *
 * The schema of the result tuples is:
 * (categoryUri: chararray, categoryCount: int, articleUri: chararray,
 *  articleAbstract: chararray)
 */

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

article_categories = LOAD '$INPUT/article_categories_$LANG.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://purl.org/dc/terms/subject')
  AS (articleUri: chararray, categoryUri: chararray);

-- Only keep the topics of the most popular articles while also
-- filtering out overly broad categories such as living people
grouped_categories = GROUP article_categories BY categoryUri;

counted_categories = FOREACH grouped_categories
  GENERATE
    group AS categoryUri,
    COUNT(article_categories.articleUri) AS categoryCount,
    flatten(article_categories.articleUri) AS articleUri;
    
popuplar_categories =
  FILTER counted_categories
    BY categoryCount < 10 OR categoryCount > 100000;

ordered_categories = ORDER counted_categories BY
  categoryCount DESC, categoryUri ASC, articleUri ASC;

--article_abstracts = LOAD '$INPUT/long_abstracts_$LANG.nt'
--  USING pignlproc.storage.UriStringLiteralNTriplesLoader(
--    'http://dbpedia.org/ontology/abstract')
--  AS (articleUri: chararray, articleAbstract: chararray);
-- filter and project as early as possible

STORE ordered_categories INTO '$OUTPUT/$LANG/tmp';
