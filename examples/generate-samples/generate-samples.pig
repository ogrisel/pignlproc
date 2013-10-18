
/**
 * Extracts from DBpedia sources the necessary data for the wikipedia sample.
 * Designed to run on a cluster...
 *
 */

-- Launch on cluster with: pig -p PIGNLPROC_JAR=../../target/pignlproc-0.1.0-SNAPSHOT.jar -p LANG=en -p RESOURCES_PATH=/wikipedia -p DEPENDENCIES=../../target/lib/*.jar generate-samples.pig

REGISTER $PIGNLPROC_JAR
REGISTER $DEPENDENCIES
%DECLARE WIKISAMPLE wiki-20090902-pages-articles-sample.xml
%DECLARE WIKIFILE $LANG$WIKISAMPLE
DEFINE extract_sentences pignlproc.evaluation.SentencesWithLink();

wikipedia_data = LOAD '$RESOURCES_PATH/$WIKIFILE'
  USING pignlproc.storage.ParsingWikipediaLoader('$LANG')
  AS (title, wikiuri, text, redirect, links, headers, paragraphs);

-- Load wikipedia, instance types and redirects from dbpedia dumps
dbpedia_wikipedia_links = LOAD '$RESOURCES_PATH/wikipedia_links_$LANG.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://xmlns.com/foaf/0.1/primaryTopic', '', '')
  AS (wikiuri: chararray, dburi: chararray);

-- Get all useful dbpedia_wikipedia_links
wikipedia_data_wikiuris = FOREACH wikipedia_data GENERATE wikiuri ;
selected_dburis = JOIN dbpedia_wikipedia_links BY wikiuri, wikipedia_data_wikiuris BY wikiuri ;
dbpedia_wikipedia_links_RDF = FOREACH selected_dburis GENERATE dbpedia_wikipedia_links::wikiuri, dburi ;

STORE dbpedia_wikipedia_links_RDF INTO 'wikipedia_links_$LANG-sample.nt' USING pignlproc.storage.UriUriNTriplesStorer('http://xmlns.com/foaf/0.1/primaryTopic') ;


/*
--dbpedia_redirects = LOAD '$INPUT/redirects_$LANG.nt'
--  USING pignlproc.storage.UriUriNTriplesLoader(
--    'http://dbpedia.org/ontology/wikiPageRedirects', '', '')
--  AS (source: chararray, target: chararray);

-- the last tuple can be (null, null)

--wikipedia_links_notnull =
--  FILTER wikipedia_links BY wikiuri IS NOT NULL;

-- follow the redirect links if any

--redirect_joined = JOIN wikipedia_links_notnull BY dburi LEFT OUTER, redirects BY source;
--redirected_wikipedia_links = FOREACH redirect_joined GENERATE
--  (target IS NOT NULL ? target : dburi) AS dburi, wikiuri;

-- Load dbpedia type data and filter out the overly generic owl:Thing type

--dbpedia_instance_types =
--  LOAD '$INPUT/instance_types_en.nt'
--  USING pignlproc.storage.UriUriNTriplesLoader(
--    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', '', '')
--  AS (dburi: chararray, type: chararray);

--instance_types_no_thing =
--  FILTER instance_types BY type NEQ 'http://www.w3.org/2002/07/owl#Thing';

--joined = JOIN instance_types_no_thing BY dburi, redirected_wikipedia_links BY dburi;

*/


