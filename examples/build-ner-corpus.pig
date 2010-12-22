-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Parse the wikipedia dump and extract text and links data
parsed =
  LOAD '$WIKIPEDIA'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, wikiuri, text, redirect, links, headers, paragraphs);

-- Load wikipedia, instance types and redirects from dbpedia dumps
wikipedia_links =
  LOAD '$DBPEDIA/wikipedia_links_en.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://xmlns.com/foaf/0.1/primaryTopic')
  AS (wikiuri, dburi);

instance_types =
  LOAD '$DBPEDIA/instance_types_en.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
  AS (dburi, type);

instance_types_no_thing =
  FILTER instance_types BY type != 'http://www.w3.org/2002/07/owl#Thing';

redirects =
  LOAD '$DBPEDIA/redirects_en.nt'
  USING pignlproc.storage.UriUriNTriplesLoader(
    'http://dbpedia.org/property/redirect')
  AS (redirected_from, redirerected_to);

-- Extract the sentence contexts of the links respecting the paragraph
-- boundaries
sentences =
  FOREACH parsed
  GENERATE flatten(pignlproc.evaluation.SentencesWithLink(
    text, links, paragraphs));

-- Perform successive joins to find the type of the linkTarget
joined1 = JOIN sentences BY linkTarget, wikipedia_links BY wikiuri USING 'skewed';
joined2 = JOIN joined1 BY dburi, instance_types_no_thing BY dburi USING 'skewed';
-- TODO: handle the redirects properly with a left outer join and a conditional
-- expression

result = FOREACH joined2 GENERATE type, linkTarget, linkBegin, linkEnd, sentence;

--tmp = LIMIT joined2 10; DUMP tmp;
STORE result INTO '$OUTPUT' USING PigStorage();
