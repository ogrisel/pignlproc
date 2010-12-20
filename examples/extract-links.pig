-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER ../target/pignlproc-0.1.0-SNAPSHOT.jar

-- Parse the wikipedia dump and extract text and links data
parsed =
  LOAD '../src/test/resources/enwiki-20090902-pages-articles-sample.xml'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, uri, text, links);

DESCRIBE parsed;

-- Flatten the links
links1 =
  FOREACH parsed
  GENERATE uri, flatten(links);

DESCRIBE links1;

-- Select the target link
links2 =
  FOREACH links1
  GENERATE uri, value;

DESCRIBE links2;
DUMP links2;

--STORE links2 INTO 'extract-links-results' USING PigStorage();
