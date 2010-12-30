-- Query incoming link popularity - local mode

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR

-- Parse the wikipedia dump and extract text and links data
parsed =
  LOAD '$INPUT'
  USING pignlproc.storage.ParsingWikipediaLoader('en')
  AS (title, uri, text, redirect, links, headers, paragraphs);

-- Extract the sentence contexts of the links respecting the paragraph
-- boundaries
sentences =
  FOREACH parsed
  GENERATE uri, pignlproc.evaluation.SentencesWithLink(text, links, paragraphs);

-- Flatten the links
flattened =
  FOREACH sentences
  GENERATE uri, flatten($1);

STORE flattened INTO '$OUTPUT' USING PigStorage();
