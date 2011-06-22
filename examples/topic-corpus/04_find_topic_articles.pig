/**
 * Find all the articles uri of topics that are descendants up
 * to level 3 of grounded topics.
 */

SET default_parallel 20

-- Register the project jar to use the custom loaders and UDFs
REGISTER target/pignlproc-0.1.0-SNAPSHOT.jar

