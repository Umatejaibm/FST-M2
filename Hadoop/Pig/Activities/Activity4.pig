inputFile = LOAD '/root/input.txt' AS (lines:chararray);
-- tokenize the lines to get words
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;
-- Combine the words from the above stage
grpd = GROUP words BY word;
-- Count the occurence of each word (Reduce)
cntd = FOREACH grpd GENERATE $0, COUNT($1) AS wordCount;
--Delete the output folder

-- Store the result in HDFS
STORE cntd INTO '/root/PigResult';