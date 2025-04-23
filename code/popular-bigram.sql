/*
This Hive query grabs the single bigram that appears most often
across the entire dataset:
1. Selects the bigram, its year, and its total count.
2. Sorts the results so the highest count is first.
3. Limits the output to just that top row.
*/

-- 1. Pull the columns we care about from the Hive table
SELECT
    bigram,        -- the two word phrase
    year,          -- the year it appears
    `count`        -- how many times it occurred that year
FROM hue__tmp_googlebooks

-- 2. Order by occurrence count descending to bring the max to the top
ORDER BY
    `count` DESC

-- 3. Limit to just the top record (the overall most common bigram)
LIMIT
    1;
