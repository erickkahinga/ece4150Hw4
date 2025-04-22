-- Return the single (bigram, year, count) row that has the highest count value in the entire googlebooks table.

-- Read all rows from googlebooks table and extract: bigram, year and count.
SELECT
    bigram,
    year,
    `count`
FROM hue__tmp_googlebooks
-- Ordering by Descending values means the largest count is first.
ORDER BY `count` DESC
-- So by limiting to 1 means returing the top row aka highest count bigram.
LIMIT 1;
