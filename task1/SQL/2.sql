SELECT *
FROM (
SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY date_added) AS rn
FROM WorkExperience
) AS subquery
WHERE rn = 2 AND user_id = <user_id>;



-- another

SELECT *
FROM WorkExperience
WHERE user_id = <user_id>
ORDER BY date_added
LIMIT 1 OFFSET 1;

