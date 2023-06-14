SELECT u.user_id, u.name
FROM User_data u
INNER JOIN UserSkills us ON u.user_id = us.user_id
INNER JOIN (
    SELECT skill_id
    FROM Skills
    ORDER BY skill_name DESC
    LIMIT 10
) top_skills ON us.skill_id = top_skills.skill_id
GROUP BY u.user_id, u.name
HAVING COUNT(*) >= 5;
