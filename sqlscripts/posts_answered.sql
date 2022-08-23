/* What is the percentage of questions that have been answered? */
SELECT
	SUM(CASE 
			WHEN answer_count > 0 THEN 1 
			ELSE 0 
		END) AS posts_answered, -- Number of posts answered.
	SUM(CASE 
			WHEN accepted_answer_id IS NOT NULL THEN 1 
			ELSE 0 END) 
		AS posts_answered_accepted, -- Number of posts with an accepted answer.
	COUNT(*) AS posts_total, -- Total number of posts.
	ROUND(CAST(SUM(
		CASE WHEN answer_count > 0 THEN 1 ELSE 0 END) AS FLOAT) 
		/ CAST(COUNT(id) AS FLOAT) * 100, 2) AS posts_answered_percentage, -- Percentage of posts answered.
	ROUND(CAST(SUM(
		CASE WHEN accepted_answer_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) 
		/ CAST(COUNT(id) AS FLOAT) * 100, 2) AS posts_accepted_answer_percentage -- Percentage of posts with accepted answer.
FROM dbo.posts
WHERE post_type_id = 1 -- Filter on questions only