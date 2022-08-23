/* Which day of the week has most questions answered within an hour? */
SELECT	DATENAME(weekday, day_of_week) AS day_of_week,
		COUNT(nr_of_questions) AS nr_of_questions
FROM (
	SELECT	DATEPART(weekday, MIN(q.creation_date)) AS day_of_week,
			/* All questions answered within 60 minutes get value 1 and others 0 to be able to count them. */
			CASE
				WHEN DATEDIFF(minute, MIN(q.creation_date), MIN(a.creation_date)) < 60 THEN 1
				ELSE 0
			END AS nr_of_questions
	FROM dbo.posts a
	LEFT JOIN dbo.posts AS q -- Join with same table to join questions with answers.
	ON	q.id = a.parent_id
	AND q.archive_id = a.archive_id
	WHERE a.post_type_id = 2
	AND q.answer_count > 0
	GROUP BY a.id -- Group by post to match question and first answer. 
) AS questions_answered_within_hour
GROUP BY day_of_week -- Group by weekday to get number of questions per weekday. 
ORDER BY nr_of_questions DESC