/*  How long does it take for questions to receive answers? 
	Remove top outliers first (outliers where the time spent for an answer is more than 2 standard
	deviations from the mean)*/
SELECT	MAX(timespent) AS maximum_time_days,
		STDEV(timespent) AS standard_deviation_days,
		AVG(timespent) AS mean_time_days
FROM (
		SELECT DATEDIFF(day, MIN(q.creation_date), MIN(a.creation_date)) AS timespent
		FROM dbo.posts AS a
		LEFT JOIN dbo.posts AS q -- Join with same table to join questions with answers.
		ON q.id = a.parent_id
		AND q.archive_id = a.archive_id -- Make sure the join is with the same archive_id
		WHERE a.post_type_id = 2
		AND q.answer_count > 0 -- Filter posts that have answers.
		GROUP BY a.id -- Group by post to get time answered per post.
) AS timespent_per_question
WHERE timespent < 70 + 2 * 242
