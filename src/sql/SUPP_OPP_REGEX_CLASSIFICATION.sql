SELECT 
	COUNT(
		CASE 
			WHEN doc_text 
				LIKE '%in%support%of%'
			THEN 1
			ELSE NULL
		END		
	) AS IN_SUPPORT_COUNT,
	COUNT(
		CASE 
			WHEN doc_text 
				LIKE '%in%opposition%to%'
			THEN 1
			ELSE NULL
		END		
	) AS IN_OPPOSITION_COUNT,
	COUNT(
		CASE 
			WHEN 
				(doc_text LIKE '%in%opposition%to%'
				AND doc_text LIKE '%in%support%of%')
			THEN 1
			ELSE NULL
		END		
	) AS SUPPORT_AND_OPP,
	COUNT(
		CASE 
			WHEN 
				doc_text LIKE '%neither%for%nor%against%'
			THEN 1
			ELSE NULL
		END		
	) AS NEITHER,
	COUNT(*) AS TOTAL
FROM main.TESTIMONY_DETAIL
