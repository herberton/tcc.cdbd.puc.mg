WITH 
	race AS (
		SELECT 1 AS id, 'White'	AS description
		UNION ALL SELECT 2, 'Black'
		UNION ALL SELECT 3, 'American Indian'
		UNION ALL SELECT 4, 'Chinese'
		UNION ALL SELECT 5, 'Japanese'
		UNION ALL SELECT 6, 'Hawaiian'
		UNION ALL SELECT 7, 'Filipino'
		UNION ALL SELECT 9, 'Unknown/Other'
		UNION ALL SELECT 18, 'Asian Indian'
		UNION ALL SELECT 28, 'Korean'
		UNION ALL SELECT 39, 'Samoan'
		UNION ALL SELECT 48, 'Vietnamese')
SELECT
	natality.birth_year,
  natality.birth_month,
  natality.birth_day,
  natality.birth_date,
	natality.birth_fu,
	natality.is_male,
  child_race.description		AS child_race,
	natality.weight_pounds,
	natality.plurality,
	natality.mother_residence_fu,
  mother_race.description		AS mother_race,
	natality.mother_age, 
	natality.gestation_weeks,
	natality.lmp_year,
  natality.lmp_month, 
  natality.lmp_day,
  natality.lmp_date,
  natality.is_mother_married,
	natality.mother_birth_fu,
	natality.cigarettes_per_day,
	natality.drinks_per_week,
	natality.born_alive_alive,
	natality.born_alive_dead,
	natality.ever_born,
  father_race.description		AS father_race,
	natality.father_age,
	natality.is_born_dead
FROM (
	SELECT
		natality.birth_year,
    natality.birth_month,
    natality.birth_day,
    natality.birth_date,
		natality.birth_fu,
		natality.is_male,
		natality.child_race,
		natality.weight_pounds,
		natality.plurality,
		natality.mother_residence_fu,
		natality.mother_race,
		natality.mother_age,
		CASE
      WHEN natality.gestation_weeks IS NOT NULL
        THEN natality.gestation_weeks
      WHEN natality.birth_date IS NOT NULL AND natality.lmp_date IS NOT NULL
        THEN DATE_DIFF(natality.birth_date, natality.lmp_date, WEEK)
      ELSE NULL
    END AS gestation_weeks,
		natality.lmp_year,
    natality.lmp_month, 
    natality.lmp_day,
    natality.lmp_date,
		natality.is_mother_married,
		natality.mother_birth_fu,
		natality.cigarettes_per_day,
		natality.drinks_per_week,
		natality.weight_gain_pounds,
		natality.born_alive_alive,
		natality.born_alive_dead,
		natality.ever_born,
		natality.father_race,
		natality.father_age,
		natality.is_born_dead
	FROM (
		SELECT
			natality.birth_year,
      natality.birth_month,
      natality.birth_day,
      natality.birth_date,
			natality.birth_fu,
			natality.is_male,
			natality.child_race,
			natality.weight_pounds,
			natality.plurality,
			natality.mother_residence_fu,
			natality.mother_race,
			natality.mother_age,
			natality.gestation_weeks,
			natality.lmp_year,
      natality.lmp_month, 
      natality.lmp_day,
      CASE
				WHEN natality.lmp_date IS NOT NULL
					THEN natality.lmp_date
				WHEN natality.gestation_weeks IS NOT NULL
					THEN DATE_SUB(natality.birth_date, INTERVAL natality.gestation_weeks WEEK)
				ELSE NULL
			END AS lmp_date,
			natality.is_mother_married,
			natality.mother_birth_fu,
			natality.cigarettes_per_day,
			natality.drinks_per_week,
			natality.weight_gain_pounds,
			natality.born_alive_alive,
			natality.born_alive_dead,
			natality.ever_born,
			natality.father_race,
			natality.father_age,
			natality.is_born_dead
		FROM
			(
			SELECT
				natality.birth_year,
        natality.birth_month,
        natality.birth_day,
				natality.birth_date,
				natality.birth_fu,
				natality.is_male,
				natality.child_race,
				natality.weight_pounds,
				natality.plurality,
				natality.mother_residence_fu,
				natality.mother_race,
				natality.mother_age,
				natality.gestation_weeks,
				natality.lmp_year,
        natality.lmp_month, 
        natality.lmp_day,
				CASE 
					WHEN SAFE_CAST(FORMAT('%i-%i-%i', natality.lmp_year, natality.lmp_month, natality.lmp_day) AS DATE) IS NULL
						THEN NULL
					ELSE DATE(natality.lmp_year, natality.lmp_month, natality.lmp_day)
				END	AS lmp_date,
				natality.is_mother_married,
				natality.mother_birth_fu,
				natality.cigarettes_per_day,
				natality.drinks_per_week,
				natality.weight_gain_pounds,
				natality.born_alive_alive,
				natality.born_alive_dead,
				natality.ever_born,
				natality.father_race,
				natality.father_age,
				CASE 
					WHEN natality.born_dead > 0 THEN TRUE 
					ELSE FALSE 
				END AS is_born_dead
			FROM (
				SELECT 
					natality.year					                       AS birth_year,
          natality.month				                       AS birth_month,
          natality.day					                       AS birth_day,
					CASE
            WHEN SAFE_CAST(FORMAT('%i-%i-%i', natality.year, natality.month, natality.day) AS DATE) IS NULL
						  THEN NULL
						ELSE DATE(natality.year, natality.month, natality.day)
					END										                       AS birth_date,
					CASE 
						WHEN TRIM(natality.state) = '' THEN NULL 
						ELSE TRIM(natality.state) 
					END										                       AS birth_fu,
					SAFE_CAST(natality.is_male AS STRING)        AS is_male,
					COALESCE(natality.child_race, 9)             AS child_race,
					natality.weight_pounds,
					COALESCE(natality.plurality, 1)			         AS plurality,
					CASE
						WHEN TRIM(natality.mother_residence_state) = '' THEN NULL 
						ELSE TRIM(natality.mother_residence_state)
					END										AS mother_residence_fu,
					COALESCE(natality.mother_race, 9)            AS mother_race,
					natality.mother_age,
					CASE 
						WHEN COALESCE(natality.gestation_weeks, 99) = 99 THEN NULL 
						ELSE natality.gestation_weeks 
					END										                       AS gestation_weeks, 
					CASE 
						WHEN SAFE_CAST(SUBSTR(natality.lmp, 5, 4) AS INT64) IS NULL
						THEN 
							CASE
								WHEN SUBSTR(natality.lmp, 5, 4) = FORMAT('%s%s-', SUBSTR(SAFE_CAST(natality.year AS STRING), 0, 2), SUBSTR(SAFE_CAST(natality.year AS STRING), 4, 1)) 
									THEN natality.year
								WHEN SUBSTR(natality.lmp, 5, 4) = FORMAT('%s%s-', SUBSTR(SAFE_CAST(natality.year-1 AS STRING), 0, 2), SUBSTR(SAFE_CAST(natality.year-1 AS STRING), 4, 1)) 
									THEN natality.year-1
								ELSE NULL
							END
						ELSE SAFE_CAST(SUBSTR(natality.lmp, 5, 4) AS INT64)
					END										                       AS lmp_year,
					CASE 
						WHEN SUBSTR(lmp, 0, 2) = '99' THEN NULL 
						ELSE SAFE_CAST(SUBSTR(lmp, 0, 2) AS INT64) 
					END										                       AS lmp_month,
					CASE 
						WHEN SUBSTR(natality.lmp, 3, 2) = '99' THEN NULL 
						ELSE SAFE_CAST(SUBSTR(natality.lmp, 3, 2) AS INT64) 
					END										                       AS lmp_day,
					SAFE_CAST(natality.mother_married AS STRING) AS is_mother_married,
					CASE 
						WHEN TRIM(mother_birth_state) = '' THEN NULL 
						ELSE TRIM(mother_birth_state) 
					END										                       AS mother_birth_fu,
					CASE
						WHEN COALESCE(natality.cigarettes_per_day, 0) > 0
							THEN natality.cigarettes_per_day 
						ELSE
							CASE
								WHEN natality.cigarette_use IS NULL
									THEN 0
								WHEN natality.cigarette_use = TRUE
									THEN
										CASE
											WHEN COALESCE(natality.cigarettes_per_day, 0) = 0
												THEN 1
											ELSE natality.cigarettes_per_day
										END
								ELSE 0
							END
					END										                        AS cigarettes_per_day,
					CASE
						WHEN COALESCE(natality.drinks_per_week, 0) > 0
							THEN natality.drinks_per_week 
						ELSE
							CASE
								WHEN natality.alcohol_use IS NULL
									THEN 0
								WHEN natality.alcohol_use = TRUE
									THEN
										CASE
											WHEN COALESCE(natality.drinks_per_week, 0) = 0
												THEN 1
											ELSE natality.drinks_per_week
										END
								ELSE 0
							END
					END										                       AS drinks_per_week,
					natality.weight_gain_pounds,
					COALESCE(natality.born_alive_alive, 0)	     AS born_alive_alive,
					COALESCE(natality.born_alive_dead, 0)	       AS born_alive_dead,
					COALESCE(natality.ever_born, 0)					     AS ever_born,
					COALESCE(natality.father_race, 9)            AS father_race,
					natality.father_age,
					COALESCE(natality.born_dead, 0)              AS born_dead
				FROM `bigquery-public-data.samples.natality`   AS natality
				WHERE COALESCE(ever_born, 0) > COALESCE(born_dead, 0)
			)	AS natality
		)	AS natality
	)	AS natality
)	AS natality
INNER JOIN race                                 AS child_race
	ON child_race.id = natality.child_race
INNER JOIN race                                 AS mother_race
	ON mother_race.id = natality.mother_race
INNER JOIN race                                 AS father_race
	ON father_race.id = natality.father_race
WHERE natality.gestation_weeks > 3
  AND natality.gestation_weeks <= 42