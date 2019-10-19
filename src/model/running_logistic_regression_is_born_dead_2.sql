SELECT
	predicted_is_born_dead,
	predicted_is_born_dead_probs	AS metrics 
FROM 
	ML.PREDICT(
	MODEL `tcc-puc-mg-2019.model.logistic_regression_is_born_dead_2`, 
	(
		SELECT 
			'true'			AS is_male,
			6.63370946358	AS weight_pounds,
			1				AS plurality, 
			'Filipino'		AS mother_race,
			45				AS mother_age,  
			38				AS gestation_weeks,
			'true'			AS is_mother_married,
			0				AS cigarettes_per_day,   
			0				AS drinks_per_week, 
			0				AS ever_born, 
			'Filipino'		AS father_race, 
			49				AS father_age, 
			0.908			AS birth_fu_birth_year_sub_national_hdi, 
			0.905			AS mother_residence_fu_lmp_year_sub_national_hdi,
			0.908			AS mother_residence_fu_birth_year_sub_national_hdi
	),
	STRUCT(0.4422 AS threshold)
