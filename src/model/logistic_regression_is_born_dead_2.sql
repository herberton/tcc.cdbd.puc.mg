CREATE OR REPLACE MODEL
	`tcc-puc-mg-2019.model.logistic_regression_is_born_dead_2`
OPTIONS (
	MODEL_TYPE			= 'LOGISTIC_REG',
	AUTO_CLASS_WEIGHTS	= TRUE,
	INPUT_LABEL_COLS	= ['is_born_dead']
) AS
	SELECT
        data_source.is_male,
        data_source.weight_pounds,
        data_source.plurality, 
        data_source.mother_race,
        data_source.mother_age,  
        data_source.gestation_weeks,
        data_source.is_mother_married,
        data_source.cigarettes_per_day,   
        data_source.drinks_per_week, 
        data_source.ever_born, 
        data_source.father_race, 
        data_source.father_age, 
        data_source.birth_fu_birth_year_sub_national_hdi, 
        data_source.mother_residence_fu_lmp_year_sub_national_hdi,
        data_source.mother_residence_fu_birth_year_sub_national_hdi,
        data_source.is_born_dead
FROM 
        `tcc-puc-mg-2019.model.logistic_regression_is_born_dead_data_source_2` AS data_source