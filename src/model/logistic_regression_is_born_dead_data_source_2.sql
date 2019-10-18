SELECT

  natality.is_male,
  natality.weight_pounds,
  natality.plurality,
  natality.mother_race,
  natality.mother_age,
  natality.gestation_weeks,
  natality.is_mother_married,
  natality.cigarettes_per_day,
  natality.drinks_per_week,
  natality.ever_born,
  natality.father_race,
  natality.father_age,
  
  -- metrics_birth_fu_birth_year
  metrics_birth_fu_birth_year.sub_national_hdi                            AS birth_fu_birth_year_sub_national_hdi,
  
  -- metrics_mother_residence_fu_lmp_year
  metrics_mother_residence_fu_lmp_year.sub_national_hdi                   AS mother_residence_fu_lmp_year_sub_national_hdi,
  
  -- metrics_mother_residence_fu_birth_year
  metrics_mother_residence_fu_birth_year.sub_national_hdi                 AS mother_residence_fu_birth_year_sub_national_hdi,
  
  natality.is_born_dead
  
FROM `tcc-puc-mg-2019.stage.prepared_natality`           AS natality
INNER JOIN `tcc-puc-mg-2019.stage.prepared_gdl_metrics`  AS metrics_birth_fu_birth_year
  ON  metrics_birth_fu_birth_year.fu = natality.birth_fu
  AND metrics_birth_fu_birth_year.year = natality.birth_year
INNER JOIN `tcc-puc-mg-2019.stage.prepared_gdl_metrics`  AS metrics_mother_residence_fu_lmp_year
  ON  metrics_mother_residence_fu_lmp_year.fu   = natality.mother_residence_fu
  AND metrics_mother_residence_fu_lmp_year.year = natality.lmp_year
INNER JOIN `tcc-puc-mg-2019.stage.prepared_gdl_metrics`  AS metrics_mother_residence_fu_birth_year
  ON  metrics_mother_residence_fu_birth_year.fu   = natality.mother_residence_fu
  AND metrics_mother_residence_fu_birth_year.year = natality.birth_year 