  --select * from `bigquery-public-data.iowa_liquor_sales.sales` limit 1
  --select count(1) from `weathersource-com.pub_weather_data_samples.sample_weather_and_crime_comparison_chicago_daily_2016`
  --SELECT * FROM `bigquery-public-data.ml_datasets.ulb_fraud_detection` LIMIT 1
  /*
SELECT 
  COALESCE(plurality, 0)        AS qtd_nascidos, 
  COALESCE(ever_born, 0)        AS qtd_filhos, 
  --COALESCE(born_dead, 0)        AS qtd_filhos_nao_nasceram, 
  --COALESCE(born_alive_dead, 0)  AS qtd_filhos_nasceram_vivos_e_estao_mortos, 
  --COALESCE(born_alive_alive, 0) AS qtd_filhos_nasceram_vivos_e_estao_vivos, 
  COALESCE(born_dead, 0) + COALESCE(born_alive_dead, 0) AS qtd_filhos_mortos,
  COALESCE(plurality, 0) + COALESCE(born_alive_alive, 0) AS qtd_filhos_vivos,
  COUNT(1) qtd
FROM `bigquery-public-data.samples.natality`
GROUP BY 
  qtd_nascidos, 
  qtd_filhos, 
  qtd_filhos_mortos,
  qtd_filhos_vivos
ORDER BY 
  qtd_nascidos, 
  qtd_filhos, 
  qtd_filhos_mortos,
  qtd_filhos_vivos
*/
  --select * from `bigquery-public-data.samples.natality` limit 10

/*
--FASES DA LUA
SELECT
  DATE(atual.peak_datetime) AS data_atual,
  anterior.phase AS fase_anterior,
  atual.phase AS fase_atual
FROM (  SELECT
          DATE(hoje.peak_datetime) AS data_atual,
          DATE(MAX(passado.peak_datetime)) data_anterior
        FROM `bigquery-public-data.moon_phases.moon_phases` AS hoje
        INNER JOIN `bigquery-public-data.moon_phases.moon_phases` AS passado
          ON DATE(passado.peak_datetime) < DATE(hoje.peak_datetime)
        GROUP BY data_atual ) AS relacionamento
INNER JOIN
  `bigquery-public-data.moon_phases.moon_phases` AS atual
    ON DATE(atual.peak_datetime) = relacionamento.data_atual
INNER JOIN
  `bigquery-public-data.moon_phases.moon_phases` AS anterior
    ON DATE(anterior.peak_datetime) = relacionamento.data_anterior
ORDER BY data_atual
*/

/*
select DATE(year, month, day), * 
from `bigquery-public-data.samples.natality`
limit 10
*/

--select SAFE_CAST('1970-11-30' AS DATE)
/*
select 
--  ( sum(CASE WHEN day IS NULL THEN 1 ELSE 0 END) / count(1) ) * 100 as percentual_dias_nao_preenchidos,
--  ( sum(CASE WHEN day IS NOT NULL THEN 1 ELSE 0 END) / count(1) ) * 100 as percentual_dias_preenchidos,
  ( sum(CASE WHEN day IS NOT NULL THEN 1 ELSE 0 END) ) as qtd_dias_preenchidos,
  ( min(SAFE_CAST(FORMAT('%i-%i-%i', year, month, day) AS DATE)) ) as min_data_nasc,
  ( max(SAFE_CAST(FORMAT('%i-%i-%i', year, month, day) AS DATE)) ) as max_data_nasc,
  min(year) as min_year,
  max(year) as max_year,
  'natality' as origem
from `bigquery-public-data.samples.natality`
union all
select
  count(1),
  date(min(peak_datetime)),
  date(max(peak_datetime)),
  extract(YEAR from date(min(peak_datetime))),
  extract(YEAR from date(max(peak_datetime))),
  'moon_phases'
FROM `bigquery-public-data.moon_phases.moon_phases`
*/


-- pergunta: como prever um aborto espontâneo?

/*
select 
  state, is_male, child_race, weight_pounds, plurality, apgar_1min, apgar_5min, mother_residence_state, mother_race,
  mother_age, gestation_weeks, lmp, mother_married, mother_birth_state, cigarette_use, cigarettes_per_day, alcohol_use, 
  drinks_per_week, weight_gain_pounds, born_alive_alive, born_alive_dead, ever_born, father_race, father_age,
  record_weight, born_dead
from 
  `bigquery-public-data.samples.natality` 
 limit 1000000
*/
/*
SELECT
  CAST(FORMAT('0x%s', TO_HEX(CODE_POINTS_TO_BYTES(TO_CODE_POINTS(COALESCE(state, 'XX'))))) AS INT64) AS state,
  COALESCE(SAFE_CAST(is_male AS INT64), -1) AS is_male,
  COALESCE(child_race, 99) AS child_race,
  COALESCE(weight_pounds, -1.0) AS weight_pounds,
  COALESCE(plurality, 1) AS plurality,
  CAST(FORMAT('0x%s', TO_HEX(CODE_POINTS_TO_BYTES(TO_CODE_POINTS(COALESCE(mother_residence_state, 'XX'))))) AS INT64) AS mother_residence_state,
  COALESCE(mother_race, 99) AS mother_race,
  COALESCE(mother_age, -1) AS mother_age, 
  COALESCE(gestation_weeks, gestation_weeks_calc) AS gestation_weeks,
  COALESCE(SAFE_CAST(mother_married AS INT64), -1) AS mother_married,
  CAST(FORMAT('0x%s', TO_HEX(CODE_POINTS_TO_BYTES(TO_CODE_POINTS(COALESCE(mother_birth_state, 'XX'))))) AS INT64) AS mother_birth_state,
  COALESCE(SAFE_CAST(cigarette_use AS INT64), -1) AS cigarette_use,
  COALESCE(cigarettes_per_day, 0) AS cigarettes_per_day,
  COALESCE(SAFE_CAST(alcohol_use AS INT64), -1) AS alcohol_use,
  COALESCE(drinks_per_week, 0) AS drinks_per_week,
  COALESCE(weight_gain_pounds, -1.0) AS weight_gain_pounds,
  COALESCE(born_alive_alive, 0) AS born_alive_alive,
  COALESCE(born_alive_dead, 0) AS born_alive_dead,
  COALESCE(born_dead, 0) AS born_dead,
  COALESCE(ever_born, 0) AS ever_born,
  COALESCE(father_race, 99) AS father_race,
  COALESCE(father_age, -1) AS father_age,
  CASE
    WHEN born_dead > 0
      THEN 1
    ELSE 0
  END AS y
FROM (
  SELECT
    state,
    is_male,
    child_race,
    weight_pounds,
    plurality,
    mother_residence_state,
    mother_race,
    mother_age,
    gestation_weeks,
    DATE_DIFF(birth_date, lmp_date, WEEK) AS gestation_weeks_calc,
    mother_married,
    mother_birth_state,
    cigarette_use,
    cigarettes_per_day,
    alcohol_use,
    drinks_per_week,
    weight_gain_pounds,
    born_alive_alive,
    born_alive_dead,
    born_dead,
    ever_born,
    father_race,
    father_age
  FROM (
    SELECT
      birth_date,
      state,
      is_male,
      child_race,
      weight_pounds,
      plurality,
      mother_residence_state,
      mother_race,
      mother_age,
      gestation_weeks,
      CASE
        WHEN lmp_date IS NOT NULL
          THEN lmp_date
        WHEN gestation_weeks IS NOT NULL
          THEN DATE_SUB(birth_date, INTERVAL gestation_weeks WEEK)
        ELSE NULL
      END AS lmp_date,
      mother_married,
      mother_birth_state,
      cigarette_use,
      cigarettes_per_day,
      alcohol_use,
      drinks_per_week,
      weight_gain_pounds,
      born_alive_alive,
      born_alive_dead,
      born_dead,
      ever_born,
      father_race,
      father_age
    FROM (
      SELECT
        CASE 
          WHEN SAFE_CAST(FORMAT('%i-%i-%i', birth_year, birth_month, birth_day) AS DATE) IS NULL
            THEN NULL
          ELSE DATE(birth_year, birth_month, birth_day)
        END AS birth_date,
        state,
        is_male,
        child_race,
        weight_pounds,
        plurality,
        mother_residence_state,
        mother_race,
        mother_age,
        CASE
          WHEN gestation_weeks = 99
            THEN NULL
          ELSE gestation_weeks
        END AS gestation_weeks,
        CASE 
          WHEN SAFE_CAST(FORMAT('%i-%i-%i', lmp_year, lmp_month, lmp_day) AS DATE) IS NULL
            THEN NULL
          ELSE DATE(lmp_year, lmp_month, lmp_day)
        END AS lmp_date,
        mother_married,
        mother_birth_state,
        cigarette_use,
        cigarettes_per_day,
        alcohol_use,
        drinks_per_week,
        weight_gain_pounds,
        born_alive_alive,
        born_alive_dead,
        born_dead,
        ever_born,
        father_race,
        father_age
      FROM (
        SELECT
          year AS birth_year,
          month AS birth_month,
          day AS birth_day,
          CASE
            WHEN TRIM(state) = ''
              THEN NULL
            ELSE TRIM(state)
          END AS state,
          is_male,
          child_race,
          weight_pounds,
          plurality,
          CASE
            WHEN TRIM(mother_residence_state) = ''
              THEN NULL
            ELSE TRIM(mother_residence_state)
          END AS mother_residence_state,
          mother_race,
          mother_age,
          gestation_weeks, 
          CASE
            WHEN SUBSTR(lmp, 0, 2) = '99'
              THEN NULL
            ELSE SAFE_CAST(SUBSTR(lmp, 0, 2) AS INT64)
          END AS lmp_month,
          CASE
            WHEN SUBSTR(lmp, 3, 2) = '99'
              THEN NULL
            ELSE SAFE_CAST(SUBSTR(lmp, 3, 2) AS INT64)
          END AS lmp_day,
          CASE 
            WHEN SAFE_CAST(SUBSTR(lmp, 5, 4) AS INT64) IS NULL
              THEN 
                CASE
                  WHEN SUBSTR(lmp, 5, 4) = FORMAT('%s%s-', SUBSTR(SAFE_CAST(year AS STRING), 0, 2), SUBSTR(SAFE_CAST(year AS STRING), 4, 1)) 
                    THEN year
                  WHEN SUBSTR(lmp, 5, 4) = FORMAT('%s%s-', SUBSTR(SAFE_CAST(year-1 AS STRING), 0, 2), SUBSTR(SAFE_CAST(year-1 AS STRING), 4, 1)) 
                    THEN year-1
                  ELSE NULL
                END
             ELSE SAFE_CAST(SUBSTR(lmp, 5, 4) AS INT64)
          END AS lmp_year,
          mother_married,
          CASE
            WHEN TRIM(mother_birth_state) = ''
              THEN NULL
            ELSE TRIM(mother_birth_state)
          END AS mother_birth_state,
          cigarette_use,
          cigarettes_per_day,
          alcohol_use,
          drinks_per_week,
          weight_gain_pounds,
          born_alive_alive,
          born_alive_dead,
          born_dead,
          COALESCE(ever_born, 0) AS ever_born,
          father_race,
          father_age
        FROM
          `bigquery-public-data.samples.natality`
    WHERE COALESCE(ever_born, 0) > COALESCE(born_dead, 0)
      AND born_dead IS NOT NULL
        )
      )
    )
  WHERE lmp_date IS NOT NULL
    AND birth_date IS NOT NULL
    AND lmp_date < birth_date
  )
WHERE COALESCE(gestation_weeks, gestation_weeks_calc) > 0
  AND COALESCE(gestation_weeks, gestation_weeks_calc) <= 50 

select 
  sum(case when y=1 then 1 else 0 end) as count_1,
  sum(case when y=0 then 1 else 0 end) as count_0,
  sum(case when y=1 then 1 else 0 end)/count(1) as perc_1, 
  sum(case when y=0 then 1 else 0 end)/count(1) as perc_0 
from `tcc-puc-mg-2019.book.natality`

*/
/*
SELECT count(1) from (

        SELECT
          *
        FROM (
          SELECT 
              is_male, child_race, weight_pounds, plurality, mother_race, 
              mother_age,gestation_weeks, mother_married, cigarette_use, 
              cigarettes_per_day, alcohol_use, drinks_per_week, weight_gain_pounds, 
              born_alive_alive, born_alive_dead, ever_born, father_race, father_age, y
          FROM 
              `tcc-puc-mg-2019.book.natality`
          WHERE y = 0
          LIMIT 7530390
        )
    UNION ALL
        SELECT
          *
        FROM (
          SELECT 
              is_male, child_race, weight_pounds, plurality, mother_race, 
              mother_age,gestation_weeks, mother_married, cigarette_use, 
              cigarettes_per_day, alcohol_use, drinks_per_week, weight_gain_pounds, 
              born_alive_alive, born_alive_dead, ever_born, father_race, father_age, y
          FROM 
              `tcc-puc-mg-2019.book.natality`
          WHERE y = 1
          LIMIT 1736279
        )
        
)
*/

/*CREATE OR REPLACE MODEL `tcc-puc-mg-2019.model.logistic_regression_is_born_dead`
OPTIONS 
  (
    MODEL_TYPE          = 'LOGISTIC_REG',
    AUTO_CLASS_WEIGHTS  = TRUE,
    INPUT_LABEL_COLS    = ['y']
  )
AS*/
  SELECT
    is_male, 
    CAST(child_race AS STRING) AS child_race, 
    weight_pounds, 
    plurality,
    CAST(mother_race AS STRING) AS mother_race, 
    mother_age,
    gestation_weeks, 
    mother_married, 
    cigarette_use, 
    cigarettes_per_day, 
    alcohol_use, 
    drinks_per_week, 
    weight_gain_pounds, 
    born_alive_alive, 
    born_alive_dead, 
    ever_born, 
    CAST(father_race AS STRING) AS father_race, 
    father_age, 
    y
  FROM 
    `tcc-puc-mg-2019.book.natality`
  LIMIT 1
  
        
