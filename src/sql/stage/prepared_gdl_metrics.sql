WITH
  state AS (
    SELECT DISTINCT
      state.state_name                AS name,
      state.state_postal_abbreviation AS fu
    FROM `bigquery-public-data.census_utility.fips_codes_states` AS state
    WHERE TRIM(COALESCE(state.state_name, '')) != ''
      AND TRIM(COALESCE(state.state_postal_abbreviation, '')) != ''
  ),
  year AS (
    SELECT 1990 AS value
    UNION ALL SELECT 1991 
    UNION ALL SELECT 1992 
    UNION ALL SELECT 1993 
    UNION ALL SELECT 1994 
    UNION ALL SELECT 1995 
    UNION ALL SELECT 1996 
    UNION ALL SELECT 1997 
    UNION ALL SELECT 1998 
    UNION ALL SELECT 1999 
    UNION ALL SELECT 2000 
    UNION ALL SELECT 2001 
    UNION ALL SELECT 2002 
    UNION ALL SELECT 2003 
    UNION ALL SELECT 2004 
    UNION ALL SELECT 2005 
    UNION ALL SELECT 2006 
    UNION ALL SELECT 2007 
    UNION ALL SELECT 2008 
    UNION ALL SELECT 2009 
    UNION ALL SELECT 2010 
    UNION ALL SELECT 2011 
    UNION ALL SELECT 2012 
    UNION ALL SELECT 2013 
    UNION ALL SELECT 2014 
    UNION ALL SELECT 2015 
    UNION ALL SELECT 2016 
    UNION ALL SELECT 2017),
  educational_index AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_educational_index_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  expected_years_schooling AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_expected_years_schooling_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  health_index AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_health_index_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  income_index AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_income_index_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  life_expectancy AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_life_expectancy_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  gross_national_income_per_capita AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_log_gross_national_income_per_capita_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  mean_years_schooling AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_mean_years_schooling_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  population_size_in_thousands AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_population_size_in_thousands_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != ''),
  sub_national_hdi AS (
    SELECT
      [
        STRUCT(data.region AS state, 1990 AS year, data._1990 AS value),
        STRUCT(data.region AS state, 1991 AS year, data._1991 AS value),
        STRUCT(data.region AS state, 1992 AS year, data._1992 AS value),
        STRUCT(data.region AS state, 1993 AS year, data._1993 AS value),
        STRUCT(data.region AS state, 1994 AS year, data._1994 AS value),
        STRUCT(data.region AS state, 1995 AS year, data._1995 AS value),
        STRUCT(data.region AS state, 1996 AS year, data._1996 AS value),
        STRUCT(data.region AS state, 1997 AS year, data._1997 AS value),
        STRUCT(data.region AS state, 1998 AS year, data._1998 AS value),
        STRUCT(data.region AS state, 1999 AS year, data._1999 AS value),
        STRUCT(data.region AS state, 2000 AS year, data._2000 AS value),
        STRUCT(data.region AS state, 2001 AS year, data._2001 AS value),
        STRUCT(data.region AS state, 2002 AS year, data._2002 AS value),
        STRUCT(data.region AS state, 2003 AS year, data._2003 AS value),
        STRUCT(data.region AS state, 2004 AS year, data._2004 AS value),
        STRUCT(data.region AS state, 2005 AS year, data._2005 AS value),
        STRUCT(data.region AS state, 2006 AS year, data._2006 AS value),
        STRUCT(data.region AS state, 2007 AS year, data._2007 AS value),
        STRUCT(data.region AS state, 2008 AS year, data._2008 AS value),
        STRUCT(data.region AS state, 2009 AS year, data._2009 AS value),
        STRUCT(data.region AS state, 2010 AS year, data._2010 AS value),
        STRUCT(data.region AS state, 2011 AS year, data._2011 AS value),
        STRUCT(data.region AS state, 2012 AS year, data._2012 AS value),
        STRUCT(data.region AS state, 2013 AS year, data._2013 AS value),
        STRUCT(data.region AS state, 2014 AS year, data._2014 AS value),
        STRUCT(data.region AS state, 2015 AS year, data._2015 AS value),
        STRUCT(data.region AS state, 2016 AS year, data._2016 AS value),
        STRUCT(data.region AS state, 2017 AS year, data._2017 AS value)
      ] AS metric  
    FROM `tcc-puc-mg-2019.raw.gdl_sub_national_hdi_data` AS data
    WHERE TRIM(COALESCE(data.region, '')) != '')
  SELECT DISTINCT
    state.name                                                AS state,
    state.fu                                                  AS fu,
    year.value                                                AS year,
    CAST(educational_index.value AS FLOAT64)                  AS educational_index,
    CAST(expected_years_schooling.value AS FLOAT64)           AS expected_years_schooling,
    CAST(health_index.value AS FLOAT64)                       AS health_index,
    CAST(income_index.value AS FLOAT64)                       AS income_index,
    CAST(life_expectancy.value AS FLOAT64)                    AS life_expectancy,
    CAST(gross_national_income_per_capita.value AS FLOAT64)   AS gross_national_income_per_capita,
    CAST(mean_years_schooling.value AS FLOAT64)               AS mean_years_schooling,
    CAST(population_size_in_thousands.value AS FLOAT64)       AS population_size_in_thousands,
    CAST(sub_national_hdi.value AS FLOAT64)                   AS sub_national_hdi
  FROM
    year,
    state
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM educational_index 
      CROSS JOIN UNNEST(educational_index.metric) AS metric)                AS educational_index
     ON   educational_index.year  = year.value
     AND  educational_index.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM expected_years_schooling 
      CROSS JOIN UNNEST(expected_years_schooling.metric) AS metric)         AS expected_years_schooling
     ON   expected_years_schooling.year  = year.value
     AND  expected_years_schooling.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM health_index 
      CROSS JOIN UNNEST(health_index.metric) AS metric)                     AS health_index
     ON   health_index.year  = year.value
     AND  health_index.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM income_index 
      CROSS JOIN UNNEST(income_index.metric) AS metric)                     AS income_index
     ON   income_index.year  = year.value
     AND  income_index.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM life_expectancy 
      CROSS JOIN UNNEST(life_expectancy.metric) AS metric)                  AS life_expectancy
     ON   life_expectancy.year  = year.value
     AND  life_expectancy.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM gross_national_income_per_capita 
      CROSS JOIN UNNEST(gross_national_income_per_capita.metric) AS metric) AS gross_national_income_per_capita
     ON   gross_national_income_per_capita.year  = year.value
     AND  gross_national_income_per_capita.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM mean_years_schooling 
      CROSS JOIN UNNEST(mean_years_schooling.metric) AS metric)             AS mean_years_schooling
     ON   mean_years_schooling.year  = year.value
     AND  mean_years_schooling.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM population_size_in_thousands 
      CROSS JOIN UNNEST(population_size_in_thousands.metric) AS metric)     AS population_size_in_thousands
     ON   population_size_in_thousands.year  = year.value
     AND  population_size_in_thousands.state = state.name
  INNER JOIN
    (SELECT metric.state, metric.year, metric.value
     FROM sub_national_hdi 
      CROSS JOIN UNNEST(sub_national_hdi.metric) AS metric)                 AS sub_national_hdi
     ON   sub_national_hdi.year  = year.value
     AND  sub_national_hdi.state = state.name