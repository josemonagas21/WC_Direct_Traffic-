DECLARE start_date DATE;
DECLARE end_date DATE;

SET start_date = '2022-01-01';
SET end_date = '2022-10-31';


  SELECT 
    DATE_TRUNC(c.date, MONTH) AS date,
    user_type_1,
    COUNT(DISTINCT c.user_id) as traffic
    

FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c 
  LEFT JOIN `nyt-bigquery-beta-workspace.wirecutter_data.user_type`  ut ON ut.pageview_id = c.pageview_id
  WHERE 
       c.date BETWEEN start_date AND end_date
  GROUP BY 1,2
  ORDER BY 1 ASC