DECLARE start_date DATE;
DECLARE end_date DATE;

SET start_date = '2022-01-01';
SET end_date = '2022-01-31';

WITH ch AS
(
    SELECT c.*,
           w.object_id,
    CONCAT(c.agent_id, '-', CAST(session_index AS STRING)) AS session_id -- Create session_id
    , user_type_1
    , user_type_2
    , user_type_3
    FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
    LEFT JOIN `nyt-bigquery-beta-workspace.stuart_data.wc_devices` w ON w.pageview_id = c.pageview_id AND w.date = c.date
    LEFT JOIN `nyt-bigquery-beta-workspace.wirecutter_data.user_type`  ut ON ut.pageview_id = c.pageview_id
 
     WHERE c.date BETWEEN start_date AND end_date 
    AND (session_channel_2 = 'Direct' OR session_channel_2 = 'Organic Search') 
    AND w.device = 'Mobile'
),
fp AS -- Get first pageview for each session
(
    SELECT
        session_id,
        MIN(agent_day_session_pageview_index) AS pv
    FROM ch
    GROUP BY 1
),
behavior AS ( -- Get their browser and OS 
    SELECT
        pageview.pageview_id
        ,pageview.os_name
        ,pageview.browser_name

    FROM  `nyt-digpipelines-prd.analyst.behavior` AS bv,
        unnest(pageviews) AS p
    WHERE DATE(_pt) BETWEEN start_date AND end_date 

)
-- Select only first pageviews
    SELECT  
            DATE_TRUNC(date, MONTH) AS date,
            b.os_name,
            b.browser_name,
            COUNT(DISTINCT user_id)
      
          
    FROM ch
    JOIN fp ON
    fp.session_id = ch.session_id
    AND fp.pv = ch.agent_day_session_pageview_index
    LEFT JOIN behavior as b on b.pageview_id = ch.pageview_id
    WHERE ch.object_id = 'HOME'
    GROUP BY 1,2,3
    ORDER BY 1 ASC
    ;
