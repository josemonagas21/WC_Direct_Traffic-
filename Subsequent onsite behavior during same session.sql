-- WE WANT TO UNDERSTAND USER BEHAVIOR AFTER THEY LAND ON THE WC HP VIA DIRECT OR ORGANIC SEARCH
-- STEPS TAKEN:
-- 1) OBTAIN THE SESSION INDEX FOR THESE GROUP OF USERS AND 
-- 2) LOOK AT THE SUBSEQUENT PAGEVIEWS AND CLICKS THEY HAVE DURING THAT SAME SESSION INDEX AND FILTER BY THOSE CASES WHERE THEIR "agent_day_session_pageview_index" > 1 TO MAKE SURE THAT THESE ARE SUBSEQUENT PAGEVIEWS DURING THAT SESSION. 
-- THE APPROACH IS AS FOLLOW:
-- RUN THE FIRST SCRIP AND CREATE A TEMPORARY TABLE WITH THE SESSION DATA 
-- NEXT RUN THE SECOND SCRIP AND ADD THAT CREATED TABLE IN THE "SUBSET" CTE 
-- CAVEAT: PAY ATTENTION TO THE FILTERS IN THE WHERE CLAUSE. DEVICE AND SESSION_CHANNEL_2


-- FIRST SCRIPT -- 
DECLARE start_date DATE;
DECLARE end_date DATE;
SET start_date = '2022-10-01';
SET end_date = '2022-10-31';


--Avg sessions per month – avg count of sessions per agent 
-- Users on mobile web > Direct which their LP or first pageview in their session is the WC HOME PAGE 
WITH ch AS
(
    SELECT c.*,
           w.object_id,
    CONCAT(c.agent_id, '-', CAST(session_index AS STRING)) AS session_id -- Create session_id

    FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
            LEFT JOIN `nyt-bigquery-beta-workspace.stuart_data.wc_devices` w ON w.pageview_id = c.pageview_id AND w.date = c.date
            WHERE c.date BETWEEN start_date AND end_date 
            -- AND (session_channel_2 = 'Direct' OR session_channel_2 = 'Organic Search') 
            AND session_channel_2 = 'Direct'
),
fp AS -- Get first pageview for each session
(
    SELECT
        session_id,
        MIN(agent_day_session_pageview_index) AS pv -- pageviews 
    FROM ch
    GROUP BY 1
)
-- Select only first pageviews
    SELECT  
            DATE_TRUNC(date, MONTH) AS date,
            ch.*
    FROM ch
    JOIN fp ON
    fp.session_id = ch.session_id
    AND fp.pv = ch.agent_day_session_pageview_index
    WHERE ch.object_id = 'HOME'
    -- GROUP BY 1
    ORDER BY 3,4,6,7



-- SECOND SCRIPT -- 

DECLARE start_date DATE;
DECLARE end_date DATE;

SET start_date = '2022-10-01';
SET end_date = '2022-10-31';



WITH all_traffic AS (
      SELECT 
            *
            , CONCAT(c.agent_id, '-', CAST(session_index AS STRING)) AS session_id

        FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c 
        WHERE date BETWEEN start_date AND end_date
        
),
subset AS ( -- get session_index of the users on mobile web > Direct/Search whose first PV is the WC HP
    SELECT *
        FROM `nyt-bigquery-beta-workspace.jose_data.session_id` 
        -- from    `nyt-bigquery-beta-workspace.jose_data.mobileweb_clicks`
),
clicks AS (
  SELECT  
    -- DATE_TRUNC(date(pg._pt), MONTH) AS date
    date(pg._pt) as date
--   , pg.combined_regi_id AS users
  , pg.pageview_id AS pageview_id
  , COUNT(int.module.element.name) AS clicks
FROM
  `nyt-eventtracker-prd.et.page` AS pg,
  unnest(interactions) AS int
WHERE
  1=1
  AND int.module.element.name LIKE '%outbound_product%'
  AND DATE(_pt) BETWEEN start_date AND end_date
  AND source_app LIKE '%wirecutter%'
GROUP BY 1,2
)

SELECT 
    a.date
    , COUNT(DISTINCT a.pageview_id) as pageviews 
    , COUNT (DISTINCT a.user_id) as users
    , SUM(COALESCE(c.clicks,0)) as subsequent_clicks

 FROM all_traffic as a
 JOIN subset as s on s.session_id = a.session_id
 LEFT JOIN clicks as c on c.pageview_id = a.pageview_id
 WHERE s.session_id = a.session_id AND a.agent_day_session_pageview_index > 1 -- making sure that these are subsequent pages during the same session 
       AND a.date = s.date_1 -- making sure that the date of the sessions overlap, usually a session resets after 30 min of inactivity 
GROUP BY 1
-- ORDER BY s.session_id, a.agent_day_session_pageview_index asc
ORDER BY date asc 