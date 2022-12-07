-- select * from `nyt-wccomposer-prd.wc_data_core.airtable_active_content` AS ac

DECLARE start_date DATE;
DECLARE end_date DATE;

SET start_date = '2022-05-01';
SET end_date = '2022-05-31';

WITH ch AS
(
    SELECT c.*,
           w.object_id,
    CONCAT(c.agent_id, '-', CAST(session_index AS STRING)) AS session_id -- Create session_id
    -- , user_type_1
    -- , user_type_2
    -- , user_type_3
    FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel` c
    LEFT JOIN `nyt-bigquery-beta-workspace.stuart_data.wc_devices` w ON w.pageview_id = c.pageview_id AND w.date = c.date
    LEFT JOIN `nyt-bigquery-beta-workspace.wirecutter_data.user_type`  ut ON ut.pageview_id = c.pageview_id
    WHERE c.date BETWEEN start_date AND end_date 
    -- AND (session_channel_2 = 'Direct' OR session_channel_2 = 'Organic Search') 
    AND session_channel_2 = 'Direct'
    AND w.device = 'Mobile'
),
fp AS -- Get first pageview for each session
(
    SELECT
        session_id,
        MIN(agent_day_session_pageview_index) AS pv
    FROM ch
    GROUP BY 1
), -- 8,669,803

page_type AS (
    SELECT DISTINCT
        DATE(_pt) AS date,
        pg.combined_regi_id,
        pg.wirecutter.asset.id AS object_id,
        pageview_id,
        wirecutter.asset.headline AS page_title,
        CASE 
            when left(wirecutter.asset.id,4) = 'BLOG' then 'Blog'
            when left(wirecutter.asset.id,4) = 'HOME' then 'Site Nav'
            when left(wirecutter.asset.id,6) = 'SEARCH' then 'Site Nav'
            when left(wirecutter.asset.id,3) in ('403','404') then 'Site Nav'
            when left(wirecutter.asset.id,2) = 'AU' then 'Site Nav'
            when wirecutter.asset.id = 'ALL' then 'Site Nav'
            when left(wirecutter.asset.id,2) = 'PO' then 'Blog'
            when left(wirecutter.asset.id,2) = 'LI' then 'List'
            when left(wirecutter.asset.id,2) = 'BG' then 'Buying Guide'
            when left(wirecutter.asset.id,2) = 'SE' then 'Site Nav'
            when left(wirecutter.asset.id,2) = 'AS' then 'Site Nav'
            when wirecutter.asset.id = 'ED1256' then 'Deals'
            when left(wirecutter.asset.id,2) in ('RE' -- 1. Standard
                , 'SJ' -- 2. Subjective
                , 'TP' -- 3. Topic Page
                , 'TT' -- Topic Page
                , 'SI' -- 4. Single
                , 'CL' -- 5. Collection or Collective
                , 'HT' -- 6. How To
                , 'BG' -- 7. Buying Guide
                , 'SP' -- 8. Staff Pick
                , 'LT') -- 9. Listicle) 
                then 'Review'
            else 'Other'
        END page_type

    FROM `nyt-eventtracker-prd.et.page` AS pg    
    WHERE
        DATE(_pt) BETWEEN start_date AND end_date
        AND source_app LIKE '%wirecutter%'

),

category AS (
     SELECT
    DATE(pg._pt) date
  ,  pg.combined_regi_id
  , pg.wirecutter.asset.id AS object_id
  , pg.pageview_id
  , ac.category as category

  FROM `nyt-eventtracker-prd.et.page` AS pg 

  LEFT JOIN `nyt-wccomposer-prd.wc_data_core.airtable_active_content` AS ac -- Joining category table 
    ON ac.post_id = SUBSTR(pg.wirecutter.asset.id, 3)   
    WHERE
        DATE(_pt) BETWEEN start_date AND end_date
        AND source_app LIKE '%wirecutter%'

)

-- Select only first pageviews
    SELECT  
            DATE_TRUNC(ch.date, MONTH) AS date,
            COUNT(DISTINCT user_id) AS user_count,
            page_type,
            c.category
    FROM ch
    JOIN fp ON  fp.session_id = ch.session_id
    LEFT JOIN category AS c ON c.pageview_id = ch.pageview_id
    LEFT JOIN page_type AS pt ON pt.pageview_id = ch.pageview_id
    AND fp.pv = ch.agent_day_session_pageview_index
    WHERE ch.object_id <> 'HOME'
    GROUP BY 1,3,4
    ORDER BY 1 ASC
    ;


