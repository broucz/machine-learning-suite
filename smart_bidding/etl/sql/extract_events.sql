-- Using a Common Table Expression (CTE) to calculate 1% of the total row count for the given time range
WITH cnt AS (
    SELECT
        -- Using toUInt64 to cast the rounded result to an unsigned integer
        toUInt64(round(count(*) * {down_sampling_percentage})) AS limit_rows
    FROM
        dist_events
    -- Filtering records based on the date_time range
    WHERE
        date_time >= '{start_time}'
        AND date_time < '{end_time}'
)
-- Main query starts here
SELECT
    -- Selecting required fields
    date_time,
    country,
    idlanguage,
    region_geoname_id,
    city_geoname_id,
    iddevice,
    idbrowser,
    idos,
    idproxy,
    email_status,
    idadvertiser,
    rtb_inventory_id,
    idcampaign,
    idvariation,
    idadvertiser_ad_type,
    ad_type,
    idproduct_category,
    idpublisher,
    idsite,
    idcategory,
    idzone,
    sub,
    idtraffic_type,
    click_status,
    goal
FROM
    dist_events
-- Filtering records based on the date_time range
WHERE
    date_time >= '{start_time}'
    AND date_time < '{end_time}'
-- Randomly ordering the rows
ORDER BY
    rand()
-- Using the LIMIT calculated in the CTE, casting away any nulls
LIMIT
    assumeNotNull((SELECT limit_rows FROM cnt))
