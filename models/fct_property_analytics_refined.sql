-- Refined Analytics Model based on old_data.json
SELECT
    City,
    Property_Type,
    Asset_Class,
    COUNT(*) AS Property_Count,
    AVG(Price_INR) AS Avg_Price_INR,
    MIN(Price_INR) AS Min_Price_INR,
    MAX(Price_INR) AS Max_Price_INR,
    AVG(Area_SqFt) AS Avg_Area_SqFt,
    AVG(Bedrooms) AS Avg_Bedrooms
FROM stg_property
GROUP BY
    City,
    Property_Type,
    Asset_Class
ORDER BY
    City,
    Property_Type;
