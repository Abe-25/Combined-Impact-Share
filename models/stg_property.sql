-- Refined staging model based on old_data.json schema
SELECT
Property_ID,
City,
Property_Type,
Asset_Class,
Area_SqFt,
Price_INR,
Status,
Bedrooms,
Bathrooms
FROM PROPERTY_MASTER;