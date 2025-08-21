-- 1. How many food providers and receivers are there in each city?
SELECT City, COUNT(*) AS Providers FROM providers GROUP BY City ORDER BY Providers DESC;

SELECT City, COUNT(*) AS Receivers FROM receivers GROUP BY City ORDER BY Receivers DESC;

-- 2. Which type of food provider contributes the most food (by total quantity available)?
SELECT fl.Provider_Type, SUM(COALESCE(fl.Quantity,0)) AS Total_Quantity
FROM food_listings fl
GROUP BY fl.Provider_Type
ORDER BY Total_Quantity DESC;

-- 3. Contact information of food providers in a specific city (param: :city)
SELECT Name, Type, Address, Contact
FROM providers
WHERE City = :city
ORDER BY Name;

-- 4. Which receivers have claimed the most food (by count of claims)?
SELECT r.Receiver_ID, r.Name, COUNT(*) AS Claim_Count
FROM claims c
JOIN receivers r ON r.Receiver_ID = c.Receiver_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY Claim_Count DESC;

-- 5. Total quantity of food available from all providers
SELECT SUM(COALESCE(Quantity,0)) AS Total_Quantity_Available
FROM food_listings;

-- 6. Which city has the highest number of food listings?
SELECT Location AS City, COUNT(*) AS Listings
FROM food_listings
GROUP BY Location
ORDER BY Listings DESC;

-- 7. Most commonly available food types
SELECT Food_Type, COUNT(*) AS Count_Listings
FROM food_listings
GROUP BY Food_Type
ORDER BY Count_Listings DESC;

-- 8. How many food claims have been made for each food item?
SELECT fl.Food_ID, fl.Food_Name, COUNT(c.Claim_ID) AS Claim_Count
FROM food_listings fl
LEFT JOIN claims c ON c.Food_ID = fl.Food_ID
GROUP BY fl.Food_ID, fl.Food_Name
ORDER BY Claim_Count DESC;

-- 9. Which provider has had the highest number of successful (Completed) food claims?
SELECT p.Provider_ID, p.Name, COUNT(*) AS Completed_Claims
FROM claims c
JOIN food_listings fl ON fl.Food_ID = c.Food_ID
JOIN providers p ON p.Provider_ID = fl.Provider_ID
WHERE c.Status = 'Completed'
GROUP BY p.Provider_ID, p.Name
ORDER BY Completed_Claims DESC;

-- 10. Percentage split of claim statuses
WITH totals AS (
  SELECT COUNT(*) AS total FROM claims
),
status_counts AS (
  SELECT Status, COUNT(*) AS cnt FROM claims GROUP BY Status
)
SELECT sc.Status,
       sc.cnt AS Count,
       ROUND(100.0 * sc.cnt / NULLIF(t.total,0), 2) AS Percentage
FROM status_counts sc, totals t
ORDER BY Percentage DESC;

-- 11. Average quantity of food claimed per receiver (assumes each claim claims the full quantity of the item at claim time)
-- If you store per-claim quantity, modify accordingly.
SELECT r.Receiver_ID, r.Name,
       ROUND(AVG(COALESCE(fl.Quantity,0)),2) AS Avg_Quantity_Claimed
FROM claims c
JOIN receivers r ON r.Receiver_ID = c.Receiver_ID
JOIN food_listings fl ON fl.Food_ID = c.Food_ID
GROUP BY r.Receiver_ID, r.Name
ORDER BY Avg_Quantity_Claimed DESC;

-- 12. Which meal type is claimed the most?
SELECT fl.Meal_Type, COUNT(*) AS Claim_Count
FROM claims c
JOIN food_listings fl ON fl.Food_ID = c.Food_ID
GROUP BY fl.Meal_Type
ORDER BY Claim_Count DESC;

-- 13. Total quantity of food donated by each provider
SELECT p.Provider_ID, p.Name, SUM(COALESCE(fl.Quantity,0)) AS Total_Donated_Quantity
FROM food_listings fl
JOIN providers p ON p.Provider_ID = fl.Provider_ID
GROUP BY p.Provider_ID, p.Name
ORDER BY Total_Donated_Quantity DESC;

-- 14. Listings expiring within the next N days (param: :days)
SELECT * FROM food_listings
WHERE DATE(Expiry_Date) <= DATE('now', '+' || :days || ' day')
ORDER BY Expiry_Date;

-- 15. Claim conversion rate by provider (Completed claims / total listings linked to provider)
WITH provider_listings AS (
  SELECT Provider_ID, COUNT(*) AS listings
  FROM food_listings GROUP BY Provider_ID
),
provider_completed_claims AS (
  SELECT fl.Provider_ID, COUNT(*) AS completed_claims
  FROM claims c JOIN food_listings fl ON fl.Food_ID = c.Food_ID
  WHERE c.Status = 'Completed'
  GROUP BY fl.Provider_ID
)
SELECT p.Provider_ID, p.Name,
       COALESCE(pcc.completed_claims,0) AS Completed_Claims,
       COALESCE(pl.listings,0) AS Listings,
       ROUND(100.0*COALESCE(pcc.completed_claims,0)/NULLIF(pl.listings,0),2) AS Conversion_Rate
FROM providers p
LEFT JOIN provider_listings pl ON pl.Provider_ID = p.Provider_ID
LEFT JOIN provider_completed_claims pcc ON pcc.Provider_ID = p.Provider_ID
ORDER BY Conversion_Rate DESC;

-- 16. Top cities by completed claims
SELECT fl.Location AS City, COUNT(*) AS Completed_Claims
FROM claims c JOIN food_listings fl ON fl.Food_ID = c.Food_ID
WHERE c.Status = 'Completed'
GROUP BY fl.Location
ORDER BY Completed_Claims DESC;

-- 17. Daily trend of claims in the last 30 days
SELECT DATE(Timestamp) AS Day, COUNT(*) AS Claims
FROM claims
WHERE DATE(Timestamp) >= DATE('now','-30 day')
GROUP BY DATE(Timestamp)
ORDER BY Day;

-- 18. Unclaimed listings (no claims yet)
SELECT fl.*
FROM food_listings fl
LEFT JOIN claims c ON c.Food_ID = fl.Food_ID
WHERE c.Claim_ID IS NULL
ORDER BY fl.Expiry_Date;
