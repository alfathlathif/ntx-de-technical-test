-- Test Case 1: Channel Analysis
-- Explanation: From the question, I conclude that the task is to obtain the top 5 countries with the highest revenue for each channel grouping. 
-- To achieve this, I first performed a subquery to sum the totalTransactionRevenue column for each country and channel grouping. Then, I applied 
-- ranking to determine the order from the highest to the lowest revenue. This process uses row number and partition by. After that, another query 
-- is executed to select the countries with a rank less than or equal to 5 for each channel grouping.
-- SELECT country, channelGrouping, total_revenue
FROM (
    SELECT 
        country, 
        channelGrouping, 
        SUM(totalTransactionRevenue) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY channelGrouping ORDER BY SUM(totalTransactionRevenue) DESC) AS row_num
    FROM ecommerce_sessions
    GROUP BY country, channelGrouping
) AS ranked_data
WHERE row_num <= 5
ORDER BY channelGrouping, total_revenue DESC;


-- Test Case 2: User Behavior Analysis
-- Explanation: Based on the question, I will search for fullVisitorId where the average timeOnSite is higher than the overall average and the average 
-- pageviews are lower than the overall average. I use two subqueries to solve this case. The first subquery calculates the overall average of timeOnSite 
-- and pageviews. The second subquery calculates the average timeOnSite, pageviews, and sessionQualityDim for each fullVisitorId. After that, a query 
-- is performed combining both subqueries with the condition that the average timeOnSite is greater than the overall average and the average pageviews 
-- are lower than the overall average.
WITH avg_metrics AS (
    SELECT 
        AVG(timeOnSite) AS avg_timeOnSite, 
        AVG(pageviews) AS avg_pageviews
    FROM ecommerce_sessions
),
user_metrics AS (
    SELECT 
        fullVisitorId,
        AVG(timeOnSite) AS avg_user_timeOnSite,
        AVG(pageviews) AS avg_user_pageviews,
		AVG(sessionQualityDim) AS avg_user_sessionQualityDim 
    FROM ecommerce_sessions
    GROUP BY fullVisitorId
)
SELECT 
    fullVisitorId, 
    avg_user_timeOnSite, 
    avg_user_pageviews,
	avg_user_sessionQualityDim
FROM user_metrics, avg_metrics
WHERE avg_user_timeOnSite > avg_metrics.avg_timeOnSite
  AND avg_user_pageviews < avg_metrics.avg_pageviews;

-- Test Case 3: Product Performance
-- Explaination: While working on this case, I encountered an issue where all the necessary fields—itemRevenue, itemQuantity, and 
-- productRefundAmount—were empty in the provided data. Therefore, I created the query syntax without validation. Based on the given 
-- requirements, I need to sum itemRevenue, itemQuantity, and productRefundAmount, and calculate net_revenue by subtracting productRefundAmount 
-- from itemRevenue for each v2ProductName. Then, I added a condition where if productRefundAmount exceeds 10 percent of itemRevenue, 
-- the product will be marked as "Flagged." If the condition is not met, it will be marked as "Not Flagged." Finally, the query results 
-- are sorted from the highest to the lowest net revenue.
SELECT 
    v2ProductName,
    SUM(itemRevenue) AS total_revenue,
    SUM(itemQuantity) AS total_quantity_sold,
    SUM(productRefundAmount) AS total_refund,
    (SUM(itemRevenue) - SUM(productRefundAmount)) AS net_revenue,
    CASE 
        WHEN SUM(productRefundAmount) > (0.10 * SUM(itemRevenue)) THEN 'Flagged'
        ELSE 'Not Flagged'
    END AS refund_flag
FROM ecommerce_sessions
GROUP BY v2ProductName
ORDER BY net_revenue DESC;
