-- here table is stock_prices

-- 1. Company Wise Daily Variation of Prices
SELECT company, date, (close - open) AS daily_variation FROM stock_prices ORDER BY company, date;

-- 2. Company Wise Daily Volume Change
SELECT company, date, volume FROM stock_prices ORDER BY company, date;

-- 3. Median Daily Variation
SELECT company, percentile_cont(0.5) WITHIN GROUP (ORDER BY (close - open)) AS median_variation FROM stock_prices GROUP BY company;



