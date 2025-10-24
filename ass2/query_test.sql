with ClientsInfo as(
    SELECT
        c.customerId,
        concat(c.first_name, ' ', c.last_name) as full_name,
        ci.email,
        ci.phone_number
    from clients c
    join clients_info ci on c.customerId = ci.customerId
),
BalanceInfo as(
    SELECT
        customerId,
        COUNT(accountId) AS total_accounts,
        SUM(balance) AS total_balance,
        ROUND(AVG(balance), 2) AS avg_balance
    from accounts
    group by customerId
),
ClientCat as(
    SELECT
    customerId,
    CASE 
        WHEN SUM(balance) >= 5000 THEN 'Premium Client'
        WHEN SUM(balance) BETWEEN 1000 AND 4999 THEN 'Regular Client'
        ELSE 'Low Balance'
    END AS client_category  
    from accounts
    group by customerId  
)
SELECT distinct -- use distinct because in where clause i use data from accounts, that is not used anywhere else
    ci.customerId,
    ci.full_name,
    ci.email,
    ci.phone_number,
    cc.client_category,
    bi.total_accounts,
    bi.total_balance,
    bi.avg_balance
FROM ClientsInfo ci
join BalanceInfo bi on ci.customerId = bi.customerId
join ClientCat cc on ci.customerId = cc.customerId
join accounts a on ci.customerId = a.customerId
WHERE a.status = 'Active'
  AND a.open_date >= '2023-01-01' and bi.total_balance > 0
order by total_balance desc;


WITH FilteredAccounts AS (
    SELECT 
        customerId, 
        accountId, 
        balance
    FROM accounts
    WHERE 
        status = 'Active'
      AND open_date >= '2023-01-01'
),
BalanceInfo AS (
    SELECT
        customerId,
        COUNT(accountId) AS total_accounts,
        SUM(balance) AS total_balance,
        ROUND(AVG(balance), 2) AS avg_balance
    FROM FilteredAccounts  -- <--- Using the small, filtered CTE
    GROUP BY customerId
),
ClientCat AS (
    SELECT
        customerId,
        CASE 
            WHEN total_balance >= 5000 THEN 'Premium Client'
            WHEN total_balance BETWEEN 1000 AND 4999.99 THEN 'Regular Client'
            ELSE 'Low Balance'
        END AS client_category
    FROM BalanceInfo  -- <--- Re-use the calculation!
    WHERE total_balance > 0 -- This is the 'HAVING' clause
),
ClientsInfo AS (
    SELECT
        c.customerId,
        concat(c.first_name, ' ', c.last_name) as full_name,
        ci.email,
        ci.phone_number
    from clients c
    join clients_info ci on c.customerId = ci.customerId
)
SELECT
    ci.customerId,
    ci.full_name,
    ci.email,
    ci.phone_number,
    cc.client_category,
    bi.total_accounts,
    bi.total_balance,
    bi.avg_balance
FROM ClientsInfo ci
JOIN BalanceInfo bi ON ci.customerId = bi.customerId
JOIN ClientCat cc ON ci.customerId = cc.customerId
ORDER BY total_balance DESC;