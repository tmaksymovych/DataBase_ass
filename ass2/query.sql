use P002;

SELECT 
    c.customerId,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    ci.email,
    ci.phone_number,
    
    COUNT(a.accountId) AS total_accounts,
    SUM(a.balance) AS total_balance,
    ROUND(AVG(a.balance), 2) AS avg_balance,
    
    CASE 
        WHEN SUM(a.balance) >= 5000 THEN 'Premium Client'
        WHEN SUM(a.balance) BETWEEN 1000 AND 4999 THEN 'Regular Client'
        ELSE 'Low Balance'
    END AS client_category
    
FROM clients c
JOIN clients_info ci ON c.customerId = ci.customerId
JOIN accounts a ON c.customerId = a.customerId
WHERE a.status = 'Active'
  AND a.open_date >= '2023-01-01'
GROUP BY c.customerId, c.first_name, c.last_name, ci.date_of_birth, ci.email, ci.phone_number
HAVING total_balance > 0
ORDER BY total_balance DESC;

--drop index idx_acc_balance on accounts;
--drop index idx_status_date on accounts;

create index idx_status_date on accounts (status, open_date);
create index idx_acc_balance on accounts (customerId, balance);


with stats as(
    SELECT
        customerId,
        COUNT(accountId) AS total_accounts,
        SUM(balance) AS total_balance,
        ROUND(AVG(balance), 2) AS avg_balance,
        CASE 
            WHEN SUM(balance) >= 5000 THEN 'Premium Client'
            WHEN SUM(balance) BETWEEN 1000 AND 4999 THEN 'Regular Client'
            ELSE 'Low Balance'
        END AS client_category
    FROM accounts
    WHERE status = 'Active'
      AND open_date >= '2023-01-01'
    GROUP BY customerId
)
SELECT
    c.customerId,
    concat(c.first_name, ' ', c.last_name) as full_name,
    ci.email,
    ci.phone_number,
    s.client_category,
    s.total_accounts,
    s.total_balance,
    s.avg_balance
FROM clients c
join clients_info ci on c.customerId = ci.customerId
join stats s on c.customerId = s.customerId
where s.total_balance > 0
order by total_balance desc
    