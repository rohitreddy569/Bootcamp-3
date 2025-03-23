BOOT CAMP PROJECT 3 
Customer 360 Data Integration

Overview
A retail business wants to build a unified Customer 360 view by integrating data from multiple sources, including online transactions, in-store purchases, customer service interactions, and loyalty programs. This project uses a mix of fact and dimension tables to ensure a clean, scalable structure.
Architecture
<img width="468" alt="image" src="https://github.com/user-attachments/assets/bd59637a-09e1-4e34-95ee-2c80d91c59c8" />

 
Role:
My role in this project as an ETL developer where I create the external tables in synapse analytics, initially my data is lying in ADLS Gen 2. Using Dedicated synapse pool, I need to clean the data on the fly and corrected data needs to be stored in synapse data warehouse.
To maintain this architecture I have created two dedicated pools one with staging dB and other with Analytica DB. Staging will get the raw from ADLS and Analytical DB will have the curated data. From this curated data I will be responsible to the other teams if there is any issue with accessing the data from Dedicated pool.
In this project we are using limited data, so My company encourages to use Synapse as a DW even though it is a bit costlier compared to the ADLS Gen 2.


STEP 1: Creating the Dedicated pools in synapse analytics 
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/d23371c2-6394-408d-a967-a6819f434895" />

Created an external table with the storage account attaching to it and using external tables, I can be able to access the data from ADLS Gen 2
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/f0361170-bb88-4381-a196-275c0e7858e9" />


Now, I can able to see my data in the synapse, The external tables acts as a structure to hold the data temporarily, so I need to get the data into dedicated warehouse. 
But before I need to get the into Synapse, I have to curate the data as per the business requirements.

Below is the logic as suggested by Business(Example)
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/c1d5752e-30b0-4335-849f-c29972189989" />


No I can able to transfer the curated data into the synapse and the data is stored.
 
<img width="468" alt="image" src="https://github.com/user-attachments/assets/513d5a0d-3dbf-499c-bde3-b642912291ee" />

 
 
STEP 2: My company has now decided that the data is huge and need to implement a cost effective way to create the data and make it available for other teams.
This can be achieved in the simple way by copy activity.
Below are the steps and Logic used:


Using Meta data and copy data activity I have transferred all the raw files to the Azure sql data base where we can see the further logic to curate the data.
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/ae292bf8-8b1d-43dd-aba9-fac14c243a8a" />

Now all the raw data is moved to the Azure sql DB and can be used for further logic
 
<img width="468" alt="image" src="https://github.com/user-attachments/assets/67929476-210e-485b-9d75-ac7cdebba817" />

1.To Check the Agen t performance 
The logic is here 
-- Step 1: Create the table if not already created (only needs to be run once)
CREATE TABLE AgentPerformance (
    AgentID INT PRIMARY KEY,
    AgentName NVARCHAR(255),
    TotalInteractions INT,
    ResolvedInteractions INT,
    ResolutionSuccessRate DECIMAL(5, 2)  -- Percentage with two decimal places
);

-- Step 2: Calculate the number of interactions and resolution success rate, then insert the result
WITH AgentStats AS (
    SELECT 
        A.AgentID,
        A.Name AS AgentName,
        COUNT(T.InteractionID) AS TotalInteractions,  -- Total interactions handled by the agent
        SUM(CASE WHEN T.ResolutionStatus = 'Resolved' THEN 1 ELSE 0 END) AS ResolvedInteractions  -- Count only resolved cases
    FROM Interactions T
    INNER JOIN Agents A ON T.AgentID = A.AgentID
    GROUP BY A.AgentID, A.Name
)
-- Insert the results into the AgentPerformance table
INSERT INTO AgentPerformance (AgentID, AgentName, TotalInteractions, ResolvedInteractions, ResolutionSuccessRate)
SELECT 
    AgentID,
    AgentName,
    TotalInteractions,
    ResolvedInteractions,
    CASE 
        WHEN TotalInteractions = 0 THEN 0  -- Avoid division by zero
        ELSE (CAST(ResolvedInteractions AS DECIMAL(5,2)) / TotalInteractions) * 100  -- Calculate success rate as percentage
    END AS ResolutionSuccessRate
FROM AgentStats;

-- Verify that the data was inserted
SELECT * FROM AgentPerformance;
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/6c859350-52a7-4fa5-bb32-35e9f8b817ba" />


2.To Analyze DateTime to find peak days and times in-store vs. online
Below is the logic 

CREATE TABLE PeakActivityAnalysis (
    TransactionType VARCHAR(50),  -- To store 'Online' or other types
    DayOfWeek VARCHAR(20),       -- To store day of the week
    HourOfDay INT,               -- To store the hour of the day
    TransactionCount INT         -- To store the count of transactions
);
INSERT INTO PeakActivityAnalysis
SELECT 
    'Online' AS TransactionType,
    DATENAME(WEEKDAY, DateTime) AS DayOfWeek,
    DATEPART(HOUR, DateTime) AS HourOfDay,
    COUNT(*) AS TransactionCount
FROM OnlineTransactions
WHERE DateTime IS NOT NULL  -- Null value cleanup
GROUP BY DATENAME(WEEKDAY, DateTime), DATEPART(HOUR, DateTime);
 <img width="468" alt="image" src="https://github.com/user-attachments/assets/efc2f7e2-897d-4ece-a8ed-0873fe8d7a21" />



3. Table 1 -  for Average Order Value (AOV)
SUM(Amount) / COUNT(OrderID) per product, category, and location.


CREATE TABLE AverageOrderValue (
    ProductID INT,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Location VARCHAR(100),
    TotalAmount DECIMAL(18, 2),
    OrderCount INT,
    AverageOrderValue DECIMAL(18, 2)
);

INSERT INTO AverageOrderValue
SELECT 
    O.ProductID,  -- Only use O.ProductID if InStoreTransactions doesn't have it
    COALESCE(P.Name, 'Unknown') AS ProductName,  
    COALESCE(P.Category, 'Unknown') AS Category,  
    COALESCE(S.Location, 'Online') AS Location,  
    SUM(COALESCE(O.Amount, I.Amount)) AS TotalAmount,
    COUNT(COALESCE(O.OrderID, I.TransactionID)) AS OrderCount,
    SUM(COALESCE(O.Amount, I.Amount)) / NULLIF(COUNT(COALESCE(O.OrderID, I.TransactionID)), 0) AS AverageOrderValue
FROM OnlineTransactions O
LEFT JOIN Products P ON O.ProductID = P.ProductID  -- Only join on OnlineTransactions
LEFT JOIN InStoreTransactions I ON O.CustomerID = I.CustomerID  
LEFT JOIN Stores S ON I.StoreID = S.StoreID
WHERE COALESCE(O.Amount, I.Amount) IS NOT NULL  
GROUP BY 
    O.ProductID,  
    COALESCE(P.Name, 'Unknown'), 
    COALESCE(P.Category, 'Unknown'), 
    COALESCE(S.Location, 'Online');
<img width="468" alt="image" src="https://github.com/user-attachments/assets/7f4a3792-d496-4e0b-ba31-bcfcfa47c6a0" />
 
Table  -  for Segment customers based on total spend, purchase frequency, and loyalty tier (LoyaltyAccounts.TierLevel).
Example: "High-Value Customers" (Top 10% spenders), "One-Time Buyers," "Loyalty Champions."

WITH CustomerStats AS (
    SELECT 
        C.CustomerID,
        C.Name,
        COALESCE(L.TierLevel, 'No Tier') AS TierLevel,
        SUM(CAST(T.Amount AS DECIMAL(18,2))) AS TotalSpend,
        COUNT(*) AS PurchaseFrequency
    FROM OnlineTransactions T
    INNER JOIN Customers C ON T.CustomerID = C.CustomerID
    LEFT JOIN LoyaltyAccounts L ON C.CustomerID = L.CustomerID
    GROUP BY C.CustomerID, C.Name, L.TierLevel
),
RankedCustomers AS (  
    SELECT 
        CustomerID,
        Name,
        TierLevel,
        TotalSpend,
        PurchaseFrequency,
        NTILE(10) OVER (ORDER BY TotalSpend DESC) AS SpendPercentile,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY TotalSpend DESC) AS RowNum
    FROM CustomerStats
)
-- Perform the MERGE using the RankedCustomers CTE
MERGE INTO CustomerSegmentation AS Target
USING (
    SELECT 
        CustomerID,
        Name,
        TierLevel,
        TotalSpend,
        PurchaseFrequency,
        SpendPercentile,
        CASE 
            WHEN SpendPercentile = 1 THEN 'High-Value Customer'
            WHEN PurchaseFrequency = 1 THEN 'One-Time Buyer'
            WHEN TierLevel IN ('Gold', 'Platinum') AND PurchaseFrequency > 5 THEN 'Loyalty Champion'
            WHEN TotalSpend > 500 AND PurchaseFrequency > 3 THEN 'Frequent Shopper'
            ELSE 'Regular Customer'
        END AS CustomerSegment
    FROM RankedCustomers
    WHERE RowNum = 1  -- Ensure only one row per CustomerID
) AS Source
ON Target.CustomerID = Source.CustomerID
WHEN MATCHED THEN
    UPDATE SET 
        Target.Name = Source.Name,
        Target.TierLevel = Source.TierLevel,
        Target.TotalSpend = Source.TotalSpend,
        Target.PurchaseFrequency = Source.PurchaseFrequency,
        Target.SpendPercentile = Source.SpendPercentile,
        Target.CustomerSegment = Source.CustomerSegment
WHEN NOT MATCHED THEN
    INSERT (CustomerID, Name, TierLevel, TotalSpend, PurchaseFrequency, SpendPercentile, CustomerSegment)
    VALUES (Source.CustomerID, Source.Name, Source.TierLevel, Source.TotalSpend, Source.PurchaseFrequency, Source.SpendPercentile, Source.CustomerSegment);
 
<img width="468" alt="image" src="https://github.com/user-attachments/assets/c171bc93-1d62-412c-8958-ef8313240599" />

NOTE: Due to system issues I cannot be able to view the data in Power BI, whichi have already discussed with Varun
![image](https://github.com/user-attachments/assets/7945a2ee-7dde-4a24-a87e-0dd2c3785544)
