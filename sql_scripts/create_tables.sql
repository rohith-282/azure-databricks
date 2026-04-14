-- =============================================================
-- Azure SQL Database — Schema Setup
-- Project: Azure End-to-End Data Engineering Pipeline
-- Author:  Rohith R
-- =============================================================

-- Daily Sales Summary
CREATE TABLE dbo.daily_sales (
    sales_date          DATE            NOT NULL,
    total_orders        INT             NOT NULL,
    total_revenue       DECIMAL(18, 2)  NOT NULL,
    avg_order_value     DECIMAL(18, 2)  NOT NULL,
    max_order_value     DECIMAL(18, 2)  NOT NULL,
    unique_customers    INT             NOT NULL,
    _gold_timestamp     DATETIME2       DEFAULT GETUTCDATE(),
    CONSTRAINT PK_daily_sales PRIMARY KEY (sales_date)
);

GO

-- Customer Lifetime Summary
CREATE TABLE dbo.customer_summary (
    customer_id         NVARCHAR(50)    NOT NULL,
    total_orders        INT             NOT NULL,
    lifetime_value      DECIMAL(18, 2)  NOT NULL,
    avg_order_value     DECIMAL(18, 2)  NOT NULL,
    last_order_date     DATE,
    first_order_date    DATE,
    _gold_timestamp     DATETIME2       DEFAULT GETUTCDATE(),
    CONSTRAINT PK_customer_summary PRIMARY KEY (customer_id)
);

GO

-- Event Funnel Counts
CREATE TABLE dbo.event_funnel (
    event_type          NVARCHAR(100)   NOT NULL,
    event_count         BIGINT          NOT NULL,
    unique_users        INT             NOT NULL,
    _gold_timestamp     DATETIME2       DEFAULT GETUTCDATE(),
    CONSTRAINT PK_event_funnel PRIMARY KEY (event_type)
);

GO

-- Pipeline Run Audit Log
CREATE TABLE dbo.pipeline_audit_log (
    log_id              INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_name       NVARCHAR(200)   NOT NULL,
    layer               NVARCHAR(20)    NOT NULL,  -- bronze / silver / gold
    entity              NVARCHAR(100)   NOT NULL,
    run_start           DATETIME2       NOT NULL,
    run_end             DATETIME2,
    status              NVARCHAR(20),              -- SUCCESS / FAILED
    rows_processed      BIGINT,
    error_message       NVARCHAR(MAX),
    created_at          DATETIME2       DEFAULT GETUTCDATE()
);

GO

-- Indexes for Power BI query performance
CREATE NONCLUSTERED INDEX IX_daily_sales_date
    ON dbo.daily_sales (sales_date DESC);

CREATE NONCLUSTERED INDEX IX_customer_ltv
    ON dbo.customer_summary (lifetime_value DESC);

CREATE NONCLUSTERED INDEX IX_event_funnel_count
    ON dbo.event_funnel (event_count DESC);
