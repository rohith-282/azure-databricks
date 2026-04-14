-- =============================================================
-- Stored Procedures — Incremental Load Helpers
-- =============================================================

-- Upsert daily sales (MERGE pattern)
CREATE OR ALTER PROCEDURE dbo.usp_upsert_daily_sales
    @sales_date         DATE,
    @total_orders       INT,
    @total_revenue      DECIMAL(18,2),
    @avg_order_value    DECIMAL(18,2),
    @max_order_value    DECIMAL(18,2),
    @unique_customers   INT
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.daily_sales AS target
    USING (SELECT
        @sales_date         AS sales_date,
        @total_orders       AS total_orders,
        @total_revenue      AS total_revenue,
        @avg_order_value    AS avg_order_value,
        @max_order_value    AS max_order_value,
        @unique_customers   AS unique_customers
    ) AS source
    ON target.sales_date = source.sales_date

    WHEN MATCHED THEN UPDATE SET
        target.total_orders     = source.total_orders,
        target.total_revenue    = source.total_revenue,
        target.avg_order_value  = source.avg_order_value,
        target.max_order_value  = source.max_order_value,
        target.unique_customers = source.unique_customers,
        target._gold_timestamp  = GETUTCDATE()

    WHEN NOT MATCHED THEN INSERT (
        sales_date, total_orders, total_revenue,
        avg_order_value, max_order_value, unique_customers
    ) VALUES (
        source.sales_date, source.total_orders, source.total_revenue,
        source.avg_order_value, source.max_order_value, source.unique_customers
    );
END;

GO

-- Log pipeline run result
CREATE OR ALTER PROCEDURE dbo.usp_log_pipeline_run
    @pipeline_name      NVARCHAR(200),
    @layer              NVARCHAR(20),
    @entity             NVARCHAR(100),
    @run_start          DATETIME2,
    @run_end            DATETIME2,
    @status             NVARCHAR(20),
    @rows_processed     BIGINT          = NULL,
    @error_message      NVARCHAR(MAX)   = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.pipeline_audit_log (
        pipeline_name, layer, entity,
        run_start, run_end, status,
        rows_processed, error_message
    ) VALUES (
        @pipeline_name, @layer, @entity,
        @run_start, @run_end, @status,
        @rows_processed, @error_message
    );
END;

GO
