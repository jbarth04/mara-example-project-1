DROP TABLE IF EXISTS m_tmp.lead CASCADE;
CREATE TABLE m_tmp.lead
(
    lead_id                             TEXT                     NOT NULL,
    seller_id                           TEXT,
    sales_development_representative_id TEXT,
    sales_representative_id             TEXT,

    deal_date                           TIMESTAMP WITH TIME ZONE,
    business_segment                    TEXT,
    lead_type                           TEXT,
    lead_behaviour_profile              TEXT,
    average_stock                       TEXT,
    business_type                       TEXT,

    declared_product_catalog_size       DOUBLE PRECISION,
    declared_monthly_revenue            DOUBLE PRECISION,

    first_contact_date                  TIMESTAMP WITH TIME ZONE NOT NULL,
    landing_page_id                     TEXT                     NOT NULL,
    advertising_channel                 TEXT                     NOT NULL,
    days_to_closing_deal                INTEGER
);

INSERT INTO m_tmp.lead
SELECT mql_id                                    AS lead_id,
       seller_id,
       sdr_id                                    AS sales_development_representative_id,
       sr_id                                     AS sales_representative_id,

       won_date                                  AS deal_date,
       business_segment                          AS business_segment,
       lead_type                                 AS lead_type,
       lead_behaviour_profile                    AS lead_behaviour_profile,
       average_stock                             AS average_stock,
       business_type                             AS business_type,
       declared_product_catalog_size,
       declared_monthly_revenue,
       first_contact_date                        AS first_contact_date,
       landing_page_id,
       COALESCE(origin, 'Unknown')               AS advertising_channel,
       won_date::DATE - first_contact_date::DATE AS days_to_closing_deal
FROM m_data.marketing_qualified_lead
         LEFT JOIN m_data.closed_deal USING (mql_id);
