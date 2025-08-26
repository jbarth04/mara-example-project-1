DROP TABLE IF EXISTS m_dim_next.lead CASCADE;
CREATE TABLE m_dim_next.lead
(
    lead_id                             TEXT                              NOT NULL PRIMARY KEY,
    seller_fk                           TEXT,
    sales_development_representative_id TEXT,
    sales_representative_id             TEXT,

    deal_date                           TIMESTAMP WITH TIME ZONE,

    business_segment                    TEXT                              NOT NULL,
    lead_type                           TEXT                              NOT NULL,
    lead_behaviour_profile              TEXT                              NOT NULL,
    average_stock                       TEXT                              NOT NULL,
    business_type                       TEXT                              NOT NULL,

    declared_product_catalog_size       DOUBLE PRECISION,
    declared_monthly_revenue            DOUBLE PRECISION,

    is_closed_deal                      TEXT                              NOT NULL,

    first_contact_date                  TIMESTAMP WITH TIME ZONE          NOT NULL,
    landing_page_id                     TEXT                              NOT NULL,
    advertising_channel                 TEXT                              NOT NULL,

    number_of_orders_lifetime           INTEGER,
    revenue_lifetime                    DOUBLE PRECISION,
    days_to_closing_deal                INTEGER
);

INSERT INTO m_dim_next.lead
SELECT lead_id                                                        AS lead_id,
       seller.seller_id                                               AS seller_fk,
       sales_development_representative_id                            AS sales_development_representative_id,
       sales_representative_id                                        AS sales_representative_id,

       deal_date                                                      AS deal_date,

       COALESCE(business_segment, 'Unknown')                          AS business_segment,
       COALESCE(lead_type, 'Unknown')                                 AS lead_type,
       COALESCE(lead_behaviour_profile, 'Unknown')                    AS lead_behaviour_profile,

       COALESCE(average_stock, 'Unknown')                             AS average_stock,
       COALESCE(business_type, 'Unknown')                             AS business_type,

       coalesce(declared_product_catalog_size, 0)                     AS declared_product_catalog_size,
       coalesce(declared_monthly_revenue, 0)                          AS declared_monthly_revenue,

       CASE
           WHEN lead.deal_date IS NOT NULL
               THEN 'Is closed deal'
           ELSE 'Is not closed deal' END                              AS is_closed_deal,

       first_contact_date                                             AS first_contact_date,
       landing_page_id                                                AS landing_page_id,
       advertising_channel                                            AS advertising_channel,

       seller.number_of_orders_lifetime                               AS number_of_orders_lifetime,
       seller.revenue_lifetime                                        AS revenue_lifetime,
       days_to_closing_deal                                           AS days_to_closing_deal
FROM m_tmp.lead
         LEFT JOIN ec_dim.seller USING (seller_id);
