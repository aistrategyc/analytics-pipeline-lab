CREATE TABLE pipeline_state (
    source text PRIMARY KEY,
    watermark timestamptz NOT NULL,
    completed_at timestamptz NOT NULL
);
CREATE TABLE pipeline_runs (
    id uuid PRIMARY KEY,
    source text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    rows_read integer NOT NULL,
    revisions_added integer NOT NULL,
    facts_changed integer NOT NULL,
    completed_at timestamptz NOT NULL
);
CREATE TABLE raw_revisions (
    source text NOT NULL,
    record_id text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (source, record_id, source_updated_at)
);
CREATE TABLE ad_facts (
    source text NOT NULL,
    record_id text NOT NULL,
    day date NOT NULL,
    campaign_id text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency = 'PLN'),
    source_updated_at timestamptz NOT NULL,
    PRIMARY KEY (source, record_id),
    FOREIGN KEY (source, record_id, source_updated_at)
        REFERENCES raw_revisions(source, record_id, source_updated_at)
);
CREATE TABLE sale_facts (
    source text NOT NULL,
    record_id text NOT NULL,
    day date NOT NULL,
    campaign_id text NOT NULL,
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency = 'PLN'),
    source_updated_at timestamptz NOT NULL,
    PRIMARY KEY (source, record_id),
    FOREIGN KEY (source, record_id, source_updated_at)
        REFERENCES raw_revisions(source, record_id, source_updated_at)
);
CREATE INDEX ad_reporting ON ad_facts(day, campaign_id);
CREATE INDEX sale_reporting ON sale_facts(day, campaign_id);

-- Aggregate each side to the same grain BEFORE joining to avoid fan-out.
CREATE VIEW daily_metrics AS
WITH spend AS (
    SELECT day, campaign_id, sum(amount_minor) AS spend_minor
    FROM ad_facts GROUP BY day, campaign_id
), sales AS (
    SELECT day, campaign_id, sum(amount_minor) AS revenue_minor, count(*) AS orders
    FROM sale_facts GROUP BY day, campaign_id
)
SELECT coalesce(a.day, s.day) AS day,
       coalesce(a.campaign_id, s.campaign_id) AS campaign_id,
       coalesce(a.spend_minor, 0) AS spend_minor,
       coalesce(s.revenue_minor, 0) AS revenue_minor,
       coalesce(s.orders, 0) AS orders,
       round(coalesce(s.revenue_minor, 0) / nullif(a.spend_minor, 0), 4) AS roas
FROM spend a FULL OUTER JOIN sales s USING (day, campaign_id);
