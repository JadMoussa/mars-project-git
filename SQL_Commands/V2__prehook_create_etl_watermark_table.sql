CREATE TABLE etl_watermark (
    id serial PRIMARY KEY,
    last_processed_date DATE
);