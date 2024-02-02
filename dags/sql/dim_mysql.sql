CREATE TABLE IF NOT EXISTS case_table (
    id INT ,
    status_name VARCHAR(255) NOT NULL,
    status_detail VARCHAR(255) NOT NULL
);


-- CREATE TABLE IF NOT EXISTS province_table (
--     province_id INT PRIMARY KEY,
--     province_name VARCHAR(255) NOT NULL
-- );

-- CREATE TABLE IF NOT EXISTS district_table (
--     district_id INT PRIMARY KEY,
--     province_id INT,
--     district_name VARCHAR(255) NOT NULL,
--     FOREIGN KEY (province_id) REFERENCES province_table(province_id)
-- );


CREATE TABLE IF NOT EXISTS province_table AS
select DISTINCT kode_prov, nama_prov  from raw_covid_data;

CREATE TABLE IF NOT EXISTS district_table AS
select distinct kode_kab, kode_prov , nama_kab from raw_covid_data;