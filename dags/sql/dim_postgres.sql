-- case table
DROP TABLE IF EXISTS case_table;
CREATE TABLE IF NOT EXISTS case_table (
    id INTEGER,
    status_name VARCHAR(255) NOT NULL,
    status_detail VARCHAR(255) NOT NULL
);


-- province TABLE
DROP TABLE IF EXISTS province_table;
CREATE TABLE IF NOT EXISTS province_table (
    province_id INTEGER PRIMARY KEY,
    province_name VARCHAR(255) NOT NULL
);


-- district TABLE
DROP TABLE IF EXISTS district_table;
CREATE TABLE IF NOT EXISTS district_table (
    district_id INTEGER PRIMARY KEY,
    province_id INTEGER,
    district_name VARCHAR(255) NOT NULL
);


-- province daily table
DROP TABLE IF EXISTS daily_province;
CREATE TABLE IF NOT EXISTS daily_province (
    id INT PRIMARY KEY,
    province_id INT,
    case_id INT,
    date DATE,
    total INT
);

-- PROVINCE MONTHLY TABLE
DROP TABLE IF EXISTS monthly_province;
CREATE TABLE IF NOT EXISTS monthly_province (
    id INT PRIMARY KEY,
    province_id INT,
    case_id INT,
    month VARCHAR(255),
    total INT   
);

-- PROVINCE YEARLY TABLE
DROP TABLE IF EXISTS yearly_province;
CREATE TABLE IF NOT EXISTS yearly_province (
    id INT PRIMARY KEY,
    province_id INT,
    case_id INT,
    year VARCHAR(255),
    total INT  
);

-- DISTRICT MONTHLY TABLE
DROP TABLE IF EXISTS monthly_district;
CREATE TABLE IF NOT EXISTS monthly_district (
    id INT PRIMARY KEY,
    district_id INT,
    case_id INT,
    month VARCHAR(255),
    total INT
);

-- DISTRICT YEARLY TABLE
DROP TABLE IF EXISTS yearly_district;
CREATE TABLE IF NOT EXISTS yearly_district (
    id INT PRIMARY KEY,
    district_id INT,
    case_id INT,
    year VARCHAR(255),
    total INT
);


