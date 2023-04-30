--# Crash

CREATE TABLE crash (
  Report_number VARCHAR(50) NOT NULL,
  Report_seq_no REAL NOT NULL,
  DOT_Number VARCHAR(50),
  Report_Date DATE,
  Report_State CHAR(2),
  Fatalities REAL,
  Injuries REAL,
  Tow_Away CHAR(1),
  Hazmat_released CHAR(1),
  Trafficway_Desc VARCHAR(255),
  Access_Control_Desc VARCHAR(255),
  Road_surface_Condition_Desc VARCHAR(255),
  Weather_Condition_Desc VARCHAR(255),
  Light_Condition_Desc VARCHAR(255),
  Vehicle_ID_Number VARCHAR(50),
  Vehicle_License_number VARCHAR(50),
  Vehicle_license_state CHAR(2),
  Severity_Weight REAL,
  Time_weight REAL,
  citation_issued_desc VARCHAR(255),
  seq_num REAL,
  Not_Preventable CHAR(1)
);


-- Inspection

CREATE TABLE inspection (
  Unique_ID FLOAT PRIMARY KEY,
  Report_Number VARCHAR(255)  NOT NULL,
  Report_State VARCHAR(2) ,
  DOT_Number VARCHAR(255) NOT NULL,
  Insp_Date DATE NOT NULL,
  Insp_level_ID FLOAT NOT NULL,
  County_code_State VARCHAR(2) NOT NULL,
  Time_Weight FLOAT NOT NULL,
  Driver_OOS_Total FLOAT ,
  Vehicle_OOS_Total FLOAT,
  Total_Hazmat_Sent FLOAT ,
  OOS_Total FLOAT,
  Hazmat_OOS_Total FLOAT,
  Hazmat_Placard_req CHAR(1) ,
  Unit_Type_Desc VARCHAR(255),
  Unit_Make VARCHAR(255) ,
  Unit_License VARCHAR(255) ,
  Unit_License_State VARCHAR(2),
  VIN VARCHAR(255) ,
  Unit_Decal_Number VARCHAR(255) ,
  Unit_Type_Desc2 VARCHAR(255),
  Unit_Make2 VARCHAR(255),
  Unit_License2 VARCHAR(255),
  Unit_License_State2 VARCHAR(2),
  VIN2 VARCHAR(255),
  Unit_Decal_Number2 VARCHAR(255),
  Unsafe_Insp CHAR(1) ,
  Fatigued_Insp CHAR(1) ,
  Dr_Fitness_Insp CHAR(1) ,
  Subt_Alcohol_Insp CHAR(1),
  Vh_Maint_Insp CHAR(1),
  HM_Insp CHAR(1) ,
  BASIC_Viol FLOAT ,
  Unsafe_Viol FLOAT,
  Fatigued_Viol FLOAT,
  Dr_Fitness_Viol FLOAT,
  Subt_Alcohol_Viol FLOAT,
  Vh_Maint_Viol FLOAT ,
  HM_Viol FLOAT 
);


-- Violation

CREATE TABLE violation (
    Unique_ID FLOAT PRIMARY KEY,
    Insp_Date DATE NOT NULL,
    DOT_Number VARCHAR(50) NOT NULL,
    Viol_Code VARCHAR(10) NOT NULL,
    BASIC_Desc VARCHAR(50) ,
    OOS_Indicator CHAR(1),
    OOS_Weight REAL,
    Severity_Weight FLOAT,
    Time_Weight REAL,
    Total_Severity_Wght FLOAT,
    Section_Desc VARCHAR(200) ,
    Group_Desc VARCHAR(200),
    Viol_Unit CHAR(1)
);


-- Census

CREATE TABLE census (
    DOT_NUMBER TEXT PRIMARY KEY,
    LEGAL_NAME TEXT,
    DBA_NAME TEXT,
    CARRIER_OPERATION TEXT,
    HM_FLAG TEXT,
    PC_FLAG TEXT,
    PHY_STREET TEXT,
    PHY_CITY TEXT,
    PHY_STATE TEXT,
    PHY_ZIP TEXT,
    PHY_COUNTRY TEXT,
    MAILING_STREET TEXT,
    MAILING_CITY TEXT,
    MAILING_STATE TEXT,
    MAILING_ZIP TEXT,
    MAILING_COUNTRY TEXT,
    TELEPHONE TEXT,
    FAX TEXT,
    EMAIL_ADDRESS TEXT,
    MCS150_DATE DATE,
    MCS150_MILEAGE NUMERIC,
    MCS150_MILEAGE_YEAR FLOAT,
    ADD_DATE DATE,
    OIC_STATE TEXT,
    NBR_POWER_UNIT FLOAT,
    DRIVER_TOTAL FLOAT,
    RECENT_MILEAGE NUMERIC,
    RECENT_MILEAGE_YEAR FLOAT,
    VMT_SOURCE_ID FLOAT
);
