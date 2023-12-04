CREATE OR REPLACE TABLE RAW_TABLE(
    _ID VARCHAR,
    IOS_APP_ID BIGINT,
    TITLE VARCHAR,
    DEVELOPER_NAME VARCHAR,
    DEVELOPER_IOS_ID BIGINT,
    IOS_STORE_URL VARCHAR,
    SELLER_OFFICIAL_WEBSITE VARCHAR,
    AGE_RATING VARCHAR,
    TOTAL_AVERAGE_RATING FLOAT,
    TOTAL_NUMBER_OF_RATINGS FLOAT,
    AVERAGE_RATING_FOR_VERSION FLOAT,
    NUMBER_OF_RATINGS_FOR_VERSION BIGINT,
    ORIGINAL_RELEASE_DATE VARCHAR,
    CURRENT_VERSION_RELEASE_DATE VARCHAR,
    PRICE_USD FLOAT,
    PRIMARY_GENRE VARCHAR,
    ALL_GENRES VARCHAR,
    LANGUAGES VARCHAR,
    DESCRIPTION VARCHAR
)