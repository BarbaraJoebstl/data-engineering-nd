CREATE TABLE factRaceDate
(
    race_date_key    SERIAL PRIMARY KEY,
    date_key         INT NOT NULL REFERENCES dimDate(date_key),
    hacker_news_key  INT NOT NULL REFERENCES dimNews(hacker_key),
    race_key         INT NOT NULL REFERENCES dimRace(race_key),
    news_amount     decimal(5,2) NOT NULL
);

CREATE TABLE dimDate(
    date_key integer NOT NULL PRIMARY KEY,
    timestamp timestamp NOT NULL,
    year smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    time TODO
)

CREATE TABLE dimHackerNews(
    id key int // drop if duplicate
    author
    parent int
    text string
    time 
    kids array of ids
    dead boolean
    score int
    url string
    deleted boolean
    on_race_day boolean
)

CREATE TABLE dimRace(
    id key
    circuit_name string
    location string
    country string
    time
)