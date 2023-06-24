-- init-script.cql

-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS streaming WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
-- CREATE TABLE IF NOT EXISTS streaming.raw_row_dataset (key text PRIMARY KEY, value text);

CREATE TABLE IF NOT EXISTS streaming.raw_row_dataset (
  time_key text PRIMARY KEY,
  use_kw decimal,
  gen_kw decimal,
  house_overall_kw decimal,
  dishwasher_kw decimal,
  furnace_1_kw decimal,
  furnace_2_kw decimal,
  home_office_kw decimal,
  fridge_kw decimal,
  wine_cellar_kw decimal,
  garage_door_kw decimal,
  kitchen_12_kw decimal,
  kitchen_14_kw decimal,
  kitchen_38_kw decimal,
  barn_kw decimal,
  well_kw decimal,
  microwave_kw decimal,
  living_room_kw decimal,
  solar_kw decimal,
  temperature decimal,
  icon text,
  humidity decimal,
  visibility decimal,
  summary text,
  apparent_temperature decimal,
  pressure decimal,
  wind_speed decimal,
  cloud_cover text,
  wind_bearing decimal,
  precip_intensity decimal,
  dew_point decimal,
  precip_probability decimal
);

SELECT * FROM streaming.raw_row_dataset;