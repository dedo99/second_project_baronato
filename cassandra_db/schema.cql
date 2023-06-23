-- init-script.cql

-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS streaming WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
-- CREATE TABLE IF NOT EXISTS streaming.raw_row_dataset (key text PRIMARY KEY, value text);

CREATE TABLE IF NOT EXISTS streaming.raw_row_dataset (
  time_key text PRIMARY KEY,
  use_kw text,
  gen_kw text,
  house_overall_kw text,
  dishwasher_kw text,
  furnace_1_kw text,
  furnace_2_kw text,
  home_office_kw text,
  fridge_kw text,
  wine_cellar_kw text,
  garage_door_kw text,
  kitchen_12_kw text,
  kitchen_14_kw text,
  kitchen_38_kw text,
  barn_kw text,
  well_kw text,
  microwave_kw text,
  living_room_kw text,
  solar_kw text,
  temperature text,
  icon text,
  humidity text,
  visibility text,
  summary text,
  apparent_temperature text,
  pressure text,
  wind_speed text,
  cloud_cover text,
  wind_bearing text,
  precip_intensity text,
  dew_point text,
  precip_probability text
);

SELECT * FROM streaming.raw_row_dataset;