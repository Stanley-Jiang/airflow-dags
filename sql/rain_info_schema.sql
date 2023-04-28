CREATE TABLE IF NOT EXISTS rain_info (
  siteid text NOT NULL,
  site_name text NOT NULL,
  site_ename text NOT NULL,
  area text NOT NULL,
  county text NOT NULL,
  township text NOT NULL,
  site_address text NOT NULL,
  longitude text NOT NULL,
  latitude text NOT NULL,
  agency_name text NOT NULL,
  monitor_month text NOT NULL,
  monitor_date text NOT NULL,
  item_name text NOT NULL,
  item_ename text NOT NULL,
  result_value text NOT NULL,
  item_unit text NOT NULL,
/*  longitude_numeric numeric NOT NULL,
  latitude_numeric numeric NOT NULL,
  monitor_date_date date NOT NULL,
  result_value_numeric numeric NOT NULL,
*/
  PRIMARY KEY(siteid, monitor_month, monitor_date, item_ename)
);
