Table dim_time {
  d_date_id int [pk]
  d_date date
  n_week_id int
  n_day_id int
  n_month_id int
  n_year_id int
  n_quarter_id int 
}

Table dim_week {
  n_week_id int [pk]
  n_week int
}

Table dim_day {
  n_day_id int [pk]
  n_day int
}


Table dim_month {
  n_month_id int [pk]
  n_month int
}

Table dim_year {
  n_year_id int [pk]
  n_year int
}

Table dim_quarter {
  n_quarter_id int [pk]
  n_quarter int
}



Ref: dim_time.n_week_id > dim_week.n_week_id
Ref: dim_time.n_month_id > dim_month.n_month_id
Ref: dim_time.n_year_id > dim_year.n_year_id
Ref: dim_time.n_day_id > dim_day.n_day_id
Ref: dim_time.n_quarter_id > dim_quarter.n_quarter_id
