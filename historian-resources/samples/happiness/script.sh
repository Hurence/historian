export_csv_to_influx \
	--csv 2015.csv \
	--dbname happiness \
	--measurement "Happiness Score" \
	--tag_columns "Country" \
	--time_column Timestamp \
	--server 127.0.0.1:8086
