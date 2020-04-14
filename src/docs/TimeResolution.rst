======================================
Configuring alternate time resolutions
======================================

The most important thing to appreciate is that the settings which affect the time resolution must only be set once,
before you have written any data, and then left the same thereafter. 

Existing data is stored, encoded with time offsets relative to these settings. 
Changing the settings does not update the data: The data just becomes wrong!  

Here be dragons!

From the configuration file:

# the time resolution is the minimum time step for metrics. The default is 1ms.
# wide rows are employed in Cassandra and 3-weeks at 1ms gives ~1.8 Bn columns.
# setting a lower resolution (E.g. 1000ms (1 sec)) means more per-metric time-series data can be fit into a row
# but the data is truncated to the nearest (floor) time bucket.
# this setting is in whole milliseconds and data_table_row_width must also be set or an error will prevent KairosDB from starting.
# this setting must not be changed after the first metric has been written
#data_time_resolution: 1

# the row width (also see above) is the number of data points to fit into a wide row in Cassandra.
# adjusting the time resolution also requires changing this parameter so data is aligned to something reasonable, and less that Cassandra's 2Bn column limit.
# the default is 21days to support ~1.8 Bn data points in a row. Change this to support alternate configurations of time resolution
# this setting is a positive integer number of columns to support and data_time_resolution must also be set or an error will prevent KairosDB from starting.
# this setting must not be changed after the first metric has been written.
#data_table_row_width: 1814400000


Some examples:

1. The default: 3weeks in a row at 1ms resolution.
data_time_resolution: 1
data_table_row_width: 1814400000

2. 500 days in a row at 25ms resolution
data_time_resolution: 25
data_table_row_width: 1728000000

3. 50 years in a row at 1-second resolution
data_time_resolution: 1000
data_table_row_width: 1577880000
