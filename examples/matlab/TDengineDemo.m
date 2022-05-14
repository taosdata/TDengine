%% Connect to TDengine
clear;
fprintf("Connecting to TDengine...");
dbName = 'tsdb';
user = 'root';
password = 'taosdata';
jdbcDriverName = 'com.taosdata.jdbc.TSDBDriver';
jdbcUrl = 'jdbc:TSDB://192.168.1.113:0/';
conn = database(dbName, user, password, jdbcDriverName, jdbcUrl)
if isempty(conn.Message)
    fprintf("Connection is successfully established!\n");
else
    fprintf("Failed to connect to server: %s\n", conn.Message);
end

%% Query a table in TDengine, and store the results in a MATLAB table object 'tb1'
% Please note that the select() function retrieves all rows in a table/supertale into MATLAB
sql = "select ts, distance1 from device1 limit 5";
fprintf("Execute query: %s\n", sql);
tic
tb1 = select(conn, sql);
timeused = toc;
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", height(tb1), width(tb1), timeused);

% To go a bit further, we can convert the MATLAB table object to a MATLAB matrix object
data = table2array(tb1)

%% Query table names in a TDengine supertable, and store the results in a MATLAB table object 'stbmeta'
sql = "select tbname from devices limit 10";
fprintf("Execute query: %s\n", sql);
tic;
stbmeta = select(conn, sql);
timeused = toc;
fprintf("\tTables in supertable 'devices': %t", stbmeta);
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", height(stbmeta), width(stbmeta), timeused);

%% Query a TDengine supertable, and stores the results in a MATLAB table object 'stb'
sql = "select ts, distance1 from devices";
fprintf("Execute query: %s\n", sql);
tic;
stb = select(conn, sql);
timeused = toc;
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", height(stb), width(stb), timeused);

%% Query TDengine using cursors and specify the number of rows to fetch
sql = 'select * from device1';
rowLimit = 5;
fprintf("Execute query: %s with row limit set to %d\n", sql, rowLimit);
tic;
% Get cursor
cur = exec(conn, sql);
% Fetch data
cur = fetch(cur, rowLimit);
data = cur.Data
timeused = toc;
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", size(data, 1), size(data, 2), timeused);

%% Query specific columns in a TDenigine table 'device1', and stores the results directly in a MATLAB cell array 'data'
sql = 'SELECT * FROM device1 order by ts asc';
fprintf("Execute query: %s\n", sql);
tic;
data = fetch(conn, sql);
timeused = toc;
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", size(data, 1), size(data, 2), timeused);
% Let's now convert the cell array 'data' into some matrices, and make a plot of column 'c1' again the timestamp 'ts'
ts = cell2mat(data(:,1));
c1 = cell2mat(data(:,2));

%% Query aggregation results from a table
% TDengine is so powerful at aggregated computations. Let's calculate the max, mean, standard deviation and min values for every 10 minutes in the
% tb1's timeline, and then plot them together with all the data points in tb1
sql = sprintf('SELECT max(measure1), avg(measure1), stddev(measure1), min(measure1) FROM device1 WHERE ts >= %d and ts <= %d interval(10m)', ts(1), ts(end));
fprintf("Execute query: %s\n", sql);
tic;
c1_stats = fetch(conn, sql);
timeused = toc;
fprintf("\tQuery completed!\n\tNumber of rows retrieved: %d\n\tNumber of columns in each row: %d\n\tTime used: %g\n", size(c1_stats, 1), size(c1_stats, 2), timeused);
% Prepare data for plotting.
tsAsDate = datestr(ts/86400000 + datenum(1970,1,1), 'mm-dd HH:MM');
c1_stats = cell2mat(c1_stats);
c1_stats_ts = c1_stats(:, 1);
c1_stats_max = c1_stats(:, 2);
c1_stats_mean = c1_stats(:, 3);
c1_stats_stddev = c1_stats(:, 4);
c1_stats_min = c1_stats(:, 5);
c1_stats_tsAsDate = datestr(c1_stats(:,1)/86400000 + datenum(1970,1,1), 'mm-dd HH:MM');

%% Now let's plot the data and associated statistical aggregation calculation in a figure.
fh = figure(1);
set(fh,'position',[50 50 1300 700]);
h1 = scatter(ts, c1, 5, 'c');
hold on;
h2 = plot(c1_stats_ts + 300000, c1_stats_max, '-or', 'linewidth', 1);
hold on;
h3 = plot(c1_stats_ts + 300000, c1_stats_mean, '-xg', 'linewidth', 1);
hold on;
h4 = plot(c1_stats_ts + 300000, c1_stats_stddev, '-*y', 'linewidth', 1);
hold on;
h5 = plot(c1_stats_ts + 300000, c1_stats_min, '-+k', 'linewidth', 1);
xlabel('time');
ylabel('measurement1');
set(gca, 'xtick',[ts(1),ts(end/4),ts(2*end/4),ts(3*end/4),ts(end)]);
set(gca, 'xticklabel',{tsAsDate(1,:), tsAsDate(end/4,:),tsAsDate(2*end/4,:),tsAsDate(3*end/4,:),tsAsDate(end,:)});
xlim([ts(1), ts(end)]);
legend([h1, h2, h3, h4, h5], 'data points', 'max per 10 mins', 'mean per 10 mins', 'stddev per 10 mins', 'min per 10 mins');
title('Device Measurement Monitoring Demo');
grid on;

%% Insert data into TDengine using exec()
sql = 'insert into device1 (ts, distance1) values (now, -1)';
fprintf("Execute query: %s\n", sql);
cur = exec(conn, sql)
sql = 'select * from device1 limit 1';
fprintf("Execute query: %s\n", sql);
data = select(conn, sql)
conn.close;

%% Insert data into TDengine using datainsert()
% this is currently not supported

% colnames = {'ts','c1','c2','c3'};
% dat = {'now' 99 99 99};
% tbname = 'plane1';
% datainsert(conn, tbname, colnames, dat);
% cur = exec(conn, 'select * from ' + tbname);
% cur = fetch(cur, 5);
% data = cur.Data

