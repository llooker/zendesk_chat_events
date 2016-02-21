#!/bin/sh

# capture start time
startTime=$(date +%s)

echo "configuring aws tasks..."

# Destination Redshift cluster
RSHOST=YOUR_REDSHIFT_HOST
RSDATABASE=YOUR_REDSHIFT_DATABASE
RSUSER=YOUR_REDSHIFT_USER
RSPORT=5439
RSPASSWORD=`cat .psql_r_passwd`

# Get AWS credentials
S3_KEY=`cat .s3_key`
S3_SECRET=`cat .s3_secret`
SOURCE_BUCKET=SOURCE_YOUR_BUCKET
DESTINATION_BUCKET=YOUR_DESTINATION_BUCKET
echo "creating view public.zendesk_tickets..."

# Create a view of the data, temporarily, where we handle new-line madness
PGHOST=$RSHOST PGDATABASE=$RSDATABASE PGUSER=$RSUSER PGPORT=$RSPORT PGPASSWORD=$RSPASSWORD \
/usr/bin/psql -c "select id , created_date , replace(replace(zendesk___description___c, '\\n', '\\\n'), '\"', '\"\"') as body into public.zendesk_tickets from salesforce._zendesk___zendesk__ticket___c where zendesk___subject___c ilike '%chat%';"

echo "unloading public.zendesk_tickets to $SOURCE_BUCKET"

# Unload the view into S3
PGHOST=$RSHOST PGDATABASE=$RSDATABASE PGUSER=$RSUSER PGPORT=$RSPORT PGPASSWORD=$RSPASSWORD \
/usr/bin/psql -c "unload ('select * from public.zendesk_tickets') \
to $SOURCE_BUCKET \
with credentials 'aws_access_key_id=$S3_KEY;aws_secret_access_key=$S3_SECRET' \
delimiter '|' \
addquotes \
allowoverwrite;"

echo "dropping public.zendesk_tickets..."

# Drop the temporary view
PGHOST=$RSHOST PGDATABASE=$RSDATABASE PGUSER=$RSUSER PGPORT=$RSPORT PGPASSWORD=$RSPASSWORD \
/usr/bin/psql -c "drop table public.zendesk_tickets;"

echo "executing spark task..."

# Execute Spark job on remote cluster
ssh YOUR_USER@YOUR_ENDPOINT "cd /usr/lib/spark/ && ./bin/spark-submit --class "chatEvent" /home/hadoop/chat-events_2.10-1.0.jar"

echo "copying processed chat events into public.chat_events table..."

# Issue COPY statement
PGHOST=$RSHOST PGDATABASE=$RSDATABASE PGUSER=$RSUSER PGPORT=$RSPORT PGPASSWORD=$RSPASSWORD \
/usr/bin/psql -c "truncate table public.chat_events; copy public.chat_events \
from $DESTINATION_BUCKET \
with credentials 'aws_access_key_id=$S3_KEY;aws_secret_access_key=$S3_SECRET' \
json 's3://YOUR_JSON_SCHEMA_LOCATION/chat_jsonpath.json';"

echo "cleaning up $DESTINATION_BUCKET"

$HOME/bin/s3cmd -c $HOME/.s3cfg del $DESTINATION_BUCKET --recursive
$HOME/bin/s3cmd -c $HOME/.s3cfg del $DESTINATION_BUCKET"_\$folder\$"

# capture end time
endTime=$(date +%s)

# print duration in seconds
echo "execution time:" `expr $endTime - $startTime` "seconds"
