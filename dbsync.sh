#!/bin/bash -e

# Dumps a PostgresSQL over the network, from production to staging
# Check the internal wiki (http://wiki.scilifelab.se/bioinfo/node/3752) for 
# credentials' information

DB="<insert here the DB name>"
DB_USER="<insert here the DB user to make the dumping>"

PROD="genologics.scilifelab.se"
STAGING="genologics-stage.scilifelab.se"

# Clarity LIMS creates a new row in a table called LoginAudit every time a user logs
# in. This includes the apiuser. We connect with this user hundreds of times per day
# and this creates a really big table that fills up the disk, we just don't need that.
psql -U $DB_USER $DB -c "DELETE FROM LoginAudit WHERE (Username = 'apiuser');"
vacuumdb --dbname $DB --table LoginAudit -U $DB_USER -v -f

echo "Preventing new connections from happening on $STAGING..."
ssh $STAGING "psql -U $DB_USER $DB -c 'REVOKE CONNECT ON DATABASE \"$DB\" FROM PUBLIC;'"

# http://stackoverflow.com/questions/5408156/how-to-drop-a-postgresql-database-if-there-are-active-connections-to-it
echo "Current active sessions:"
ssh $STAGING "psql -U $DB_USER $DB -c 'SELECT * FROM pg_stat_activity'"

echo "Killing all active PostgresSQL sessions on $STAGING..."
ssh $STAGING "psql -U $DB_USER $DB -c 'SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid();'"
echo "return code: $?"

#Implicit pause to give pgsql time to drop all connection
sleep 5

echo "Active sessions after killing all sessions (should only be one connection from pg_stat_activity)"
ssh $STAGING "psql -U $DB_USER $DB -c 'SELECT * FROM pg_stat_activity'"

echo "Dropping and recreating $STAGING database..."
ssh $STAGING "dropdb -U $DB_USER $DB"
ssh $STAGING "createdb -U $DB_USER $DB"

echo "Dumping $PROD database $DB into $STAGING..."
pg_dump -U $DB_USER $DB | sed -e "s/$PROD/$STAGING/g" | ssh $STAGING "psql -U $DB_USER $DB"

echo "Re-enabling connections on $STAGING..."
ssh $STAGING "psql -U $DB_USER $DB -c 'GRANT CONNECT ON DATABASE \"$DB\" TO PUBLIC'"