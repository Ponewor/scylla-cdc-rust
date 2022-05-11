keyspaces=$(echo desc keyspaces | cqlsh | xargs -n1 echo | grep -v ^system)
query=""
for ks in $keyspaces; do
    echo Dropping "$ks"
    query="${query}drop keyspace $ks;"
done
echo "$query" | cqlsh
