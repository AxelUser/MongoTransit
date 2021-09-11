#!/bin/bash
echo "Init cluster step 1: Start all of the containers"
docker-compose up -d --force-recreate

echo "Waiting 20 seconds for all containers to start"
sleep 20s

echo "Init cluster step 2.1: Initialize config servers rs"
docker-compose exec -T configsvr01 sh -c "mongo < /scripts/init-configserver.js"

echo "Init cluster step 2.2: Initialize shard 1 rs"
docker-compose exec -T shard01-a sh -c "mongo < /scripts/init-shard01.js"

echo "Init cluster step 2.3: Initialize shard 2 rs"
docker-compose exec -T shard02-a sh -c "mongo < /scripts/init-shard02.js"

echo "Init cluster step 2.4: Initialize shard 3 rs"
docker-compose exec -T shard03-a sh -c "mongo < /scripts/init-shard03.js"

echo "Waiting 50 seconds for all replica sets to elect their primaries"
sleep 50s

echo "Init cluster step 3: Initializing the router"
docker-compose exec -T router01 sh -c "mongo < /scripts/init-router.js"
