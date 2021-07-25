echo "Step 1: Start all of the containers"
docker-compose up -d

echo "Step 2.1: Initialize config servers rs"
docker-compose exec -t configsvr01 sh -c "mongo < /scripts/init-configserver.js"

echo "Step 2.2: Initialize shard 1 rs"
docker-compose exec -t shard01-a sh -c "mongo < /scripts/init-shard01.js"

echo "Step 2.3: Initialize shard 2 rs"
docker-compose exec -t shard02-a sh -c "mongo < /scripts/init-shard02.js"

echo "Step 2.4: Initialize shard 3 rs"
docker-compose exec -t shard03-a sh -c "mongo < /scripts/init-shard03.js"

echo "Step 3: Initializing the router"
docker-compose exec -t router01 sh -c "mongo < /scripts/init-router.js"
