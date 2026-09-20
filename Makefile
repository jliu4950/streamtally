.PHONY: demo up down verify bench test check clean

## demo: build the dashboard and run the whole thing with no infrastructure at all
demo:
	npm install
	npm run build:web
	npm start

## up: start Redpanda and Postgres, then run against them
up:
	docker compose up -d --wait
	BUS=kafka STORE=postgres npm start

down:
	docker compose down -v

## verify: prove the counting claim (add BUS=kafka STORE=postgres to run it on real infra)
verify:
	npm run verify:exactness

bench:
	npm run bench

test:
	npm test

check:
	npm run lint && npm run typecheck && npm test && npm run verify:exactness

clean:
	rm -f *.db *.db-wal *.db-shm
	rm -rf web/dist
