.PHONY: fetch-cville fetch-cville-parcels fetch-albemarle fetch-albemarle-parcels \
       fetch-albemarle-custom-fields build-cville build-albemarle build test serve deploy clean

# Charlottesville
fetch-cville:
	uv run fetch_permits.py --start-date 2018-01-01 --output permits.jsonl

fetch-cville-parcels:
	uv run fetch_parcels.py

build-cville:
	uv run build_site.py

# Albemarle
fetch-albemarle:
	cd albemarle && uv run fetch_plans.py

fetch-albemarle-parcels:
	cd albemarle && uv run fetch_parcels.py

fetch-albemarle-custom-fields:
	cd albemarle && uv run fetch_custom_fields.py

build-albemarle:
	cd albemarle && uv run build_site.py

# Combined
build: build-cville build-albemarle

test:
	cd albemarle && uv run --with pydantic --with pyyaml --with pytest python -m pytest test_extract_units.py -v

serve:
	cd site && python -m http.server 8000

deploy:
	wrangler pages deploy site --project-name=cville-permits --commit-dirty=true

clean:
	rm -f site/cville/data.json site/cville/parcels.geojson
	rm -f site/albemarle/data.json site/albemarle/parcels.geojson
