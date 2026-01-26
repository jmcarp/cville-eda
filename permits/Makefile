.PHONY: fetch fetch-parcels build deploy serve clean

# Fetch permit data from the city portal
fetch:
	python fetch_permits.py

# Fetch parcel/zoning data
fetch-parcels:
	python fetch_parcels.py

# Build the static site (generate data.json)
build:
	python build_site.py

# Deploy to Cloudflare Pages
deploy:
	wrangler pages deploy site --project-name=cville-permits --commit-dirty=true

# Run a local development server
serve:
	cd site && python -m http.server 8000

# Clean generated files
clean:
	rm -f site/data.json
