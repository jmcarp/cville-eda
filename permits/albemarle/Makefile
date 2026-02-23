.PHONY: fetch fetch-parcels build serve deploy clean

# Download plan data from Socrata CSV
fetch:
	uv run fetch_plans.py

# Download parcel shapefile from Albemarle County GIS
fetch-parcels:
	uv run fetch_parcels.py

# Build the static site (generate data.json + parcels.geojson)
build:
	uv run build_site.py

# Run a local development server
serve:
	cd site && python -m http.server 8001

# Deploy to Cloudflare Pages
deploy:
	wrangler pages deploy site --project-name=albemarle-permits --commit-dirty=true

# Clean generated files
clean:
	rm -f site/data.json site/parcels.geojson parcels.zip
