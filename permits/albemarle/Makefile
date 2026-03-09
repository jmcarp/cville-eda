.PHONY: fetch fetch-parcels fetch-historical-parcels build serve clean

# Download plan data from Socrata CSV
fetch:
	uv run fetch_plans.py

# Download parcel shapefile from Albemarle County GIS
fetch-parcels:
	uv run fetch_parcels.py

# Download historical parcel snapshots (2001-2024) for fallback matching
fetch-historical-parcels:
	uv run fetch_parcels.py --historical

# Build the site data (outputs to ../site/albemarle/)
build:
	uv run build_site.py

# Run a local development server (from unified site root)
serve:
	cd ../site && python -m http.server 8000

# Clean generated files
clean:
	rm -f ../site/albemarle/data.json ../site/albemarle/parcels.geojson parcels.zip
	rm -rf parcels_historical/
