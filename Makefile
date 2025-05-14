# Default target that runs the entire dlt_pipeline
make-project: setup data figs

# Set up the project environment
setup:
	@echo "Setting up project environment..."
	conda env create -f environments/legislation_pipeline.yml
	@echo "Environment setup complete."

# Fetch and prepare raw data
data:
	@echo "Fetching and preparing data..."
	mkdir 'data'
	python src/pipeline.py

# Generate visualizations
figs:
	mkdir -p 'figs'
	@echo "Generating visualizations"
	python src/generate_graphics.py

# Define phony targets
.PHONY: all setup data figs

# Help command to display available targets
help:
	@echo "Available targets:"
	@echo "  all        : Run the entire pipeline"
	@echo "  setup      : Set up the project environment"
	@echo "  data       : Fetch and prepare raw data"
	@echo "  figs       : Generate reports and visualizations"
	@echo "  help       : Display this help message"