build: requirements.txt
	@echo "CREATING virtual environment venv...";
	@python3 -m venv venv;
	@echo "INSTALLING dependencies...";
	@. venv/bin/activate; pip install -r requirements.txt;

cluster: 
	@echo "CREATING cluster...";
	@. venv/bin/activate; python ./scripts/create_cluster.py;

run: 
	@. venv/bin/activate; python run.py

clean:
	@echo "DELETING cluster...";
	@. venv/bin/activate; python ./scripts/delete_cluster.py;
	@echo "DELETING venv and pycache...";
	@rm -r venv;
	@find . | grep -E "(__pycache__|.pytest_cache)" | xargs rm -rf;
