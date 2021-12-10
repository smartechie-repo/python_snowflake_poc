infra-up:
	docker-compose up -d

infra-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

deps:
	python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip
	python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org --editable .
	python3 -m pip install --trusted-host pypi.python.org --trusted-host files.pythonhosted.org --trusted-host pypi.org -r requirements.txt --index-url https://admin:AP4crtTCxHc32R7zM5D9qmsZbwGhHWh9iRfaZ7@rightart.jfrog.io/artifactory/api/pypi/pypi/simple
	( \
       . .env/bin/activate; \
    )

dev-env:
	python3 -m venv .env


.PHONY: deps