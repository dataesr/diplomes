DOCKER_IMAGE_NAME=ghcr.io/dataesr/diplomes
CURRENT_VERSION=$(shell cat application/__init__.py | cut -d "'" -f 2)

test: unit

unit:
	@echo Running unit tests...
	python3 -m pytest
	@echo End of unit tests

install:
	@echo Installing dependencies...
	pip install -r requirements.txt
	@echo End of dependencies installation

docker-build:
	@echo Building a new docker image
	docker build -t $(DOCKER_IMAGE_NAME):$(CURRENT_VERSION) -t $(DOCKER_IMAGE_NAME):latest .
	@echo Docker image built

docker-push:
	@echo Pushing a new docker image
	docker push $(DOCKER_IMAGE_NAME):$(CURRENT_VERSION)
	docker push $(DOCKER_IMAGE_NAME):latest
	@echo Docker image pushed

release:
	echo "__version__ = '$(VERSION)'" > application/__init__.py
	git commit -am '[release] version $(VERSION)'
	git tag $(VERSION)
	@echo If everything is OK, you can push with tags i.e. git push origin master --tags
