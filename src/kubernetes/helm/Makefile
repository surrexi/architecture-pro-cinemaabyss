lint:
	@helm lint . --strict

clean:
	@rm -f *.tar.gz *.tgz

readme: lint
	@helm-docs

schema-gen: lint
	@helm schema

package: clean lint readme schema-gen
	@helm package .

template: lint
	@helm template cinemaabyss . --namespace cinemaabyss --output-dir ../

install: lint
	@helm install cinemaabyss . --namespace cinemaabyss --create-namespace --values values.yaml

upgrade: lint
	@helm upgrade --install cinemaabyss . --namespace cinemaabyss --values values.yaml

