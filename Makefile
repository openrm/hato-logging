#!make

.PHONY: deps test coverage lint lint-fix

NPM_BIN = ./node_modules/.bin
export NODE_ENV ?= test

node_modules: package.json
	@npm install

deps: node_modules

test:
	@$(NPM_BIN)/mocha "test/**/*.js" "**/*.spec.js" --grep "it should log messages" --exit

coverage:
	@$(NPM_BIN)/nyc -x "test/*" -x "**/*.spec.js" --reporter=lcov --reporter=text-lcov --reporter=text $(MAKE) -s test

lint:
	@$(NPM_BIN)/eslint src

lint-fix:
	@$(NPM_BIN)/eslint src
