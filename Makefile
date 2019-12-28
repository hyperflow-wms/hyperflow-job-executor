all: release

# This assumes that you have commited your changes and tagged the commit
release:
	git push
	git push --tags

# For dev release update the patch place in version and push
dev-release:
	npm version patch
	git push
	git push --tags

clean: