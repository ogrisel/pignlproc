
build:
	mvn assembly:assembly

clean-pig-generated:
	rm -rf exaamples/*.log
