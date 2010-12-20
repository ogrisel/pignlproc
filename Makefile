
build:
	mvn assembly:assembly

clean-pig-generated:
	rm -rf examples/*.log
	rm -rf *.log
