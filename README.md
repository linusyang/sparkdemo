Spark Live Demo
=====
Copyright (c) 2014 Linus Yang & Tomas Tauber


Introduction
-----
* This is for the "Spark Live Demo Section" of the _Big Data Analytics in Scala_ group meeting at http://www.meetup.com/HK-Functional-programming/events/179181332/. Slides available at https://linusyang.github.io/sparkdemo/slides/.

* This demo is originally made by Nick Pentreath at http://mlnick.github.io/blog/2013/04/01/movie-recommendations-and-more-with-spark/.

* For more details about the calculation of correlation between movies, which we didn't talk clearly in the meeting, please refer to http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/.

* Movie datasets are from _MovieLens 100k dataset_, which can be found at http://grouplens.org/datasets/movielens/.

Usage
-----
1. Install the latest [Spark](https://spark.apache.org/downloads.html) releases on your node. And if you need to build the Scala code, you need to install [sbt](http://www.scala-sbt.org/).
2. Setup your Spark cluster by [this tutorial](https://spark.apache.org/docs/latest/cluster-overview.html) (_[Standalone mode](https://spark.apache.org/docs/latest/spark-standalone.html) is recommended_).
3. Clone this repository on __every node__ of your Spark cluster __at the same location__ by running `git clone https://github.com/linusyang/sparkdemo.git && cd sparkdemo/`.
4. Setup the configuration by editing the `Makefile` (_you should have the `make` utility first_):
	* `SPARK_HOME`: Directory where Spark is installed
	* `SPARK_MASTER`: URL (`spark://`) of Spark master node
	* `SPARK_MEMORY`: Memory size used for every Spark worker node
5. Startup the Spark service by running `make up`.
6. Run the demo by either interactively in a shell by `make` or in a batch by `make run`:
	* `make`: If you run in the shell, type `new Worker().run(sc)` or type `new Worker().read(sc).pair().calc().show()` to get the result.
	* `make run`: Or if you run in a batch, you will directly get the result.
7. If you want to stop the Spark cluster when finished the demo, use `make down` to shutdown all Spark instances.

License
-----
Licensed under [GPLv3](http://www.gnu.org/copyleft/gpl.html).
