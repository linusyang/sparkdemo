MOVIE = Star Wars (1977)

# Edit the configuration for your own Spark cluster
SPARK_HOME = /root/spark-1.0.1-bin-hadoop2
SPARK_MASTER = spark://10.130.216.202:7077
SPARK_MEMORY = 900m

APP = MovieSimilarities
SRC = $(APP).scala
JAR = target/scala-2.10/movie-similarities_2.10-1.0.jar

all: shell

%.jar: $(SRC)
	-sbt package

up:
	$(SPARK_HOME)/sbin/start-all.sh

down:
	$(SPARK_HOME)/sbin/stop-all.sh

shell: $(JAR)
	$(SPARK_HOME)/bin/spark-shell --master $(SPARK_MASTER) --jars $(JAR)

status:
	jps

run: $(JAR)
	$(SPARK_HOME)/bin/spark-submit --class "$(APP)" --master $(SPARK_MASTER) --executor-memory $(SPARK_MEMORY) $(JAR) "$(MOVIE)"

.PHONY: all run up down shell status
