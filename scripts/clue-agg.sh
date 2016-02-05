spark-submit --class ClueAggFilter --master=yarn-cluster --queue=shortrunning --num-executors 200 --driver-memory 7g --executor-memory 7g ~/noun-sense-induction-scala/bin/spark/nsi_2.10-0.0.1.jar ukwac/voc-50.csv ukwac/senses/senses-ukwac-dep-cw-e0-N200-n200-minsize5-js-format.csv ukwac/output/W ukwac/output/WF ukwac/output/F
