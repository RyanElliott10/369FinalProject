# 369FinalProject

### Dataset.py
**Usage**
`python dataset.py -d /Users/ryanelliott/Downloads/archive/wallstreetbets_comments.csv -o /Users/ryanelliott/Documents/college/fourth_year/winter/csc369/369FinalProject/data/data.csv  -l 16 -c 20000`

### WSBSentiment Scala Package
**Usage**
`sbt package`
`spark-submit --class WSBSentiment.App --master yarn target/scala-2.12/wsbsentiment_2.12-0.1.jar ./data/comments.csv`