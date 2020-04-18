labels_strings = [] #?
ur = Url2DomainTransformer(inputCol="visits", outputCol="urls")
cv = CountVectorizer(inputCol="urls", outputCol="features")
indexer = StringIndexer(inputCol="gender_age", outputCol="label")
lr = LogisticRegression()
​
pipeline = Pipeline(stages=[cv, indexer, lr])
​
pipepline_crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=ParamGridBuilder() \
    .addGrid(LogisticRegression.regParam, [0.2, 0.1, 0.01]) \
    .addGrid(LogisticRegression.elasticNetParam, [0.2, 0.1, 0.01]).build(),
    evaluator=MulticlassClassificationEvaluator(metricName="accuracy"),
    numFolds=3,
    parallelism=4
)