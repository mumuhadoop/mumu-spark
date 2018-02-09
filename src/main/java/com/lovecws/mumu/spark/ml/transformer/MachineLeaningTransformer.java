package com.lovecws.mumu.spark.ml.transformer;

import com.lovecws.mumu.spark.MumuSparkConfiguration;
import org.apache.spark.ml.attribute.Attribute;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.types.DataTypes.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: tf-idf数据抽取
 * @date 2018-02-07 16:48
 */
public class MachineLeaningTransformer {

    private SQLContext spark = new MumuSparkConfiguration().sqlContext();

    /**
     * 将短语拆分成多个单词
     */
    public void tokenizer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(1, "I wish Java could use case classes"),
                RowFactory.create(2, "Logistic,regression,models,are,neat")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", IntegerType, false, Metadata.empty()),
                new StructField("sentence", StringType, false, Metadata.empty())
        });
        Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

        spark.udf().register("countTokens", (WrappedArray<?> words) -> words.size(), IntegerType);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        Dataset<Row> tokenized = tokenizer.transform(sentenceDataFrame);
        tokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);

        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("sentence")
                .setOutputCol("words")
                .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);
        Dataset<Row> regexTokenized = regexTokenizer.transform(sentenceDataFrame);
        regexTokenized.select("sentence", "words")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .show(false);

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filtered");
        remover.transform(regexTokenized).select("sentence", "words", "filtered")
                .withColumn("tokens", callUDF("countTokens", col("words")))
                .withColumn("filtereTokens", callUDF("countTokens", col("filtered")))
                .show(false);
    }

    /**
     * 使用停用词 将一些停用词过滤掉
     */
    public void stopWordsRemover() {
        List<Row> data = Arrays.asList(
                RowFactory.create(Arrays.asList("I", "saw", "the", "red", "balloon")),
                RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("raw", DataTypes.createArrayType(StringType), false, Metadata.empty())
        });
        Dataset<Row> dataset = spark.createDataFrame(data, schema);

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("raw")
                .setOutputCol("filtered");
        remover.transform(dataset).show(false);
    }

    /**
     * 将多个单词 组成可能的短语
     */
    public void ngram() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, Arrays.asList("Hi", "I", "heard", "about", "Spark")),
                RowFactory.create(1, Arrays.asList("I", "wish", "Java", "could", "use", "case", "classes")),
                RowFactory.create(2, Arrays.asList("Logistic", "regression", "models", "are", "neat"))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", IntegerType, false, Metadata.empty()),
                new StructField("words", DataTypes.createArrayType(StringType), false, Metadata.empty())
        });
        Dataset<Row> wordDataFrame = spark.createDataFrame(data, schema);

        NGram ngramTransformer = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");

        Dataset<Row> ngramDataFrame = ngramTransformer.transform(wordDataFrame);
        ngramDataFrame.show(false);
    }

    /**
     * 特征值 大于threshold 为1，小于threshold 为0
     */
    public void binarizer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.1),
                RowFactory.create(1, 0.8),
                RowFactory.create(2, 0.2)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", IntegerType, false, Metadata.empty()),
                new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> continuousDataFrame = spark.createDataFrame(data, schema);

        Binarizer binarizer = new Binarizer()
                .setInputCol("feature")
                .setOutputCol("binarized_feature")
                .setThreshold(0.5);

        Dataset<Row> binarizedDataFrame = binarizer.transform(continuousDataFrame);

        System.out.println("Binarizer output with Threshold = " + binarizer.getThreshold());
        binarizedDataFrame.show();
    }

    public void pca() {
        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
                RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
                RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(3)
                .fit(df);

        Dataset<Row> result = pca.transform(df).select("pcaFeatures");
        result.show(false);
    }

    public void polynomialExpansion() {
        PolynomialExpansion polyExpansion = new PolynomialExpansion()
                .setInputCol("features")
                .setOutputCol("polyFeatures")
                .setDegree(3);

        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.dense(2.0, 1.0)),
                RowFactory.create(Vectors.dense(0.0, 0.0)),
                RowFactory.create(Vectors.dense(3.0, -1.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Dataset<Row> polyDF = polyExpansion.transform(df);
        polyDF.show(false);
    }

    public void dct() {
        List<Row> data = Arrays.asList(
                RowFactory.create(Vectors.dense(0.0, 1.0, -2.0, 3.0)),
                RowFactory.create(Vectors.dense(-1.0, 2.0, 4.0, -7.0)),
                RowFactory.create(Vectors.dense(14.0, -2.0, -5.0, 1.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        DCT dct = new DCT()
                .setInputCol("features")
                .setOutputCol("featuresDCT")
                .setInverse(false);

        Dataset<Row> dctDf = dct.transform(df);

        dctDf.select("featuresDCT").show(false);
    }

    public void stringIndexer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("category", StringType, false)
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        StringIndexer indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex");

        Dataset<Row> indexed = indexer.fit(df).transform(df);
        indexed.show();
    }

    public void IndexToString() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        System.out.println("Transformed string column '" + indexer.getInputCol() + "' " +
                "to indexed column '" + indexer.getOutputCol() + "'");
        indexed.show();

        StructField inputColSchema = indexed.schema().apply(indexer.getOutputCol());
        System.out.println("StringIndexer will store labels in output column metadata: " +
                Attribute.fromStructField(inputColSchema).toString() + "\n");

        IndexToString converter = new IndexToString()
                .setInputCol("categoryIndex")
                .setOutputCol("originalCategory");
        Dataset<Row> converted = converter.transform(indexed);

        System.out.println("Transformed indexed column '" + converter.getInputCol() + "' back to " +
                "original string column '" + converter.getOutputCol() + "' using labels in metadata");
        converted.select("id", "categoryIndex", "originalCategory").show();
    }

    public void OneHotEncoder() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, "a"),
                RowFactory.create(1, "b"),
                RowFactory.create(2, "c"),
                RowFactory.create(3, "a"),
                RowFactory.create(4, "a"),
                RowFactory.create(5, "c")
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("category", DataTypes.StringType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("category")
                .setOutputCol("categoryIndex")
                .fit(df);
        Dataset<Row> indexed = indexer.transform(df);

        OneHotEncoder encoder = new OneHotEncoder()
                .setInputCol("categoryIndex")
                .setOutputCol("categoryVec");

        Dataset<Row> encoded = encoder.transform(indexed);
        encoded.show();
    }

    public void vectorIndexer() {
        Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

        VectorIndexer indexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexed")
                .setMaxCategories(10);
        VectorIndexerModel indexerModel = indexer.fit(data);

        Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
        System.out.print("Chose " + categoryMaps.size() + " categorical features:");

        for (Integer feature : categoryMaps.keySet()) {
            System.out.print(" " + feature);
        }
        System.out.println();

        Dataset<Row> indexedData = indexerModel.transform(data);
        indexedData.show(false);
    }

    public void interaction() {
        List<Row> data = Arrays.asList(
                RowFactory.create(1, 1, 2, 3, 8, 4, 5),
                RowFactory.create(2, 4, 3, 8, 7, 9, 8),
                RowFactory.create(3, 6, 1, 9, 2, 3, 6),
                RowFactory.create(4, 10, 8, 6, 9, 4, 5),
                RowFactory.create(5, 9, 2, 7, 10, 7, 3),
                RowFactory.create(6, 1, 1, 4, 2, 8, 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id1", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id2", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id3", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id4", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id5", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id6", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("id7", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(new String[]{"id2", "id3", "id4"})
                .setOutputCol("vec1");

        Dataset<Row> assembled1 = assembler1.transform(df);

        VectorAssembler assembler2 = new VectorAssembler()
                .setInputCols(new String[]{"id5", "id6", "id7"})
                .setOutputCol("vec2");

        Dataset<Row> assembled2 = assembler2.transform(assembled1).select("id1", "vec1", "vec2");

        Interaction interaction = new Interaction()
                .setInputCols(new String[]{"id1", "vec1", "vec2"})
                .setOutputCol("interactedCol");

        Dataset<Row> interacted = interaction.transform(assembled2);

        interacted.show(false);
    }

    public void Normalizer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 0.1, -8.0)),
                RowFactory.create(1, Vectors.dense(2.0, 1.0, -4.0)),
                RowFactory.create(2, Vectors.dense(4.0, 10.0, 8.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        Normalizer normalizer = new Normalizer()
                .setInputCol("features")
                .setOutputCol("normFeatures")
                .setP(1.0);

        Dataset<Row> l1NormData = normalizer.transform(dataFrame);
        l1NormData.show();

        Dataset<Row> lInfNormData =
                normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        lInfNormData.show();
    }

    public void StandardScaler() {
        Dataset<Row> dataFrame =
                spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

        StandardScaler scaler = new StandardScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setWithStd(true)
                .setWithMean(false);

        StandardScalerModel scalerModel = scaler.fit(dataFrame);

        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.show();
    }

    public void MinMaxScaler() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 0.1, -1.0)),
                RowFactory.create(1, Vectors.dense(2.0, 1.1, 1.0)),
                RowFactory.create(2, Vectors.dense(3.0, 10.1, 3.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures");

        MinMaxScalerModel scalerModel = scaler.fit(dataFrame);

        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        System.out.println("Features scaled to range: [" + scaler.getMin() + ", "
                + scaler.getMax() + "]");
        scaledData.select("features", "scaledFeatures").show();
    }

    public void MaxAbsScaler() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, Vectors.dense(1.0, 0.1, -8.0)),
                RowFactory.create(1, Vectors.dense(2.0, 1.0, -4.0)),
                RowFactory.create(2, Vectors.dense(4.0, 10.0, 8.0))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        MaxAbsScaler scaler = new MaxAbsScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures");

        MaxAbsScalerModel scalerModel = scaler.fit(dataFrame);

        Dataset<Row> scaledData = scalerModel.transform(dataFrame);
        scaledData.select("features", "scaledFeatures").show();
    }

    public void Bucketizer() {
        double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};

        List<Row> data = Arrays.asList(
                RowFactory.create(-999.9),
                RowFactory.create(-0.5),
                RowFactory.create(-0.3),
                RowFactory.create(0.0),
                RowFactory.create(0.2),
                RowFactory.create(999.9)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("features", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        Bucketizer bucketizer = new Bucketizer()
                .setInputCol("features")
                .setOutputCol("bucketedFeatures")
                .setSplits(splits);

        Dataset<Row> bucketedData = bucketizer.transform(dataFrame);

        System.out.println("Bucketizer output with " + (bucketizer.getSplits().length - 1) + " buckets");
        bucketedData.show();
    }

    public void ElementwiseProduct() {
        List<Row> data = Arrays.asList(
                RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
                RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
        );

        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("vector", new VectorUDT(), false));

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

        Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

        ElementwiseProduct transformer = new ElementwiseProduct()
                .setScalingVec(transformingVector)
                .setInputCol("vector")
                .setOutputCol("transformedVector");

        transformer.transform(dataFrame).show();
    }

    public void SQLTransformer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 1.0, 3.0),
                RowFactory.create(2, 2.0, 5.0)
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("v1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("v2", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        SQLTransformer sqlTrans = new SQLTransformer().setStatement(
                "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");

        sqlTrans.transform(df).show();
    }

    public void VectorAssembler() {
        StructType schema = createStructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("hour", IntegerType, false),
                createStructField("mobile", DoubleType, false),
                createStructField("userFeatures", new VectorUDT(), false),
                createStructField("clicked", DoubleType, false)
        });
        Row row = RowFactory.create(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0);
        Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row), schema);

        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
                .setOutputCol("features");

        Dataset<Row> output = assembler.transform(dataset);
        System.out.println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column " +
                "'features'");
        output.select("features", "clicked").show(false);
    }

    public void QuantileDiscretizer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 18.0),
                RowFactory.create(1, 19.0),
                RowFactory.create(2, 8.0),
                RowFactory.create(3, 5.0),
                RowFactory.create(4, 2.2)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("hour", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        QuantileDiscretizer discretizer = new QuantileDiscretizer()
                .setInputCol("hour")
                .setOutputCol("result")
                .setNumBuckets(3);

        Dataset<Row> result = discretizer.fit(df).transform(df);
        result.show();
    }

    public void Imputer() {
        List<Row> data = Arrays.asList(
                RowFactory.create(1.0, Double.NaN),
                RowFactory.create(2.0, Double.NaN),
                RowFactory.create(Double.NaN, 3.0),
                RowFactory.create(4.0, 4.0),
                RowFactory.create(5.0, 5.0)
        );
        StructType schema = new StructType(new StructField[]{
                createStructField("a", DoubleType, false),
                createStructField("b", DoubleType, false)
        });
        Dataset<Row> df = spark.createDataFrame(data, schema);

        Imputer imputer = new Imputer()
                .setInputCols(new String[]{"a", "b"})
                .setOutputCols(new String[]{"out_a", "out_b"});

        ImputerModel model = imputer.fit(df);
        model.transform(df).show();
    }
}
