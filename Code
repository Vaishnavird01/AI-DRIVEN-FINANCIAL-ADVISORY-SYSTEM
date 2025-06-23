from pyspark.sql import SparkSession

df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

display(df)

from pyspark.sql.functions import col, to_date, lit
from pyspark.sql import SparkSession

df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count

# Initialize Spark session
spark = SparkSession.builder.appName("Data Processing Application").getOrCreate()

# Example of loading data and using to_date, col, and count
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Assuming there's a 'date' column in the original CSV in the format 'yyyy-MM-dd'
df = df.withColumn("Date", to_date(col("date"), "yyyy-MM-dd"))

# Example of using the count function in an aggregation after grouping by 'Date'
daily_counts = df.groupBy("Date").agg(count("date").alias("TotalCount"))

daily_counts_pd = daily_counts.toPandas()

# Show the results
daily_counts.show()

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, count

# Initialize Spark session
spark = SparkSession.builder.appName("Data Processing Application").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Convert the 'date' column to a date type
df = df.withColumn("Date", to_date(col("date"), "yyyy-MM-dd"))

# Aggregate count for each date
daily_counts = df.groupBy("Date").agg(count("date").alias("TotalCount"))

# Convert the daily_counts DataFrame to a Pandas DataFrame
daily_counts_pd = daily_counts.toPandas()

# Set 'Date' column as the index
daily_counts_pd.set_index('Date', inplace=True)

# Sort the DataFrame by the index (Date)
daily_counts_pd.sort_index(inplace=True)

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(daily_counts_pd.index, daily_counts_pd['TotalCount'], label='Number of Headlines')
plt.title('Number of Headlines Per Day')
plt.xlabel('Date')
plt.ylabel('Number of Headlines')
plt.legend()
plt.grid(True)
plt.show()

from pyspark.sql import SparkSession
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' exists and has data
if 'category' in df.columns:
    pandas_df = df.select("category").toPandas()
    print(pandas_df['category'].value_counts())

    # Check if DataFrame is not empty
    if not pandas_df.empty:
        # Basic text preprocessing if necessary
        pandas_df['category'] = pandas_df['category'].astype(str).str.lower().str.replace('[^\w\s]', '')

        # Attempt to vectorize with TfidfVectorizer
        vectorizer = TfidfVectorizer(stop_words=None)  # Consider removing stop_words restriction or customizing the list
        try:
            X_tfidf = vectorizer.fit_transform(pandas_df['category'])
            print(f"TF-IDF Matrix shape: {X_tfidf.shape}")
        except ValueError as e:
            print(f"Error with TF-IDF: {str(e)} - Possibly due to insufficient textual content.")

        # Use OneHotEncoder for categorical data
        encoder = OneHotEncoder(sparse=False)
        category_encoded = encoder.fit_transform(pandas_df[['category']])
        print(f"OneHotEncoded Data shape: {category_encoded.shape}")
    else:
        print("DataFrame is empty after selection.")
else:
    print("Column 'category' does not exist in DataFrame.")

import matplotlib.pyplot as plt

# Verify that 'category' exists and has data
if 'category' in df.columns:
    pandas_df = df.select("category").toPandas()

    # Check if DataFrame is not empty
    if not pandas_df.empty:
        # Get the top 15 categories
        top_categories = pandas_df['category'].value_counts().nlargest(50)

        # Plot histogram of top 15 categories
        plt.figure(figsize=(20, 12))
        top_categories.plot(kind='bar')
        plt.xlabel('Category')
        plt.ylabel('Count')
        plt.title('Top 50 Categories by Count')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()
    else:
        print("DataFrame is empty after selection.")
else:
    print("Column 'category' does not exist in DataFrame.")

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Data Processing").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' exists and has data
if 'category' in df.columns:
    # Filter for rows where the category is 'MONEY' and count them
    money_count = df.filter(df.category == "MONEY").count()
    print(f"Number of entries for 'MONEY': {money_count}")
else:
    print("Column 'category' does not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, window, lit

# Create or get the Spark session
spark = SparkSession.builder.appName("Daily Counts").getOrCreate()

# Convert '_c5' to a datetime column using to_timestamp (assuming df is your DataFrame and '_c5' is the datetime column)
df = df.withColumn('date', to_timestamp(col('date')))

# Filter for entries with the "MONEY" category
df_money = df.filter(df.category == "MONEY")

# Adding a count column
df_money = df_money.withColumn('Count', lit(1))

# Group by day using window function
daily_counts = df_money.groupBy(window(col('date'), "1 day")).count()

# Rename the columns for clarity
daily_counts = daily_counts.select(
    col("window.start").alias("Day_Start"),
    col("window.end").alias("Day_End"),
    col("count").alias("Total_Count")
)

# Order by Total_Count in descending order
daily_counts = daily_counts.orderBy(col("Total_Count").desc())

# Show the result
daily_counts.show()

import matplotlib.pyplot as plt
import pandas as pd

# Convert daily_counts DataFrame to Pandas DataFrame
daily_counts_pd = daily_counts.toPandas()

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(daily_counts_pd['Total_Count'], daily_counts_pd['Day_Start'], marker='o', linestyle='-')
plt.title('Daily Counts of "MONEY" Category')
plt.xlabel('Total Count')
plt.ylabel('Date')
plt.yticks(rotation=45)
plt.grid(True)
plt.tight_layout()
plt.show()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, isnan, when, count
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Sentiment Analysis for MONEY Category").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'headline' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Check if there is data in the filtered DataFrame
    if df_money.count() > 0:
        # Define a Pandas UDF to apply sentiment analysis
        @pandas_udf('double', PandasUDFType.SCALAR)
        def get_sentiment(texts):
            # Handling None or empty strings to avoid errors in TextBlob
            return texts.apply(lambda text: TextBlob(text).sentiment.polarity if text else 0.0)

        # Apply the sentiment analysis UDF to the 'headline' column
        df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

        # Display some results and check for any NaN or NULL values in sentiment
        df_money.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in ["sentiment"]]).show()

        # Calculate average sentiment
        try:
            average_sentiment = df_money.agg({'sentiment': 'mean'}).collect()[0][0]
            print(f"Average Sentiment for 'FINANCE' headlines: {average_sentiment}")
        except Exception as e:
            print(f"Failed to calculate average sentiment: {e}")

        # Show some entries for verification
        df_money.show()
    else:
        print("No data available for 'MONEY' category.")
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Random Forest Financial Sentiment Analysis").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'headline' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Define a Pandas UDF to apply sentiment analysis
    @pandas_udf('double', PandasUDFType.SCALAR)
    def get_sentiment(texts):
        # Handling None or empty strings to avoid errors in TextBlob
        return texts.apply(lambda text: TextBlob(text if text is not None else '').sentiment.polarity)

    # Apply the sentiment analysis UDF to the 'headline' column
    df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

    # Define labels based on sentiment: 1 for positive, 0 for negative or neutral
    df_money = df_money.withColumn("label", 
                                   when(col("sentiment") > 0, 1)  # Increase Investment
                                   .when(col("sentiment") < 0, 0)  # Reduce Investment
                                   .otherwise(0))  # Hold

    # Assemble features for the Random Forest Classifier
    assembler = VectorAssembler(inputCols=["sentiment"], outputCol="features")

    # Random Forest Classifier
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=100, maxDepth=5)

    # Pipeline: Vector Assembler -> Random Forest
    pipeline = Pipeline(stages=[assembler, rf])

    # Split the data into training and test sets (70% training, 30% testing)
    train_data, test_data = df_money.randomSplit([0.7, 0.3], seed=42)

    # Train the model on the training data
    model = pipeline.fit(train_data)

    # Make predictions on both the training data and test data
    train_predictions = model.transform(train_data)
    test_predictions = model.transform(test_data)

    # Evaluate the model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    train_accuracy = evaluator.evaluate(train_predictions)
    test_accuracy = evaluator.evaluate(test_predictions)
    print(f"Training Accuracy: {train_accuracy * 100:.2f}%")
    print(f"Test Accuracy: {test_accuracy * 100:.2f}%")
    test_predictions.select("headline", "sentiment", "prediction").show(10)

else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Financial Sentiment-Based Advice Prediction").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'short_description' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Check if DataFrame is empty
    if df_money.count() > 0:
        # Define a Pandas UDF to apply sentiment analysis
        @pandas_udf('double', PandasUDFType.SCALAR)
        def get_sentiment(texts):
            # Ensure text is not None
            return texts.fillna('').apply(lambda text: TextBlob(text).sentiment.polarity)

        # Apply the sentiment analysis UDF to the 'short_description' column
        df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

        # Assume binary classification: 1 if sentiment is positive, 0 otherwise
        df_money = df_money.withColumn("label", (col("sentiment") > 0).cast("integer"))

        # Assemble features (assuming only 'sentiment' for simplicity)
        assembler = VectorAssembler(inputCols=["sentiment"], outputCol="features")

        # Random Forest classifier
        rf = RandomForestClassifier(featuresCol="features", labelCol="label")

        # Pipeline: Vector Assembler -> Random Forest
        pipeline = Pipeline(stages=[assembler, rf])

        # Split data into training and testing
        train_data, test_data = df_money.randomSplit([0.7, 0.3], seed=42)

        # Train the model on the training data
        model = pipeline.fit(train_data)

        # Make predictions on test data
        predictions = model.transform(test_data)

        # Evaluate model
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        test_accuracy = evaluator.evaluate(predictions)
        print(f"Test Accuracy: {test_accuracy * 100:.2f}%")

        # Output prediction summary based on average sentiment
        average_sentiment = predictions.agg({'sentiment': 'mean'}).collect()[0][0]
        if average_sentiment > 0:
            advice = "Increase Investment."
        else:
            advice = "Hold or Reduce Investment."

        print(f"Average Sentiment for 'FINANCE' short_descriptions: {average_sentiment}")
        print(advice)
    else:
        print("No data available for 'FINANCE' category.")
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, to_date, udf
from pyspark.sql.types import StringType
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Detailed Sentiment-Based Financial Advice").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category', 'short_description', and 'date' columns exist
if 'category' in df.columns and 'short_description' in df.columns and 'date' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Convert the 'date' column to a date type, adjust the format string as needed
    df_money = df_money.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

    # Define a Pandas UDF to apply sentiment analysis
    @pandas_udf('double', PandasUDFType.SCALAR)
    def get_sentiment(texts):
        return texts.apply(lambda text: TextBlob(text).sentiment.polarity)

    # Apply the sentiment analysis UDF to the 'short_description' column
    df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

    # Define a UDF to convert sentiment to financial advice
    def sentiment_to_advice(sentiment):
        if sentiment > 0:
            return "Increase Investment"
        else:
            return "Hold or Reduce Investment"

    advice_udf = udf(sentiment_to_advice, StringType())

    # Apply the advice UDF based on the sentiment
    df_money = df_money.withColumn("Finance_advice", advice_udf(col("sentiment")))

    # Display the results with date, short_description, sentiment, and financial advice
    df_money.select("date", "short_description", "sentiment", "Finance_advice").show(truncate=False)
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, udf
from pyspark.sql.types import StringType
from textblob import TextBlob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder.appName("Sentiment-Based Financial Advice Visualization").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'short_description' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Define a Pandas UDF to apply sentiment analysis
    @pandas_udf('double', PandasUDFType.SCALAR)
    def get_sentiment(texts):
        def analyze_sentiment(text):
            if text and isinstance(text, str):
                return TextBlob(text).sentiment.polarity
            else:
                return None
        return texts.apply(analyze_sentiment)

    # Apply the sentiment analysis UDF to the 'short_description' column
    df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

    # Define a UDF to convert sentiment to financial advice
    def sentiment_to_advice(sentiment):
        if sentiment is not None:
            if sentiment > 0:
                return "Increase Investment"
            else:
                return "Hold or Reduce Investment"
        else:
            return "Unknown"

    advice_udf = udf(sentiment_to_advice, StringType())

    # Apply the advice UDF based on the sentiment
    df_money = df_money.withColumn("Finance_advice", advice_udf(col("sentiment")))

    # Convert to Pandas DataFrame for visualization
    pd_df = df_money.select("short_description", "sentiment", "Finance_advice").toPandas()

    # Plotting
    plt.figure(figsize=(20, 12))
    # Create a numeric index for x-axis
    pd_df['index'] = range(1, len(pd_df) + 1)
    sns.scatterplot(x='index', y='sentiment', hue='Finance_advice', style='Finance_advice', data=pd_df, s=100)
    plt.title('short_description Sentiment and Financial Advice')
    plt.xlabel('short_description Index')
    plt.ylabel('Sentiment Score')
    plt.legend(title='Financial Advice')
    plt.grid(True)
    plt.xticks([])  
    plt.show()
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Decision Tree Financial Advice Accuracy Assessment").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'short_description' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY").na.drop(subset=["short_description"])  # Dropping rows where short_description is null

    if df_money.count() > 0:
        # Define a Pandas UDF to apply sentiment analysis
        @pandas_udf('double', PandasUDFType.SCALAR)
        def get_sentiment(texts):
            return texts.apply(lambda text: TextBlob(text if text is not None else '').sentiment.polarity)

        # Apply the sentiment analysis UDF to the 'short_description' column
        df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

        # Define the labels based on sentiment
        df_money = df_money.withColumn("label", 
                                            when(col("sentiment") > 0, 1)  # Increase Investment
                                            .when(col("sentiment") < 0, 0)  # Reduce Investment
                                            .otherwise(0))  # Hold

        # Assemble features for Decision Tree Classifier
        assembler = VectorAssembler(inputCols=["sentiment"], outputCol="features")

        # Decision Tree Classifier
        dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")

        # Pipeline: Vector Assembler -> Decision Tree
        pipeline = Pipeline(stages=[assembler, dt])

        # Split the data into training and test sets
        train_data, test_data = df_money.randomSplit([0.8, 0.2], seed=42)

        # Train the model on the training data
        model = pipeline.fit(train_data)

        # Make predictions on the training data and test data
        train_predictions = model.transform(train_data)
        test_predictions = model.transform(test_data)

        # Evaluate the model
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        train_accuracy = evaluator.evaluate(train_predictions)
        test_accuracy = evaluator.evaluate(test_predictions)
        print(f"Training Accuracy: {train_accuracy * 100:.2f}%")
        print(f"Test Accuracy: {test_accuracy * 100:.2f}%")

        # Display some predictions
        test_predictions.select("short_description", "sentiment", "prediction").show(10)
    else:
        print("No data available after filtering for 'MONEY'.")
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Financial Sentiment-Based Advice Prediction with Decision Tree").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'short_description' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY' and short_descriptions are not null
    df_money = df.filter((df.category == "MONEY") & (df.short_description.isNotNull()))

    # Define a Pandas UDF to apply sentiment analysis
    @pandas_udf('double', PandasUDFType.SCALAR)
    def get_sentiment(texts):
        return texts.apply(lambda text: TextBlob(text if text is not None else '').sentiment.polarity)

    # Apply the sentiment analysis UDF to the 'short_description' column
    df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

    # Assume binary classification: 1 if sentiment is positive, 0 otherwise
    df_money = df_money.withColumn("label", (col("sentiment") > 0).cast("integer"))

    # Assemble features (assuming only 'sentiment' for simplicity)
    assembler = VectorAssembler(inputCols=["sentiment"], outputCol="features")

    # Decision Tree Classifier
    dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")

    # Pipeline: Vector Assembler -> Decision Tree
    pipeline = Pipeline(stages=[assembler, dt])

    # Split the data into training and test sets (70% training, 30% testing)
    train_data, test_data = df_money.randomSplit([0.7, 0.3], seed=42)
    train_data.cache()  # Cache the training data

    # Train the model on the training data
    model = pipeline.fit(train_data)

    # Make predictions on the test data
    predictions = model.transform(test_data)

    # Evaluate model
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    test_accuracy = evaluator.evaluate(predictions)
    print(f"Test Accuracy: {test_accuracy * 100:.2f}%")

    # Output prediction summary based on average sentiment
    average_sentiment = predictions.agg({'sentiment': 'mean'}).collect()[0][0]
    if average_sentiment > 0:
        advice = "Increase Investment"
    else:
        advice = "Hold or Reduce Investment"

    print(f"Average Sentiment for 'MONEY' short_descriptions: {average_sentiment}")
    print(advice)
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, to_date, when, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from textblob import TextBlob
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("Decision Tree Based Financial Advice").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category', 'short_description', and 'date' columns exist
if 'category' in df.columns and 'short_description' in df.columns and 'date' in df.columns:
    # Filter for rows where the category is 'MONEY' and short_descriptions are not null
    df_money = df.filter((df.category == "MONEY") & (df.short_description.isNotNull()))

    if df_money.count() > 0:
        # Convert the 'date' column to a date type, assuming format is correct
        df_money = df_money.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))

        # Define a Pandas UDF to apply sentiment analysis
        @pandas_udf('double', PandasUDFType.SCALAR)
        def get_sentiment(texts):
            return texts.apply(lambda text: TextBlob(text if text is not None else '').sentiment.polarity)

        # Apply the sentiment analysis UDF to the 'short_description' column
        df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

        # Assume binary classification: 1 if sentiment is positive, 0 if negative, 2 if neutral
        df_money = df_money.withColumn("label", 
                                             when(col("sentiment") > 0, 1).otherwise(
                                                 when(col("sentiment") < 0, 0).otherwise(0)))

        # Assemble features for Decision Tree Classifier
        assembler = VectorAssembler(inputCols=["sentiment"], outputCol="features")

        # Decision Tree Classifier
        dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")

        # Pipeline: Vector Assembler -> Decision Tree
        pipeline = Pipeline(stages=[assembler, dt])

        # Train the model on the entire dataset
        model = pipeline.fit(df_money)

        # Make predictions
        predictions = model.transform(df_money)

        # Register UDF for converting labels to advice
        advice_udf = udf(lambda label: "Incease Investment" if label == 1 else "Hold or Reduce Investment", StringType())

        # Apply the advice UDF to convert labels to human-readable advice
        predictions = predictions.withColumn("Finance_advice", advice_udf(col("label")))

        # Evaluate model
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print(f"Test Accuracy: {accuracy * 100:.2f}%")

        # Display results
        predictions.select("date", "short_description", "sentiment", "Finance_advice").show(truncate=False)
    else:
        print("No data available after filtering for 'MONEY'.")
else:
    print("Required columns do not exist in DataFrame.")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType, udf
from pyspark.sql.types import StringType
from textblob import TextBlob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder.appName("Sentiment-Based Financial Advice Visualization").getOrCreate()

# Load data from a CSV file into a Spark DataFrame
df = spark.read.csv("/FileStore/tables/News_Category_Dataset.csv", header=True, inferSchema=True)

# Verify that 'category' and 'short_description' columns exist
if 'category' in df.columns and 'short_description' in df.columns:
    # Filter for rows where the category is 'MONEY'
    df_money = df.filter(df.category == "MONEY")

    # Define a Pandas UDF to apply sentiment analysis
    @pandas_udf('double', PandasUDFType.SCALAR)
    def get_sentiment(texts):
        def analyze_sentiment(text):
            if text and isinstance(text, str):
                return TextBlob(text).sentiment.polarity
            else:
                return None
        return texts.apply(analyze_sentiment)

    # Apply the sentiment analysis UDF to the 'short_description' column
    df_money = df_money.withColumn("sentiment", get_sentiment(col("short_description")))

    # Define a UDF to convert sentiment to financial advice
    def sentiment_to_advice(sentiment):
        if sentiment is not None:
            if sentiment > 0:
                return "Increase Investment"
            else:
                return "Hold or Reduce Investment"
        else:
            return "Unknown"

    advice_udf = udf(sentiment_to_advice, StringType())

    # Apply the advice UDF based on the sentiment
    df_money = df_money.withColumn("Finance_advice", advice_udf(col("sentiment")))

    # Convert to Pandas DataFrame for visualization
    pd_df = df_money.select("short_description", "sentiment", "Finance_advice").toPandas()

    # Plotting
    plt.figure(figsize=(20, 12))
    # Create a numeric index for x-axis
    pd_df['index'] = range(1, len(pd_df) + 1)
    sns.scatterplot(x='index', y='sentiment', hue='Finance_advice', style='Finance_advice',
                    palette={'Increase Investment': 'blue', 'Hold or Reduce Investment': 'green', 'Unknown': 'gray'},
                    data=pd_df, s=100)
    plt.title('short_description Sentiment and Predicted Financial Advice')
    plt.xlabel('short_description Index')
    plt.ylabel('Sentiment Score')
    plt.legend(title='Financial Advice')
    plt.grid(True)
    plt.xticks([])  
    plt.show()
else:
    print("Required columns do not exist in DataFrame.")

import matplotlib.pyplot as plt
import numpy as np

# Data
classifiers = ['Random Forest', 'Decision Tree']
train_accuracies = [100, 100]  
test_accuracies = [99.63, 100]   

x = np.arange(len(classifiers))  # the label locations
width = 0.35  # the width of the bars

fig, ax = plt.subplots()
rects1 = ax.bar(x - width/2, train_accuracies, width, label='Training Accuracy')
rects2 = ax.bar(x + width/2, test_accuracies, width, label='Test Accuracy')

# Add some text for labels, title, and custom x-axis tick labels, etc.
ax.set_xlabel('Classifiers')
ax.set_ylabel('Accuracy (%)')
ax.set_title('Training and Test Accuracy by Classifier')
ax.set_xticks(x)
ax.set_xticklabels(classifiers)
ax.legend()

ax.bar_label(rects1, padding=3)
ax.bar_label(rects2, padding=3)

fig.tight_layout()

plt.show()

import matplotlib.pyplot as plt
import numpy as np

# Data
classifiers = ['Random Forest', 'Decision Tree']
train_accuracies = [100, 100]  
test_accuracies = [99.63, 100]
x = np.arange(len(classifiers))  # the label locations

fig, ax = plt.subplots()
# Creating a line plot with markers
train_line = ax.plot(x, train_accuracies, label='Training Accuracy', marker='o', markersize=10, linestyle='-', linewidth=2)
test_line = ax.plot(x, test_accuracies, label='Test Accuracy', marker='s', markersize=10, linestyle='-', linewidth=2)

# Adding some text for labels, title and custom x-axis tick labels, etc.
ax.set_xlabel('Classifiers')
ax.set_ylabel('Accuracy (%)')
ax.set_title('Comparison of Training and Test Accuracy for Classifiers')
ax.set_xticks(x)
ax.set_xticklabels(classifiers)
ax.legend()

# Setting grid for better readability
plt.grid(True, linestyle='--', which='major', color='grey', alpha=.25)

# Rotate the tick labels for better readability
plt.setp(ax.get_xticklabels(), rotation=30, horizontalalignment='right')

# Show plot
plt.show()
