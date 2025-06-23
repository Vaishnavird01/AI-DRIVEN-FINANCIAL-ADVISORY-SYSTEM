# AI-DRIVEN-FINANCIAL-ADVISORY-SYSTEM

## Project Overview  
This project presents an **AI-powered financial advisory platform** that uses **Natural Language Processing (NLP)** and **Big Data technologies** to analyze financial news headlines and deliver **personalized investment recommendations**.

Using **PySpark on Databricks**, the system processes a large-scale dataset from Kaggle to:
- Perform **sentiment analysis** on financial headlines  
- Classify sentiments using **Decision Tree** and **Random Forest** models  
- Recommend actions like *hold*, *invest more*, or *withdraw* based on sentiment polarity

---

## Motivation  
The financial sector is undergoing a transformation powered by **AI and Big Data**. Traditional methods often lack agility in real-time sentiment shifts. Our system bridges that gap using **NLP + ML**, empowering investors with context-aware, data-backed financial decisions.

---

## Dataset Used  
- **Source**: [News Category Dataset – Kaggle](https://www.kaggle.com)  
- **Size**: 209,527 rows × 6 columns  
- **Date Range**: Jan 2012 – Sep 2022  
- **Key Features**: Headline, Category, Short Description, Author, Date  
- **Target Categories**: POLITICS, WELLNESS, ENTERTAINMENT, TRAVEL, STYLE & BEAUTY  

---

## Tech Stack

| Layer           | Tools & Libraries                     |
|----------------|----------------------------------------|
| Big Data        | PySpark (Databricks)                  |
| NLP             | Custom pipeline + Sentiment Scoring   |
| ML Models       | Decision Tree, Random Forest          |
| Language        | Python                                |
| Data Handling   | Spark DataFrames                      |

---

## Process Breakdown

### 1. **Data Preprocessing**
- Removed nulls & handled skewed category distribution
- Computed sentiment scores (-1.0 to 1.0) for each headline
- Analyzed average headline and description lengths

### 2. **Natural Language Processing**
- Applied sentiment analysis on headlines
- Used NLP to assign sentiment: **Positive, Neutral, Negative**
- Scores drive downstream ML predictions

### 3. **Model Processing**
#### Decision Tree
- Non-parametric supervised model
- Recursively partitions data based on sentiment features  
- Accuracy: **Training – 100% | Test – 91.82%**

#### Random Forest
- Ensemble learning method combining multiple trees
- Handles high-dimensional, non-linear text data  
- Accuracy: **Training – 100% | Test – 89.82%**

---

## Sample Recommendations

| Headline Sentiment | Score  | Advice                   |
|--------------------|--------|--------------------------|
| Negative           | < 0    | Reduce investment        |
| Neutral            | ~0     | Hold                     |
| Positive           | > 0    | Increase investment      |

---

## 📈 Results

| Model          | Accuracy (Test) | Strengths                          |
|----------------|-----------------|------------------------------------|
| Decision Tree  | 91.82%          | Interpretability, fast inference   |
| Random Forest  | 89.82%          | Robustness, handles complexity     |

>  Both models offered high precision in translating sentiment into **actionable investment advice**.

---

## Future Scope

-  Integrate **deep learning (LSTM, Transformers)** for sentiment & trend prediction  
-  Incorporate **time-series forecasting** for market prediction  
-  Personalize recommendations using **user financial profiles**  
-  Enable **real-time advisory** using streaming data pipelines (Kafka, Spark Streaming)  


> 💬 “Empowering financial decisions through language understanding.”


