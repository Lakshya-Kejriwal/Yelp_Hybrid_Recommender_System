# Hybrid_Recommender_System

The goal of this project is to create a hyrbid recommendation system using the Yelp dataset([here](https://www.yelp.com/dataset)) to recommend businesses to users.

# 1. Collaborative Filtering

# 2. Content Based

### Prerequisites

* Numpy
* Pandas
* Scikit-learn

* Install `findspark` using the following command
```python
pip install findspark
```

* Install Gensim for doc2vec
```python
pip install gensim
```
### Instructions

* In order to run the code for any content based model give the path for `train_file` and `test_file` under `Load Train and Test Data`
* Change the parameters for `TfidfVectorizer` under `TF-IDF Vector Creation` or `Doc2Vec` under `Doc2Vec Model Creation`. (optional)
* Hypertune the parameters for regression under `Training Regressor on similarity values` if the data is changed. (optional)
