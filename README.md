# Yelp_Hybrid_Recommender_System

The goal of this project is to create a hyrbid recommendation system using the Yelp dataset([here](https://www.yelp.com/dataset)) to recommend businesses to users.

# Data

# 1. Collaborative Filtering

Collaborative-Filtering (CF) Based Recommendation techniques have been quite successful in the past giving promising results in this domain. The main idea behind these set of algorithms is that the users with similar interests may like similar items. These algorithms unlike Content-Based approaches don’t require the content(features) of the items we want to recommend to users but instead only requires ratings various users have given to various items. Based on just this information, it is able to predict rating a user may give to some item it has not rated. Moreover, it helps in recommending new items to users which he may like.

### Prerequisites

### Instructions

# 2. Content Based

Content based recommendation systems try to find similarity between users and items by building their corresponding vectors. The most important aspect of such models is choosing features. We follow three different approaches to build user and item features. These features are transformed into vectors and then different similarity measures can be used to find compute the similarity between them. Unlike collaborative recommendation systems, in content-based models we used text in the review to form user and item vectors.

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

* Install nltk for pre-processing
```python
pip install nltk
```

### Instructions

* In order to run the code for any content based model give the path for `train_file` and `test_file` under `Load Train and Test Data`
* Change the parameters for `TfidfVectorizer` under `TF-IDF Vector Creation` or `Doc2Vec` under `Doc2Vec Model Creation`. (optional)
* Hypertune the parameters for regression under `Training Regressor on similarity values` if the data is changed. (optional)
* Give the path and name of file to save test results under `Save predictions file`.

# 3. Hybrid Recommender System
