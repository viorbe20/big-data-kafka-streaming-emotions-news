from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputClassifier
from sklearn.pipeline import Pipeline
from sklearn.svm import SVC
import nltk
import pandas as pd
import pickle
import re

# Download the 'punkt' tokenizer model from the NLTK package
nltk.download('punkt')

# Download the list of stopwords from the NLTK package
nltk.download('stopwords')

# Create a set of English stop words for filtering out common words in text processing
stop_words = set(stopwords.words('spanish'))

# Function to update the emotion columns
def update_emotion_columns(row):
    emotion_string = row['emotion']
    if len(emotion_string) > 0:
        if len(emotion_string) == 1 and emotion_string in emotions:
            row[emotion_string] = 1
        elif len(emotion_string) > 1:
            for char in emotion_string:
                if char in emotions:
                    row[char] = 1
    return row

def preprocess_text(text):
    # Tokenize the text and convert to lower case
    tokens = word_tokenize(text.lower())

    # Filter out tokens that are not alphabetic and are in the stop words list
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]

    # Join the filtered tokens back into a string
    return " ".join(filtered_tokens)

# Load the CSV file
df = pd.read_csv("spanish-emotions-dataset.csv", sep='\t', header=None, names=['id', 'text', 'author'])

# Use a lambda function to remove numbers and keep only letters
df['emotion'] = df['id'].apply(lambda x: re.sub(r'\d+', '', x))
df = df[['text', 'emotion']]

# Emotions to check
emotions = ['A', 'F', 'H', 'L', 'S']
for emotion in emotions:
    df[emotion] = 0

# Apply the function to each row of the DataFrame
df = df.apply(update_emotion_columns, axis=1)

# Tokenization and stopword removal
stop_words = set(stopwords.words('spanish'))

# Apply preprocessing to the DataFrame
df['processed_text'] = df['text'].apply(preprocess_text)

X = df['processed_text']
y = df[emotions]

# Split data into training and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# TF-IDF Vectorization
tfidf_vectorizer = TfidfVectorizer()
X_train_tfidf = tfidf_vectorizer.fit_transform(X_train)
X_test_tfidf = tfidf_vectorizer.transform(X_test)

# Define the base SVM classifier
base_svm = SVC()

# Define the pipeline with TF-IDF vectorizer and multilabel SVM classifier
pipeline = Pipeline([
    ('tfidf', TfidfVectorizer()),
    ('svm', MultiOutputClassifier(base_svm))
])

# Define parameters for hyperparameter tuning
param_grid = {
    'svm__estimator__C': [0.1, 1, 10, 50],
    'svm__estimator__gamma': [0.1, 0.01, 0.001, 0.0001],
    'svm__estimator__kernel': ['rbf', 'poly', 'sigmoid']
}

# Perform hyperparameter tuning using GridSearchCV
grid_search = GridSearchCV(pipeline, param_grid, cv=5)
grid_search.fit(X_train, y_train)

# Get the best parameters found
best_params = grid_search.best_params_

# Define the base SVM classifier with the best parameters
best_svm = SVC(C=best_params['svm__estimator__C'],
    gamma=best_params['svm__estimator__gamma'],
    kernel=best_params['svm__estimator__kernel'])

# Create a multilabel classifier using MultiOutputClassifier
multi_label_svm = MultiOutputClassifier(best_svm)

# Train the multilabel model with the training data
multi_label_svm.fit(X_train_tfidf, y_train)

# Save the model and the vectorizer to a pickle file
with open('spanish-emotions-model.pkl', 'wb') as model_file:
    pickle.dump((multi_label_svm, tfidf_vectorizer), model_file)

# Confirm that the model has been successfully saved
print("Model successfully saved to spanish-emotions-model.pkl")