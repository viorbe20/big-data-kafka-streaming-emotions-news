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

# Download the 'punkt' tokenizer model from the NLTK package
nltk.download('punkt')

# Download the list of stopwords from the NLTK package
nltk.download('stopwords')

# Create a set of English stop words for filtering out common words in text processing
stop_words = set(stopwords.words('english'))

def preprocess_text(text):
    # Tokenize the text and convert to lower case
    tokens = word_tokenize(text.lower())

    # Filter out tokens that are not alphabetic and are in the stop words list
    filtered_tokens = [token for token in tokens if token.isalpha() and token not in stop_words]

    # Join the filtered tokens back into a string
    return " ".join(filtered_tokens)

# Load the dataset
df = pd.read_csv("english-emotions-dataset.csv", sep=',')

# Delete rows
df = df.drop(labels=range(200000), axis=0)

# Select only the necessary columns for further analysis and store them in a new DataFrame
columns_to_keep = ['text', 'anger', 'fear', 'joy', 'love', 'sadness']

df_filtered = df[columns_to_keep]

# Define a dictionary to map the old emotion column names to new shorter ones
new_column_names = {
    'anger': 'A',
    'fear': 'F',
    'joy': 'H',
    'love': 'L',
    'sadness': 'S'
}

df_renamed = df_filtered.rename(columns=new_column_names)

# Update the dictionary to include a renaming of the 'text' column to 'emotion' if desired
new_column_names['average'] = 'emotion'

# Apply the renaming to the DataFrame, using the updated dictionary
df = df_filtered.rename(columns=new_column_names)

# Apply the preprocessing function to the 'text' column and store the results in a new column 'processed_text'
df['processed_text'] = df['text'].apply(preprocess_text)

# Prepare the feature and target variables for the machine learning model
X = df['processed_text']  # Feature variable containing preprocessed text
y = df[['A', 'F', 'H', 'L', 'S']]  # Target variables representing different emotions

# Split the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_features = train_test_split(X, y, test_size=0.2, random_state=42)

# Initialize the TF-IDF Vectorizer
tfidf_vectorizer = TfidfVectorizer()

# Transform the training data to a matrix of TF-IDF features
X_train_tfidf = tfidf_vectorizer.fit_transform(X_train)

# Transform the testing data to a matrix of TF-IDF features
X_test_tfidf = tfidf_vectorizer.transform(X_test)

# Initialize the base SVM classifier
base_svm = SVC()

# Define a pipeline that incorporates the TF-IDF vectorizer and a multi-label SVM classifier
pipeline = Pipeline([
    ('tfidf', TfidfVectorizer()),  # You can adjust TfidfVectorizer parameters here
    ('svm', MultiOutputClassifier(base_svm))
])

# Set up the hyperparameter grid for tuning the SVM classifier
param_grid = {
    'svm__estimator__C': [0.1, 1, 10, 50],  # Regularization parameter
    'svm__estimator__gamma': [0.1, 0.01, 0.001, 0.0001],  # Kernel coefficient for ‘rbf’, ‘poly’ and ‘sigmoid’
    'svm__estimator__kernel': ['rbf', 'poly', 'sigmoid']  # Specifies the kernel type to be used in the SVM
}

# Perform hyperparameter search using GridSearchCV
grid_search = GridSearchCV(pipeline, param_grid, cv=5)
grid_search.fit(X_train, y_train)

# Retrieve and print the best parameters found by GridSearchCV
best_params = grid_search.best_params_

# Initialize the SVM classifier with specific hyperparameters
best_svm = SVC(C=100, gamma=0.01, kernel='rbf')

# Create a multi-label classifier using the OneVsRest strategy wrapped around the optimized SVM
multi_label_svm = MultiOutputClassifier(best_svm)

# Train the multi-label SVM model with the TF-IDF transformed training data
multi_label_svm.fit(X_train_tfidf, y_train)

# Save the trained multi-label SVM model and the TF-IDF vectorizer to a pickle file for later use
with open('english-emotions-model.pkl', 'wb') as model_file:
    pickle.dump((multi_label_svm, tfidf_vectorizer), model_file)

# Confirm that the model has been successfully saved
print("Model successfully saved to english-emotions-model.pkl")
