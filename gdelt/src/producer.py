import json
import time
from faker import Faker
import random
from googletrans import Translator
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Initialize Faker for English and Translator
fake_en = Faker('en_US')  # Use en_US to avoid Latin
translator = Translator()

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers='192.168.11.10:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of languages and their respective domains
languages = {
    'spanish': ('es', 'es'),
    'english': ('uk', 'en')
}

# URLs for English and Spanish news

urls_en = [
    "http://skynews.uk",
    "http://bbc.uk",
    "http://theguardian.uk",
    "http://telegraph.uk",
    "http://independent.uk",
    "http://times.uk",
    "http://mirror.uk",
    "http://sun.uk",
    "http://express.uk",
    "http://newsnow.uk"
]

urls_es = [
    "http://elpais.es",
    "http://elmundo.es",
    "http://abc.es",
    "http://laVanguardia.es",
    "http://elconfidencial.es",
    "http://elpuntavui.es",
    "http://elperiodico.es",
    "http://20minutos.es",
    "http://marca.es",
    "http://as.es"
]

# Function to translate text
def translate_text(text, lang_code):
    try:
        translated = translator.translate(text, dest=lang_code)
        return translated.text
    except Exception as e:
        print(f"Translation error: {e}")
        return text

# Generate data interval
start_date = datetime(2020, 1, 1)  # Start date: January 1, 2020
end_date = datetime.now()          # End date: Current date

def random_date(start, end):
    """Generate a random date within a given range."""
    delta = end - start
    random_days = random.randrange(delta.days)
    random_date = start + timedelta(days=random_days)
    return random_date.isoformat()  # Use .isoformat() to return the date in ISO 8601 string format

# Generate synthetic data based on the template
def generate_synthetic_data():

    synthetic_data = []

    for _ in range(5):
        lang, (domain, locale) = random.choice(list(languages.items()))
        quote = fake_en.sentence()
        title = fake_en.sentence(nb_words=6)

        # Translate text if the language is Spanish
        if lang == 'spanish':
            pre = translate_text(quote, 'es')
            quote = translate_text(quote, 'es')
            title = translate_text(title, 'es')
            url = random.choice(urls_es)
            post = translate_text(quote, 'es')

        # Translate text if the language is English
        if lang == 'english':
            pre = translate_text(quote, 'en')
            quote = translate_text(quote, 'en')
            title = translate_text(title, 'en')
            url = random.choice(urls_en)
            post = translate_text(quote, 'en')

        # Create synthetic entry
        synthetic_entry = {
            "date": random_date(start_date, end_date),
            "lang": lang,
            "pre": pre,
            "quote": quote,
            "title": title,
            "url": url,
            "post": post
        }
        synthetic_data.append(synthetic_entry)
    return synthetic_data

# Main function to generate and send data to Kafka
def main():
    while True:
        synthetic_data = generate_synthetic_data()
        for data in synthetic_data:
            producer.send('gdelt-news', value=data)
            print(f"Sent: {data}")  # Log each message sent
        producer.flush()
        time.sleep(1)

if __name__ == "__main__":
    main()
