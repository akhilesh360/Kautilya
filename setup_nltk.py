import nltk

def setup():
    print("Downloading NLTK corpora...")
    corpora = [
        'punkt',
        'punkt_tab',
        'averaged_perceptron_tagger',
        'averaged_perceptron_tagger_eng',
        'brown',
        'wordnet'
    ]
    for corpus in corpora:
        try:
            nltk.download(corpus, quiet=False)
            print(f"Successfully downloaded {corpus}")
        except Exception as e:
            print(f"Failed to download {corpus}: {e}")

if __name__ == "__main__":
    setup()
