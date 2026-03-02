import spacy
from tqdm import tqdm


def load_spacy_model(model_name: str = 'en_core_web_sm'):
    """Load spaCy model optimized for sentence extraction only."""
    # First try: load the small English model if available
    try:
        nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
    except OSError:
        # Attempt an in-process download without relying on pip CLI
        try:
            tqdm.write(f'Model {model_name} not found. Attempting download...')
            from spacy.cli import download as spacy_download
            spacy_download(model_name, False)
            nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
        except Exception:
            # Final fallback: use a lightweight blank English pipeline
            tqdm.write(f'Could not download {model_name}. Falling back to spacy.blank("en") with sentencizer.')
            nlp = spacy.blank('en')

    if not nlp.has_pipe('sentencizer'):
        nlp.add_pipe('sentencizer')
    return nlp


