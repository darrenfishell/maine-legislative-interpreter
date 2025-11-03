import subprocess
import sys
import spacy


def load_spacy_model(model_name: str = 'en_core_web_sm'):
    """Load spaCy model optimized for sentence extraction only."""
    try:
        nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
        if not nlp.has_pipe('sentencizer'):
            nlp.add_pipe('sentencizer')
        return nlp
    except OSError:
        print(f'Model {model_name} not found. Downloading...')
        subprocess.check_call([sys.executable, '-m', 'spacy', 'download', model_name])
        nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
        if not nlp.has_pipe('sentencizer'):
            nlp.add_pipe('sentencizer')
        return nlp


