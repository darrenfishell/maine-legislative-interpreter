import dlt
from typing import List, Dict
from ..config import Config
from .. import db_access as dba
from tqdm import tqdm

from ..utils.text import clean_text
from ..utils.nlp import load_spacy_model
from ..services.embeddings import EmbeddingService


@dlt.source
def text_vectorization(session: int):
    import torch

    tqdm.write(f'Generating embeddings for {session} testimony')

    @dlt.resource(
        primary_key='doc_id',
        parallelized=True,
    )
    def doc_text():
        db = dba.Database(Config.DB_NAME, Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA)
        unprocessed_docs = db.get_unprocessed_documents(Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA, session)
        tqdm.write(f'Processing {len(unprocessed_docs)} docs for {session}')

        for doc in unprocessed_docs:
            doc['doc_text'] = clean_text(doc['doc_text'])
            yield doc

    @dlt.transformer
    def doc_sentence(doc: Dict):
        if not hasattr(doc_sentence, 'nlp'):
            doc_sentence.nlp = load_spacy_model()
            if not doc_sentence.nlp.has_pipe('sentencizer'):
                doc_sentence.nlp.add_pipe('sentencizer')

        nlp = doc_sentence.nlp

        doc_id = doc.get('doc_id')
        doc_text_val = doc.get('doc_text', '')
        doc_length = len(doc_text_val)

        nlp.max_length = doc_length + 1000

        sentences: List[Dict] = []
        with nlp.select_pipes(enable=['sentencizer']):
            processed_doc = nlp(doc_text_val)
            for idx, sent in enumerate(processed_doc.sents):
                sent_text = sent.text.strip()
                if len(sent_text) > 10:
                    sentences.append({'doc_id': doc_id, 'sentence': sent_text, 'sentence_index': idx})

        return sentences

    @dlt.transformer(
        max_table_nesting=0,
        primary_key=['doc_id', 'sentence_index'],
        columns={"embedding": {"data_type": "json"}},
    )
    def document_sentence_vector(sentence_batch: List[Dict]):
        # Extract sentences and metadata
        sentences: List[str] = []
        metadata: List[Dict] = []
        for sentence_data in sentence_batch:
            sentence_text = sentence_data.get('sentence', '')
            if sentence_text and len(sentence_text.strip()) > 0:
                sentences.append(sentence_text)
                metadata.append(sentence_data)

        if not sentences:
            return []

        try:
            embeddings = EmbeddingService.encode(sentences)
            results: List[Dict] = []
            for embedding, sentence_data in zip(embeddings, metadata):
                results.append({
                    'doc_id': sentence_data['doc_id'],
                    'sentence': sentence_data['sentence'],
                    'sentence_index': sentence_data['sentence_index'],
                    'embedding': embedding.tolist(),
                    'model_name': Config.EMBEDDING_MODEL,
                    'embedding_dimension': len(embedding),
                })
            return results
        except Exception as e:
            if not Config.QUIET_ERRORS:
                tqdm.write(f"Error encoding sentences for doc_id {metadata[0]['doc_id'] if metadata else 'unknown'}: {e}")
            return []
        finally:
            try:
                if torch.backends.mps.is_available():
                    torch.mps.empty_cache()
            except Exception as e:
                if not Config.QUIET_ERRORS:
                    tqdm.write(f"GPU cleanup warning: {e}")

    return (doc_text, doc_text | doc_sentence | document_sentence_vector)


