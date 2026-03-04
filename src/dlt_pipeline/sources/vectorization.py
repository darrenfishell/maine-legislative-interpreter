import threading
import dlt
from typing import List, Dict
from ..config import Config
from .. import db_access as dba
from tqdm import tqdm

from ..utils.nlp import load_spacy_model
from ..services.embeddings import EmbeddingService

_lock = threading.Lock()
_progress: dict = {}


def _update(session: int, *, docs: int = 0, sentences: int = 0, errors: int = 0):
    with _lock:
        stats = _progress.get(session)
        if stats is None:
            return
        stats['docs_done'] += docs
        stats['sentences'] += sentences
        stats['errors'] += errors
        if docs:
            stats['pbar'].update(docs)


@dlt.source
def text_vectorization(session: int):
    import torch

    @dlt.resource(primary_key='doc_id')
    def doc_text():
        db = dba.Database(Config.DB_NAME, Config.RAW_SCHEMA, Config.STAGING_SCHEMA)
        unprocessed_docs = db.get_unprocessed_documents(session)

        if not unprocessed_docs:
            tqdm.write(f'Session {session}: no documents to embed')
            return

        with _lock:
            _progress[session] = {
                'total': len(unprocessed_docs), 'docs_done': 0,
                'sentences': 0, 'errors': 0,
                'pbar': tqdm(total=len(unprocessed_docs), desc=f'Session {session}', unit='doc', leave=False),
            }

        for doc in unprocessed_docs:
            yield doc

    nlp = load_spacy_model()
    if not nlp.has_pipe('sentencizer'):
        nlp.add_pipe('sentencizer')
    nlp.max_length = 10_000_000  # safe: only sentencizer is active, no parser/NER memory concern

    @dlt.transformer(parallelized=True)
    def doc_sentence(doc: Dict):
        doc_id = doc.get('doc_id')
        doc_text_val = doc.get('cleaned_text', '')

        sentences: List[Dict] = []
        with nlp.select_pipes(enable=['sentencizer']):
            processed_doc = nlp(doc_text_val)
            for idx, sent in enumerate(processed_doc.sents):
                sent_text = sent.text.strip()
                if not sent_text:
                    continue

                tokens = sent_text.split()
                letters = sum(ch.isalpha() for ch in sent_text)
                total = len(sent_text)
                letter_ratio = letters / total if total else 0.0

                # Drop very short / low-signal sentences (e.g. PDF glyph junk, closings)
                if len(tokens) < 3:
                    continue
                if letters < 8:
                    continue
                if letter_ratio < 0.6:
                    continue

                sentences.append({'doc_id': doc_id, 'sentence': sent_text, 'sentence_index': idx})

        return sentences

    @dlt.transformer(
        max_table_nesting=0,
        primary_key=['doc_id', 'sentence_index'],
        columns={"embedding": {"data_type": "json"}},
    )
    def int_sentence_embeddings(sentence_batch: List[Dict]):
        sentences: List[str] = []
        metadata: List[Dict] = []
        for sentence_data in sentence_batch:
            sentence_text = sentence_data.get('sentence', '')
            if sentence_text and len(sentence_text.strip()) > 0:
                sentences.append(sentence_text)
                metadata.append(sentence_data)

        if not sentences:
            return []

        doc_ids_in_batch = set(m['doc_id'] for m in metadata)

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
            _update(session, docs=len(doc_ids_in_batch), sentences=len(sentences))
            return results
        except Exception as e:
            if not Config.QUIET_ERRORS:
                tqdm.write(f"  Error encoding doc_id {metadata[0]['doc_id'] if metadata else '?'}: {e}")
            _update(session, docs=len(doc_ids_in_batch), errors=len(doc_ids_in_batch))
            return []
        finally:
            try:
                if torch.backends.mps.is_available():
                    torch.mps.empty_cache()
            except Exception:
                pass

    return (doc_text, doc_text | doc_sentence | int_sentence_embeddings)
