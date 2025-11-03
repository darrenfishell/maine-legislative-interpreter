from typing import List
import torch
from sentence_transformers import SentenceTransformer
from ..config import Config


class EmbeddingService:
    """Singleton-like embeddings service with device/batch control."""

    _model = None
    _device = None
    _batch_size = None

    @classmethod
    def _ensure_initialized(cls):
        if cls._model is not None:
            return

        if torch.backends.mps.is_available():
            cls._device = torch.device('mps')
            cls._batch_size = Config.MPS_BATCH_SIZE
            print('Using M3 GPU (MPS) for embeddings')
        else:
            cls._device = torch.device('cpu')
            cls._batch_size = Config.CPU_BATCH_SIZE
            print('MPS not available, using CPU')

        cls._model = SentenceTransformer(Config.EMBEDDING_MODEL)
        cls._model.to(cls._device)

    @classmethod
    def encode(cls, sentences: List[str]):
        cls._ensure_initialized()
        return cls._model.encode(
            sentences,
            show_progress_bar=False,
            device=cls._device,
            batch_size=cls._batch_size,
            convert_to_numpy=True,
            normalize_embeddings=False,
        )

    @classmethod
    def cleanup(cls):
        try:
            if torch.backends.mps.is_available():
                torch.mps.empty_cache()
        except Exception as e:
            print(f'GPU cleanup warning: {e}')


