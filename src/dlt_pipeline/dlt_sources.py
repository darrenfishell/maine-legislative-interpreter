from .sources.session import session_data
from .sources.pdf_extraction import pdf_text_extraction
from .sources.staging import text_cleaning
from .sources.vectorization import text_vectorization
from .services.api import get_current_legislature as current_session


def main(dev_mode: bool = False):
    from .pipeline.run import main as run_main
    run_main(dev_mode=dev_mode)


if __name__ == '__main__':
    main(dev_mode=False)
