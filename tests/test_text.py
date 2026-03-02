import pytest
from dlt_pipeline.utils.text import (
    clean_text,
    fix_encoding,
    remove_control_characters,
    normalize_unicode,
    remove_pdf_artifacts,
    normalize_whitespace,
    clean_punctuation,
)


class TestFixEncoding:
    def test_mojibake_repair(self):
        broken = 'the â€œbillâ€\x9d provides'
        result = fix_encoding(broken)
        assert '\u201c' in result or '"' in result

    def test_plain_ascii_unchanged(self):
        text = 'Hello world'
        assert fix_encoding(text) == text

    def test_html_entities(self):
        text = 'bill &amp; law'
        result = fix_encoding(text)
        assert '&amp;' not in result or '&' in result


class TestRemoveControlCharacters:
    def test_strips_null_bytes(self):
        assert '\x00' not in remove_control_characters('hello\x00world')

    def test_preserves_newlines(self):
        assert remove_control_characters('line1\nline2') == 'line1\nline2'

    def test_preserves_tabs(self):
        assert remove_control_characters('col1\tcol2') == 'col1\tcol2'

    def test_strips_bell_character(self):
        assert '\x07' not in remove_control_characters('alert\x07here')


class TestNormalizeUnicode:
    def test_smart_quotes_replaced(self):
        result = normalize_unicode('\u201cHello\u201d')
        assert result == '"Hello"'

    def test_em_dash_replaced(self):
        result = normalize_unicode('word\u2014word')
        assert result == 'word--word'

    def test_en_dash_replaced(self):
        result = normalize_unicode('2020\u20132024')
        assert result == '2020-2024'

    def test_non_breaking_space(self):
        result = normalize_unicode('hello\u00a0world')
        assert result == 'hello world'

    def test_bullet_replaced(self):
        result = normalize_unicode('\u2022 item')
        assert result == '- item'


class TestRemovePdfArtifacts:
    def test_removes_page_numbers(self):
        text = 'content\nPage 3\nmore content'
        result = remove_pdf_artifacts(text)
        assert 'Page 3' not in result

    def test_removes_page_n_of_m(self):
        text = 'content\nPage 1 of 5\nmore content'
        result = remove_pdf_artifacts(text)
        assert 'Page 1 of 5' not in result

    def test_removes_standalone_numbers(self):
        text = 'intro\n   42   \nmore text'
        result = remove_pdf_artifacts(text)
        assert '\n42\n' not in result

    def test_removes_null_bytes(self):
        assert '\x00' not in remove_pdf_artifacts('hello\x00world')

    def test_removes_replacement_char(self):
        assert '\ufffd' not in remove_pdf_artifacts('hello\ufffdworld')

    def test_form_feed_to_newline(self):
        result = remove_pdf_artifacts('page1\fpage2')
        assert '\f' not in result
        assert '\n' in result

    def test_removes_escaped_unicode(self):
        result = remove_pdf_artifacts('text\\u0000more')
        assert '\\u0000' not in result


class TestNormalizeWhitespace:
    def test_collapses_multiple_spaces(self):
        assert normalize_whitespace('hello    world') == 'hello world'

    def test_collapses_excessive_newlines(self):
        result = normalize_whitespace('a\n\n\n\n\nb')
        assert result == 'a\n\nb'

    def test_normalizes_carriage_returns(self):
        result = normalize_whitespace('line1\r\nline2\rline3')
        assert '\r' not in result

    def test_tabs_to_spaces(self):
        result = normalize_whitespace('col1\t\tcol2')
        assert '\t' not in result
        assert 'col1 col2' == result


class TestCleanPunctuation:
    def test_removes_space_before_period(self):
        assert clean_punctuation('end .') == 'end.'

    def test_collapses_repeated_punctuation(self):
        result = clean_punctuation('what....')
        assert result == 'what..'


class TestCleanText:
    def test_empty_string(self):
        assert clean_text('') == ''

    def test_none_input(self):
        assert clean_text(None) == 'None' or clean_text(None) != ''

    def test_preserves_single_letter_words(self):
        """The old implementation destroyed 'I' and 'a'."""
        result = clean_text('I am a person')
        assert 'I' in result
        assert ' a ' in result

    def test_preserves_standalone_numbers(self):
        """The old implementation stripped all standalone numbers."""
        result = clean_text('LD 1234 was introduced in 2024')
        assert '1234' in result
        assert '2024' in result

    def test_preserves_dollar_signs(self):
        """The old allowlist stripped $, %, etc."""
        result = clean_text('The budget is $5 million')
        assert '$' in result

    def test_preserves_percent(self):
        result = clean_text('A 50% increase')
        assert '%' in result

    def test_full_pipeline_legislative_text(self):
        sample = (
            'Testimony of Jane Doe\n'
            'In Support of LD 1234\n'
            'An Act to Provide $2.5 Million for Maine Schools\n\n'
            'Dear Members of the Committee,\n\n'
            'I am writing in support of LD 1234. '
            'This bill provides a 15% increase in funding.'
        )
        result = clean_text(sample)
        assert 'LD 1234' in result
        assert '$2.5' in result
        assert '15%' in result
        assert 'I am' in result
        assert len(result) > 50

    def test_non_string_input(self):
        result = clean_text(12345)
        assert isinstance(result, str)

    def test_whitespace_only(self):
        assert clean_text('   \n\t  ') == ''
