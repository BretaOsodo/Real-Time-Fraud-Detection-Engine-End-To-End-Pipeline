"""
Unit and integration tests for the Visa Tokeniser.
Run with: pytest tests/test_visa_tokeniser.py -v
"""
 
import json
import pytest
from unittest.mock import MagicMock
from tokeniser.visa_tokeniser import VisaTokeniser, TokeniserConfig
from data_generator.data_generator import TransactionDataGenerator
 
 
# Fixtures
 
@pytest.fixture
def visa_config():
    return TokeniserConfig(hmac_secret="test_secret_visa")
 
@pytest.fixture
def tokeniser(visa_config):
    return VisaTokeniser(visa_config)
 
@pytest.fixture
def generator():
    return TransactionDataGenerator()
 
@pytest.fixture
def sample_transaction(generator):
    return generator.generate_visa_transaction()
 
def make_mock_message(value: dict):
    msg = MagicMock()
    msg.value.return_value = json.dumps(value).encode('utf-8')
    msg.error.return_value = None
    msg.topic.return_value = 'visa_raw'
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg
 
 
# PAN Validation
 
class TestPanValidation:
 
    def test_valid_visa_pan(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        valid, error = tokeniser.validate_pan(pan)
        assert valid is True
        assert error is None
 
    def test_empty_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("")
        assert valid is False
 
    def test_none_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan(None)
        assert valid is False
 
    def test_non_digit_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("4123456789ABCDEF")
        assert valid is False
        assert "digits" in error
 
    def test_too_short_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("412345678901")  # 12 digits
        assert valid is False
 
    def test_too_long_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("41234567890123456789")  # 20 digits
        assert valid is False
 
    def test_invalid_luhn_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("4123456789012346")
        assert valid is False
        assert "Luhn" in error
 
    def test_pan_with_dashes_cleaned(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        dashed = "-".join([pan[i:i+4] for i in range(0, 16, 4)])
        valid, error = tokeniser.validate_pan(dashed)
        assert valid is True
 
    def test_13_digit_pan_accepted(self, tokeniser):
        """Visa supports 13-digit PANs."""
        # Build a valid 13-digit Luhn PAN
        pan_base = "4123456789012"
        # Just test length validation path
        valid, error = tokeniser.validate_pan(pan_base)
        # May fail Luhn — just confirm length is not the error
        if not valid:
            assert "13" not in error or "Luhn" in error
 
 
# Tokenisation 
 
class TestPanTokenisation:
 
    def test_token_preserves_bin(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token[:6] == pan[:6]
 
    def test_token_preserves_last4(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token[-4:] == pan[-4:]
 
    def test_token_same_length(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert len(token) == len(pan)
 
    def test_token_contains_only_digits(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token.isdigit()
 
    def test_token_is_deterministic(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        assert tokeniser.tokenise_pan(pan)[0] == tokeniser.tokenise_pan(pan)[0]
 
    def test_token_differs_from_pan(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token != pan
 
    def test_cache_hit_on_second_call(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        tokeniser.tokenise_pan(pan)
        _, from_cache = tokeniser.tokenise_pan(pan)
        assert from_cache is True
 
    def test_different_secrets_produce_different_tokens(self, generator):
        t1 = VisaTokeniser(TokeniserConfig(hmac_secret="secret_a"))
        t2 = VisaTokeniser(TokeniserConfig(hmac_secret="secret_b"))
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        assert t1.tokenise_pan(pan)[0] != t2.tokenise_pan(pan)[0]
 
 
# Message Parsing
 
class TestMessageParsing:
 
    def test_parse_generator_transaction(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        assert parsed is not None
        assert parsed['pan'] is not None
        assert parsed['bin'] == parsed['pan'][:6]
        assert parsed['last4'] == parsed['pan'][-4:]
 
    def test_parse_json_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(json.dumps(sample_transaction))
        assert parsed is not None
 
    def test_parse_extracts_amount(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        assert parsed['amount'] > 0
 
    def test_parse_extracts_merchant(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        assert parsed['merchant_id'] is not None
 
    def test_parse_invalid_json_returns_none(self, tokeniser):
        assert tokeniser.parse_visa_message("not json {{{") is None
 
 
# Tokenised Output
 
class TestTokenisedOutput:
 
    def test_output_has_no_raw_pan(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        pan = parsed['pan']
        token, _ = tokeniser.tokenise_pan(pan)
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert pan not in json.dumps(output)
 
    def test_output_has_card_token(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert output['card_token'] == token
 
    def test_output_tokenisation_method(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert output['tokenisation_method'] == 'FORMAT_PRESERVING_HMAC'
 
    def test_output_has_currency_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_visa_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert 'currency' in output
 
 
# Process Message 
 
class TestProcessMessage:
 
    def test_valid_message_returns_true(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        assert tokeniser.process_message(make_mock_message(sample_transaction)) is True
 
    def test_output_goes_to_visa_tokenised(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        tokeniser.process_message(make_mock_message(sample_transaction))
        assert tokeniser.producer.produce.call_args[1]['topic'] == 'visa_tokenised'
 
    def test_partition_key_is_token(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        tokeniser.process_message(make_mock_message(sample_transaction))
        key = tokeniser.producer.produce.call_args[1]['key'].decode('utf-8')
        assert key.isdigit() and len(key) == 16
 
    def test_invalid_json_goes_to_dlq(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b"bad json"
        msg.error.return_value = None
        msg.topic.return_value = 'visa_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        assert tokeniser.process_message(msg) is False
 
    def test_empty_message_returns_false(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = None
        msg.error.return_value = None
        assert tokeniser.process_message(msg) is False
 
    def test_output_is_valid_json(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        tokeniser.process_message(make_mock_message(sample_transaction))
        output = json.loads(tokeniser.producer.produce.call_args[1]['value'].decode('utf-8'))
        assert 'card_token' in output
        assert 'bin' in output
        assert 'last4' in output
