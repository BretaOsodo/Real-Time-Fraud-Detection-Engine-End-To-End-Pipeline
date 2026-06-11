"""
Unit and Integration tests for the mastercard Tokeniser
"""

import json 
import pytest
from unittest.mock import MagicMock
from tokeniser.mastercard_tokeniser import MastercardTokeniser,MastercardTokeniserConfig
from data_generator.data_generator import TransactionDataGenerator

#fixtures 
@pytest.fixture

def mc_config():
    return MastercardTokeniserConfig(hmac_secret="test_secret_mastercard")

@pytest.fixture
def tokeniser(mc_config):
    return MastercardTokeniser(mc_config)

@pytest.fixture
def generator():
    return TransactionDataGenerator()

@pytest.fixture
def sample_transaction(generator):
    return generator.generate_mastercard_transaction()

def make_mock_message(value: dict):
    msg = MagicMock()
    msg.value.return_value = json.dumps(value).encode('utf-8')
    msg.error.return_value = None
    msg.topic.return_value = 'mastercard_raw'
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg

#Pan Validation 
class TestPanValidation:
 
    def test_valid_mastercard_pan(self, tokeniser):
        # Known valid Mastercard PANs
        valid_pan = "5412345678901234"  # Need to find a valid one
        # Use generator to get a real valid PAN
        gen = TransactionDataGenerator()
        tx = gen.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        valid, error = tokeniser.validate_pan(pan)
        assert valid is True
        assert error is None
 
    def test_empty_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("")
        assert valid is False
        assert error is not None
 
    def test_none_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan(None)
        assert valid is False
 
    def test_non_digit_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("5123456789ABCDEF")
        assert valid is False
        assert "digits" in error
 
    def test_wrong_length_pan_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("512345678901")  # 12 digits
        assert valid is False
        assert "16" in error
 
    def test_invalid_luhn_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("5123456789012345")  # wrong luhn
        assert valid is False
        assert "Luhn" in error
 
    def test_pan_with_spaces_is_cleaned(self, tokeniser):
        gen = TransactionDataGenerator()
        tx = gen.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        spaced = " ".join([pan[i:i+4] for i in range(0, 16, 4)])
        valid, error = tokeniser.validate_pan(spaced)
        assert valid is True

#Tokenisation
class TestPanTokenisation:
 
    def test_token_preserves_bin(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token[:6] == pan[:6]
 
    def test_token_preserves_last4(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token[-4:] == pan[-4:]
 
    def test_token_same_length_as_pan(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert len(token) == len(pan)
 
    def test_token_contains_only_digits(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token.isdigit()
 
    def test_token_is_deterministic(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token1, _ = tokeniser.tokenise_pan(pan)
        token2, _ = tokeniser.tokenise_pan(pan)
        assert token1 == token2
 
    def test_different_pans_get_different_tokens(self, tokeniser, generator):
        pan1 = generator.generate_mastercard_transaction()['bitmap']['DE002']
        pan2 = generator.generate_mastercard_transaction()['bitmap']['DE002']
        if pan1 != pan2:
            t1, _ = tokeniser.tokenise_pan(pan1)
            t2, _ = tokeniser.tokenise_pan(pan2)
            assert t1 != t2
 
    def test_token_is_not_equal_to_pan(self, tokeniser, generator):
        tx = generator.generate_mastercard_transaction()
        pan = tx['bitmap']['DE002']
        token, _ = tokeniser.tokenise_pan(pan)
        assert token != pan
 
    def test_cache_hit_on_second_call(self, tokeniser, generator):
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        tokeniser.tokenise_pan(pan)
        _, from_cache = tokeniser.tokenise_pan(pan)
        assert from_cache is True
 
    def test_cache_miss_on_first_call(self, tokeniser, generator):
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        _, from_cache = tokeniser.tokenise_pan(pan)
        assert from_cache is False
 
    def test_different_secrets_produce_different_tokens(self, generator):
        t1 = MastercardTokeniser(MastercardTokeniserConfig(hmac_secret="secret_a"))
        t2 = MastercardTokeniser(MastercardTokeniserConfig(hmac_secret="secret_b"))
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        tok1, _ = t1.tokenise_pan(pan)
        tok2, _ = t2.tokenise_pan(pan)
        assert tok1 != tok2

#Message Parsing
class TestMessageParsing:
 
    def test_parse_generator_transaction(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        assert parsed is not None
        assert parsed['pan'] is not None
        assert parsed['bin'] == parsed['pan'][:6]
        assert parsed['last4'] == parsed['pan'][-4:]
 
    def test_parse_json_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(json.dumps(sample_transaction))
        assert parsed is not None
        assert parsed['pan'] is not None
 
    def test_parse_extracts_amount(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        assert parsed['amount'] > 0
 
    def test_parse_extracts_merchant(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        assert parsed['merchant_id'] is not None
 
    def test_parse_extracts_currency(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        assert parsed['currency_code'] is not None
 
    def test_parse_invalid_json_returns_none(self, tokeniser):
        parsed = tokeniser.parse_mastercard_message("not json {{{")
        assert parsed is None
 
    def test_parse_assigns_transaction_id(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        assert parsed['transaction_id'] is not None

#Currency Mapping 
class TestCurrencyMapping:
 
    def test_404_maps_to_kes(self, tokeniser):
        assert tokeniser._currency_code_to_str('404') == 'KES'
 
    def test_840_maps_to_usd(self, tokeniser):
        assert tokeniser._currency_code_to_str('840') == 'USD'
 
    def test_826_maps_to_gbp(self, tokeniser):
        assert tokeniser._currency_code_to_str('826') == 'GBP'
 
    def test_978_maps_to_eur(self, tokeniser):
        assert tokeniser._currency_code_to_str('978') == 'EUR'
 
    def test_unknown_code_defaults_to_usd(self, tokeniser):
        assert tokeniser._currency_code_to_str('999') == 'USD'
 
#Tokenised message Building 
class TestTokenisedMessageBuilding:
 
    def test_output_has_card_token(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert output['card_token'] == token
 
    def test_output_has_no_raw_pan(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        pan = parsed['pan']
        token, _ = tokeniser.tokenise_pan(pan)
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert pan not in json.dumps(output)
 
    def test_output_preserves_bin(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert output['bin'] == parsed['pan'][:6]
 
    def test_output_has_tokenisation_method(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert output['tokenisation_method'] == 'FORMAT_PRESERVING_HMAC'
 
    def test_output_has_currency_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mastercard_message(sample_transaction)
        token, _ = tokeniser.tokenise_pan(parsed['pan'])
        output = tokeniser._build_tokenised_message(parsed, token, False)
        assert 'currency' in output
        assert len(output['currency']) == 3

#Process Message 
class TestProcessMessage:
 
    def test_valid_message_returns_true(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        assert tokeniser.process_message(msg) is True
 
    def test_valid_message_increments_stats(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        assert tokeniser.stats['total_consumed'] == 1
        assert tokeniser.stats['total_tokenised'] == 1
 
    def test_output_goes_to_correct_topic(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        assert call_kwargs['topic'] == 'mastercard_tokenised'
 
    def test_partition_key_is_token(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        key = call_kwargs['key'].decode('utf-8')
        assert key.isdigit() and len(key) == 16
 
    def test_invalid_json_goes_to_dlq(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b"not valid json"
        msg.error.return_value = None
        msg.topic.return_value = 'mastercard_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        result = tokeniser.process_message(msg)
        assert result is False
        assert tokeniser.stats['total_dlq'] == 1
 
    def test_empty_message_returns_false(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = None
        msg.error.return_value = None
        assert tokeniser.process_message(msg) is False
 
    def test_output_is_valid_json(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        output = json.loads(call_kwargs['value'].decode('utf-8'))
        assert isinstance(output, dict)
        assert 'card_token' in output

#Stats
class TestStats:
 
    def test_stats_initialised_to_zero(self, tokeniser):
        for k, v in tokeniser.stats.items():
            assert v == 0 or v == 0.0
 
    def test_cache_miss_increments(self, tokeniser, generator):
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        tokeniser.tokenise_pan(pan)
        assert tokeniser.stats['cache_misses'] == 1
 
    def test_cache_hit_increments(self, tokeniser, generator):
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        tokeniser.tokenise_pan(pan)
        tokeniser.tokenise_pan(pan)
        assert tokeniser.stats['cache_hits'] == 1
 
    def test_tokenisation_time_recorded(self, tokeniser, generator):
        pan = generator.generate_mastercard_transaction()['bitmap']['DE002']
        tokeniser.tokenise_pan(pan)
        assert tokeniser.stats['tokenisation_total_ms'] > 0


