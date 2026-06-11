"""
Unit and Integration tests for M-Pesa Tokeniser
"""

import json 
import pytest 
from unittest.mock import MagicMock
from tokeniser.mpesa_tokeniser import MpesaTokeniser, MpesaTokeniserConfig
from data_generator.data_generator import TransactionDataGenerator

#Fixtures 
@pytest.fixture
def mp_config():
    return MpesaTokeniserConfig(hmac_secret="test_secret_mpesa")

@pytest.fixture
def tokeniser(mp_config):
    return MpesaTokeniser(mp_config)

@pytest.fixture
def generator():
    return TransactionDataGenerator()

@pytest.fixture
def sample_transaction(generator):
    return generator.generate_mpesa_transaction()

def make_mock_message(value: dict):
    msg = MagicMock()
    msg.value.return_value = json.dumps(value).encode('utf-8')
    msg.error.return_value = None
    msg.topic.return_value = 'mpesa_raw'
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg

#MSISDN Validation
class TestMsisdnValidation:
 
    def test_valid_kenyan_msisdn_with_country_code(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("254700123456")
        assert valid is True
 
    def test_valid_kenyan_msisdn_with_zero(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("0700123456")
        assert valid is True
 
    def test_empty_msisdn_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("")
        assert valid is False
 
    def test_none_msisdn_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn(None)
        assert valid is False
 
    def test_non_digit_msisdn_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("07001234AB")
        assert valid is False
        assert "digits" in error
 
    def test_too_short_msisdn_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("07001")
        assert valid is False
 
    def test_too_long_msisdn_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("254700123456789123")
        assert valid is False
 
    def test_wrong_length_with_country_code_fails(self, tokeniser):
        valid, error = tokeniser.validate_msisdn("25470012345")  # 11 digits, should be 12
        assert valid is False
 
 #MSISDN Normalization 
class TestMsisdnNormalisation:
 
    def test_normalise_07_format(self, tokeniser):
        result = tokeniser.normalise_msisdn("0700123456")
        assert result == "254700123456"
 
    def test_normalise_7_format(self, tokeniser):
        result = tokeniser.normalise_msisdn("700123456")
        assert result == "254700123456"
 
    def test_already_normalised(self, tokeniser):
        result = tokeniser.normalise_msisdn("254700123456")
        assert result == "254700123456"
 
    def test_strips_plus(self, tokeniser):
        result = tokeniser.normalise_msisdn("+254700123456")
        assert result == "254700123456"
 
    def test_none_returns_none(self, tokeniser):
        result = tokeniser.normalise_msisdn(None)
        assert result is None
 
#MSISDN Tokenisation 
class TestMsisdnTokenisation:
 
    def test_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_msisdn("254700123456")
        assert token.startswith("MPESA_MSISDN_")
 
    def test_token_is_deterministic(self, tokeniser):
        t1, _ = tokeniser.tokenise_msisdn("254700123456")
        t2, _ = tokeniser.tokenise_msisdn("254700123456")
        assert t1 == t2
 
    def test_normalisation_applied(self, tokeniser):
        """07 and 254 formats should produce same token."""
        t1, _ = tokeniser.tokenise_msisdn("0700123456")
        t2, _ = tokeniser.tokenise_msisdn("254700123456")
        assert t1 == t2
 
    def test_different_msisdns_get_different_tokens(self, tokeniser):
        t1, _ = tokeniser.tokenise_msisdn("254700123456")
        t2, _ = tokeniser.tokenise_msisdn("254700654321")
        assert t1 != t2
 
    def test_raw_msisdn_not_in_token(self, tokeniser):
        token, _ = tokeniser.tokenise_msisdn("254700123456")
        assert "254700123456" not in token
        assert "0700123456" not in token
 
    def test_cache_hit_on_second_call(self, tokeniser):
        tokeniser.tokenise_msisdn("254700123456")
        _, from_cache = tokeniser.tokenise_msisdn("254700123456")
        assert from_cache is True
 
    def test_none_returns_none(self, tokeniser):
        token, _ = tokeniser.tokenise_msisdn(None)
        assert token is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_msisdn("254700123456")
        assert tokeniser.stats['msisdn_tokenised'] == 1

#Account Tokenisation
class TestAccountTokenisation:
 
    def test_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_account("123456")
        assert token.startswith("MPESA_ACC_")
 
    def test_token_is_deterministic(self, tokeniser):
        t1, _ = tokeniser.tokenise_account("123456")
        t2, _ = tokeniser.tokenise_account("123456")
        assert t1 == t2
 
    def test_different_accounts_get_different_tokens(self, tokeniser):
        t1, _ = tokeniser.tokenise_account("123456")
        t2, _ = tokeniser.tokenise_account("654321")
        assert t1 != t2
 
    def test_integer_input_works(self, tokeniser):
        token, _ = tokeniser.tokenise_account(123456)
        assert token is not None
        assert token.startswith("MPESA_ACC_")
 
    def test_none_returns_none(self, tokeniser):
        token, _ = tokeniser.tokenise_account(None)
        assert token is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_account("123456")
        assert tokeniser.stats['account_tokenised'] == 1

#Message Parsing 
class TestMessageParsing:
 
    def test_parse_daraja_webhook_format(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        assert parsed is not None
        assert parsed['msisdn'] is not None
        assert parsed['amount'] > 0
 
    def test_parse_json_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(json.dumps(sample_transaction))
        assert parsed is not None
 
    def test_parse_extracts_transaction_id(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        assert parsed['transaction_id'] is not None
 
    def test_parse_extracts_amount(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        assert parsed['amount'] > 0
 
    def test_parse_extracts_msisdn(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        assert parsed['msisdn'] is not None
        assert parsed['msisdn'].startswith('254')
 
    def test_parse_invalid_json_returns_none(self, tokeniser):
        parsed = tokeniser.parse_mpesa_message("not json {{{")
        assert parsed is None
 
    def test_parse_unknown_format_returns_none(self, tokeniser):
        parsed = tokeniser.parse_mpesa_message({"unknown": "format"})
        assert parsed is None
 
    def test_parse_mpesa_timestamp(self, tokeniser):
        result = tokeniser._parse_mpesa_timestamp("20260610120000")
        assert "2026-06-10" in result
 
    def test_parse_invalid_timestamp_uses_now(self, tokeniser):
        result = tokeniser._parse_mpesa_timestamp("invalid")
        assert result is not None

#Tokenised Message Building 
class TestTokenisedMessageBuilding:
 
    def test_output_has_msisdn_token(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        msisdn_token, _ = tokeniser.tokenise_msisdn(parsed['msisdn'])
        account_token, _ = tokeniser.tokenise_account(parsed.get('business_shortcode'))
        output = tokeniser._build_tokenised_message(parsed, msisdn_token, account_token, False)
        assert output['msisdn_token'] == msisdn_token
 
    def test_output_has_no_raw_msisdn(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        raw_msisdn = parsed['msisdn']
        msisdn_token, _ = tokeniser.tokenise_msisdn(raw_msisdn)
        account_token, _ = tokeniser.tokenise_account(parsed.get('business_shortcode'))
        output = tokeniser._build_tokenised_message(parsed, msisdn_token, account_token, False)
        assert raw_msisdn not in json.dumps(output)
 
    def test_output_currency_is_kes(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        msisdn_token, _ = tokeniser.tokenise_msisdn(parsed['msisdn'])
        output = tokeniser._build_tokenised_message(parsed, msisdn_token, None, False)
        assert output['currency'] == 'KES'
 
    def test_output_has_tokenisation_method(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_mpesa_message(sample_transaction)
        msisdn_token, _ = tokeniser.tokenise_msisdn(parsed['msisdn'])
        output = tokeniser._build_tokenised_message(parsed, msisdn_token, None, False)
        assert output['tokenisation_method'] == 'HMAC_SHA256_PSEUDONYMISATION'
 
 
#process Message
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
 
    def test_output_goes_to_correct_topic(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        assert call_kwargs['topic'] == 'mpesa_tokenised'
 
    def test_output_key_is_msisdn_token(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        msg = make_mock_message(sample_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        key = call_kwargs['key'].decode('utf-8')
        assert key.startswith("MPESA_MSISDN_")
 
    def test_invalid_json_goes_to_dlq(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b"not valid json"
        msg.error.return_value = None
        msg.topic.return_value = 'mpesa_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        assert tokeniser.process_message(msg) is False
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
        assert 'msisdn_token' in output
