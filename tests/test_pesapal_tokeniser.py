"""
Unit and integration tests for the Pesapal Tokeniser.
Run with: pytest tests/test_pesapal_tokeniser.py -v
"""
 
import json
import pytest
from unittest.mock import MagicMock
from tokeniser.pesapal_tokeniser import PesapalTokeniser, PesapalTokeniserConfig
from data_generator.data_generator import TransactionDataGenerator
 
 
# Fixtures 
 
@pytest.fixture
def config():
    return PesapalTokeniserConfig(hmac_secret="test_secret_pesapal")
 
@pytest.fixture
def tokeniser(config):
    return PesapalTokeniser(config)
 
@pytest.fixture
def generator():
    return TransactionDataGenerator()
 
@pytest.fixture
def sample_transaction(generator):
    return generator.generate_pesapal_transaction()
 
def make_mock_message(value: dict):
    msg = MagicMock()
    msg.value.return_value = json.dumps(value).encode('utf-8')
    msg.error.return_value = None
    msg.topic.return_value = 'pesapal_raw'
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    return msg
 
 
# Email Validation 
 
class TestEmailValidation:
 
    def test_valid_email(self, tokeniser):
        valid, error = tokeniser.validate_email("user@example.com")
        assert valid is True
 
    def test_empty_email_fails(self, tokeniser):
        valid, error = tokeniser.validate_email("")
        assert valid is False
 
    def test_none_email_fails(self, tokeniser):
        valid, error = tokeniser.validate_email(None)
        assert valid is False
 
    def test_missing_at_fails(self, tokeniser):
        valid, error = tokeniser.validate_email("userexample.com")
        assert valid is False
 
    def test_missing_dot_fails(self, tokeniser):
        valid, error = tokeniser.validate_email("user@examplecom")
        assert valid is False
 
    def test_subdomain_email_valid(self, tokeniser):
        valid, error = tokeniser.validate_email("user@mail.example.co.ke")
        assert valid is True
 
 
# Phone Validation 
 
class TestPhoneValidation:
 
    def test_valid_kenyan_phone(self, tokeniser):
        valid, error = tokeniser.validate_phone("254700123456")
        assert valid is True
 
    def test_empty_phone_fails(self, tokeniser):
        valid, error = tokeniser.validate_phone("")
        assert valid is False
 
    def test_none_phone_fails(self, tokeniser):
        valid, error = tokeniser.validate_phone(None)
        assert valid is False
 
    def test_non_digit_phone_fails(self, tokeniser):
        valid, error = tokeniser.validate_phone("0700ABCDEF")
        assert valid is False
 
    def test_too_short_fails(self, tokeniser):
        valid, error = tokeniser.validate_phone("07001")
        assert valid is False
 
    def test_too_long_fails(self, tokeniser):
        valid, error = tokeniser.validate_phone("2547001234567890")
        assert valid is False
 
 
# Payment Account Validation
 
class TestPaymentAccountValidation:
 
    def test_valid_account(self, tokeniser):
        valid, error = tokeniser.validate_payment_account("254700123456")
        assert valid is True
 
    def test_empty_account_fails(self, tokeniser):
        valid, error = tokeniser.validate_payment_account("")
        assert valid is False
 
    def test_none_account_fails(self, tokeniser):
        valid, error = tokeniser.validate_payment_account(None)
        assert valid is False
 
    def test_too_short_account_fails(self, tokeniser):
        valid, error = tokeniser.validate_payment_account("123")
        assert valid is False
 
 
# Email Tokenisation
 
class TestEmailTokenisation:
 
    def test_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_email("user@example.com")
        assert token.startswith("PESAPAL_EMAIL_")
 
    def test_token_is_deterministic(self, tokeniser):
        t1, _ = tokeniser.tokenise_email("user@example.com")
        t2, _ = tokeniser.tokenise_email("user@example.com")
        assert t1 == t2
 
    def test_case_insensitive(self, tokeniser):
        t1, _ = tokeniser.tokenise_email("User@Example.COM")
        t2, _ = tokeniser.tokenise_email("user@example.com")
        assert t1 == t2
 
    def test_different_emails_get_different_tokens(self, tokeniser):
        t1, _ = tokeniser.tokenise_email("a@example.com")
        t2, _ = tokeniser.tokenise_email("b@example.com")
        assert t1 != t2
 
    def test_raw_email_not_in_token(self, tokeniser):
        token, _ = tokeniser.tokenise_email("user@example.com")
        assert "user@example.com" not in token
 
    def test_cache_hit(self, tokeniser):
        tokeniser.tokenise_email("user@example.com")
        _, from_cache = tokeniser.tokenise_email("user@example.com")
        assert from_cache is True
 
    def test_none_returns_none(self, tokeniser):
        token, _ = tokeniser.tokenise_email(None)
        assert token is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_email("user@example.com")
        assert tokeniser.stats['email_tokenised'] == 1
 
 
#  Phone Tokenisation 
 
class TestPhoneTokenisation:
 
    def test_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_phone("254700123456")
        assert token.startswith("PESAPAL_PHONE_")
 
    def test_token_is_deterministic(self, tokeniser):
        t1, _ = tokeniser.tokenise_phone("254700123456")
        t2, _ = tokeniser.tokenise_phone("254700123456")
        assert t1 == t2
 
    def test_normalisation_applied(self, tokeniser):
        t1, _ = tokeniser.tokenise_phone("0700123456")
        t2, _ = tokeniser.tokenise_phone("254700123456")
        assert t1 == t2
 
    def test_raw_phone_not_in_token(self, tokeniser):
        token, _ = tokeniser.tokenise_phone("254700123456")
        assert "254700123456" not in token
 
    def test_none_returns_none(self, tokeniser):
        assert tokeniser.tokenise_phone(None)[0] is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_phone("254700123456")
        assert tokeniser.stats['phone_tokenised'] == 1
 
 
# Payment Account Tokenisation 
 
class TestPaymentAccountTokenisation:
 
    def test_mobile_account_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_payment_account("254700123456")
        assert token is not None
 
    def test_card_account_preserves_bin_and_last4(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_payment_account(pan)
        assert token[:6] == pan[:6]
        assert token[-4:] == pan[-4:]
 
    def test_card_account_token_is_digits(self, tokeniser, generator):
        pan = generator.generate_visa_transaction()['bitmap']['DE002']
        token, _ = tokeniser.tokenise_payment_account(pan)
        assert token.isdigit()
 
    def test_non_card_account_gets_pesapal_prefix(self, tokeniser):
        token, _ = tokeniser.tokenise_payment_account("ACC123456")
        assert token.startswith("PESAPAL_ACC_")
 
    def test_none_returns_none(self, tokeniser):
        assert tokeniser.tokenise_payment_account(None)[0] is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_payment_account("254700123456")
        assert tokeniser.stats['account_tokenised'] == 1
 
 
#  Merchant Reference Tokenisation 
 
class TestMerchantReferenceTokenisation:
 
    def test_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_merchant_reference("REF123456")
        assert token.startswith("PESAPAL_MERCHANT_")
 
    def test_token_is_deterministic(self, tokeniser):
        t1, _ = tokeniser.tokenise_merchant_reference("REF123456")
        t2, _ = tokeniser.tokenise_merchant_reference("REF123456")
        assert t1 == t2
 
    def test_none_returns_none(self, tokeniser):
        assert tokeniser.tokenise_merchant_reference(None)[0] is None
 
    def test_stat_increments(self, tokeniser):
        tokeniser.tokenise_merchant_reference("REF123456")
        assert tokeniser.stats['merchant_tokenised'] == 1
 
 
#Message Parsing
 
class TestMessageParsing:
 
    def test_parse_generator_transaction(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        assert parsed is not None
        assert parsed['transaction_id'] is not None
        assert parsed['amount'] > 0
 
    def test_parse_json_string(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(json.dumps(sample_transaction))
        assert parsed is not None
 
    def test_parse_extracts_email(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        assert parsed['customer_email'] is not None
 
    def test_parse_extracts_phone(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        assert parsed['customer_phone'] is not None
 
    def test_parse_extracts_amount(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        assert parsed['amount'] > 0
 
    def test_parse_invalid_json_returns_none(self, tokeniser):
        assert tokeniser.parse_pesapal_message("bad json {{{") is None
 
 
# Tokenised Output 
 
class TestTokenisedOutput:
 
    def test_output_has_no_raw_email(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        raw_email = parsed['customer_email']
        email_token, _ = tokeniser.tokenise_email(raw_email)
        phone_token, _ = tokeniser.tokenise_phone(parsed['customer_phone'])
        account_token, _ = tokeniser.tokenise_payment_account(parsed['payment_account'])
        merchant_token, _ = tokeniser.tokenise_merchant_reference(parsed.get('merchant_reference'))
        output = tokeniser._build_tokenised_message(
            parsed, email_token, phone_token, account_token, merchant_token, False
        )
        assert raw_email not in json.dumps(output)
 
    def test_output_has_email_token(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        email_token, _ = tokeniser.tokenise_email(parsed['customer_email'])
        phone_token, _ = tokeniser.tokenise_phone(parsed['customer_phone'])
        output = tokeniser._build_tokenised_message(
            parsed, email_token, phone_token, None, None, False
        )
        assert output['customer_email_token'] == email_token
 
    def test_output_has_tokenisation_method(self, tokeniser, sample_transaction):
        parsed = tokeniser.parse_pesapal_message(sample_transaction)
        output = tokeniser._build_tokenised_message(parsed, None, None, None, None, False)
        assert output['tokenisation_method'] == 'HMAC_SHA256_PSEUDONYMISATION'
 
 
# Process Message 
 
class TestProcessMessage:
 
    def test_valid_message_returns_true(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        assert tokeniser.process_message(make_mock_message(sample_transaction)) is True
 
    def test_output_goes_to_pesapal_tokenised(self, tokeniser, sample_transaction):
        tokeniser.producer = MagicMock()
        tokeniser.process_message(make_mock_message(sample_transaction))
        assert tokeniser.producer.produce.call_args[1]['topic'] == 'pesapal_tokenised'
 
    def test_invalid_json_goes_to_dlq(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b"bad json"
        msg.error.return_value = None
        msg.topic.return_value = 'pesapal_raw'
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
        assert 'customer_email_token' in output
        assert 'customer_phone_token' in output