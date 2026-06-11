"""
Unit and integration tests for the Flutterwave Tokeniser.
 
Run with:
    pytest tests/test_flutterwave_tokeniser.py -v
"""
 
import json
import time
import pytest
from unittest.mock import MagicMock, patch
from tokeniser.flutter_tokeniser import FlutterwaveTokeniser, FlutterwaveTokeniserConfig
 
 
# ── Fixtures ──────────────────────────────────────────────────────────────────
 
@pytest.fixture
def config():
    return FlutterwaveTokeniserConfig(
        bootstrap_servers="127.0.0.1:9092",
        hmac_secret="test_secret_key_for_unit_tests",
        input_topic="flutter_raw",
        output_topic="flutter_tokenised",
        dlq_topic="flutter_dlq",
    )
 
 
@pytest.fixture
def tokeniser(config):
    return FlutterwaveTokeniser(config)
 
 
@pytest.fixture
def sample_raw_transaction():
    """Sample raw Flutterwave webhook transaction matching data_generator output."""
    return {
        "source": "FLUTTERWAVE",
        "source_type": "WEBHOOK",
        "webhook_payload": {
            "event": "charge.completed",
            "data": {
                "id": 1234567,
                "tx_ref": "TX16000000100",
                "flw_ref": "FLW123456789",
                "device_fingerprint": "abc123def456",
                "amount": 5000.00,
                "currency": "KES",
                "charged_amount": 5000.00,
                "app_fee": 125.00,
                "merchant_fee": 50.00,
                "processor_response": "Approved",
                "auth_model": "PIN",
                "payment_type": "card",
                "status": "successful",
                "customer": {
                    "id": 1001,
                    "name": "John Doe",
                    "phone_number": "254700000001",
                    "email": "john@example.com",
                },
                "card": {
                    "first_6digits": "520000",
                    "last_4digits": "1234",
                    "issuer": "MASTERCARD",
                    "country": "KE",
                    "type": "DEBIT",
                },
            },
        },
        "verification_hash": "abc123",
        "timestamp": "2026-06-10T10:00:00",
        "event_time_ms": 1749549600000,
    }
 
 
@pytest.fixture
def sample_flat_transaction():
    """Flat Flutterwave transaction without webhook_payload wrapper."""
    return {
        "event": "charge.completed",
        "data": {
            "id": 9876543,
            "tx_ref": "TX16000000200",
            "flw_ref": "FLW987654321",
            "amount": 1500.00,
            "currency": "USD",
            "status": "successful",
            "payment_type": "card",
            "auth_model": "VBV",
            "processor_response": "Approved",
            "device_fingerprint": "xyz789",
            "customer": {
                "id": 2002,
                "name": "Jane Smith",
                "phone_number": "254700000002",
                "email": "jane@example.com",
            },
            "card": {
                "first_6digits": "530000",
                "last_4digits": "5678",
                "issuer": "MASTERCARD",
                "country": "KE",
                "type": "CREDIT",
            },
        },
    }
 
 
# ── Card validation tests ─────────────────────────────────────────────────────
 
class TestCardValidation:
 
    def test_valid_card_passes(self, tokeniser):
        valid, error = tokeniser.validate_pan("520000", "1234")
        assert valid is True
        assert error is None
 
    def test_missing_first6_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("", "1234")
        assert valid is False
        assert error is not None
 
    def test_missing_last4_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("520000", "")
        assert valid is False
        assert error is not None
 
    def test_non_digit_first6_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("52ABCD", "1234")
        assert valid is False
        assert "6 digits" in error
 
    def test_non_digit_last4_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("520000", "12AB")
        assert valid is False
        assert "4 digits" in error
 
    def test_short_first6_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("5200", "1234")
        assert valid is False
 
    def test_short_last4_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("520000", "123")
        assert valid is False
 
    def test_long_first6_fails(self, tokeniser):
        valid, error = tokeniser.validate_pan("5200001", "1234")
        assert valid is False
 
 
# ── Customer email validation tests ───────────────────────────────────────────
 
class TestEmailValidation:
 
    def test_valid_email_passes(self, tokeniser):
        valid, error = tokeniser.validate_customer_email("user@example.com")
        assert valid is True
 
    def test_empty_email_passes(self, tokeniser):
        """Email is optional — empty string should pass."""
        valid, error = tokeniser.validate_customer_email("")
        assert valid is True
 
    def test_none_email_passes(self, tokeniser):
        valid, error = tokeniser.validate_customer_email(None)
        assert valid is True
 
    def test_missing_at_sign_fails(self, tokeniser):
        valid, error = tokeniser.validate_customer_email("userexample.com")
        assert valid is False
 
    def test_missing_dot_fails(self, tokeniser):
        valid, error = tokeniser.validate_customer_email("user@examplecom")
        assert valid is False
 
 
# ── Tokenisation tests ────────────────────────────────────────────────────────
 
class TestCardTokenisation:
 
    def test_card_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_card("520000", "1234")
        assert token.startswith("FLW_520000_TOKEN_")
        assert token.endswith("_1234")
 
    def test_card_token_preserves_bin(self, tokeniser):
        token, _ = tokeniser.tokenise_card("530000", "5678")
        assert "530000" in token
 
    def test_card_token_preserves_last4(self, tokeniser):
        token, _ = tokeniser.tokenise_card("520000", "9999")
        assert token.endswith("_9999")
 
    def test_card_token_is_deterministic(self, tokeniser):
        token1, _ = tokeniser.tokenise_card("520000", "1234")
        token2, _ = tokeniser.tokenise_card("520000", "1234")
        assert token1 == token2
 
    def test_different_cards_get_different_tokens(self, tokeniser):
        token1, _ = tokeniser.tokenise_card("520000", "1234")
        token2, _ = tokeniser.tokenise_card("530000", "5678")
        assert token1 != token2
 
    def test_card_token_cache_hit(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")  # first call — miss
        _, from_cache = tokeniser.tokenise_card("520000", "1234")  # second — hit
        assert from_cache is True
 
    def test_card_token_cache_miss_on_first_call(self, tokeniser):
        _, from_cache = tokeniser.tokenise_card("520000", "9876")
        assert from_cache is False
 
    def test_cache_hit_increments_stat(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")
        tokeniser.tokenise_card("520000", "1234")
        assert tokeniser.stats['cache_hits'] >= 1
 
    def test_none_inputs_return_none(self, tokeniser):
        token, _ = tokeniser.tokenise_card(None, None)
        assert token is None
 
    def test_flw_ref_affects_token(self, tokeniser):
        """Different flw_ref should produce different tokens for same card."""
        token1, _ = tokeniser.tokenise_card("520000", "1234", "FLW111")
        token2, _ = tokeniser.tokenise_card("520000", "1234", "FLW222")
        assert token1 != token2
 
    def test_different_secrets_produce_different_tokens(self):
        """Two tokenisers with different secrets must produce different tokens."""
        t1 = FlutterwaveTokeniser(FlutterwaveTokeniserConfig(hmac_secret="secret_a"))
        t2 = FlutterwaveTokeniser(FlutterwaveTokeniserConfig(hmac_secret="secret_b"))
        tok1, _ = t1.tokenise_card("520000", "1234")
        tok2, _ = t2.tokenise_card("520000", "1234")
        assert tok1 != tok2
 
 
class TestCustomerTokenisation:
 
    def test_customer_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_customer("1001", "user@example.com", "254700000001")
        assert token.startswith("FLW_CUST_")
 
    def test_customer_token_is_deterministic(self, tokeniser):
        token1, _ = tokeniser.tokenise_customer("1001", "user@example.com", "254700000001")
        token2, _ = tokeniser.tokenise_customer("1001", "user@example.com", "254700000001")
        assert token1 == token2
 
    def test_different_customers_get_different_tokens(self, tokeniser):
        token1, _ = tokeniser.tokenise_customer("1001", "a@example.com", "254700000001")
        token2, _ = tokeniser.tokenise_customer("1002", "b@example.com", "254700000002")
        assert token1 != token2
 
    def test_no_customer_data_returns_none(self, tokeniser):
        token, _ = tokeniser.tokenise_customer(None, None, None)
        assert token is None
 
    def test_customer_token_cache_hit(self, tokeniser):
        tokeniser.tokenise_customer("1001", "user@example.com", "254700000001")
        _, from_cache = tokeniser.tokenise_customer("1001", "user@example.com", "254700000001")
        assert from_cache is True
 
 
class TestReferenceTokenisation:
 
    def test_ref_token_format(self, tokeniser):
        token, _ = tokeniser.tokenise_reference("TX16000000100")
        assert token.startswith("FLW_REF_")
 
    def test_ref_token_is_deterministic(self, tokeniser):
        token1, _ = tokeniser.tokenise_reference("TX16000000100")
        token2, _ = tokeniser.tokenise_reference("TX16000000100")
        assert token1 == token2
 
    def test_different_refs_get_different_tokens(self, tokeniser):
        token1, _ = tokeniser.tokenise_reference("TX16000000100")
        token2, _ = tokeniser.tokenise_reference("TX16000000200")
        assert token1 != token2
 
    def test_none_ref_returns_none(self, tokeniser):
        token, _ = tokeniser.tokenise_reference(None)
        assert token is None
 
 
# ── Message parsing tests ─────────────────────────────────────────────────────
 
class TestMessageParsing:
 
    def test_parse_webhook_payload_format(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed is not None
        assert parsed['source'] == 'FLUTTERWAVE'
        assert parsed['first6'] == '520000'
        assert parsed['last4'] == '1234'
 
    def test_parse_flat_format(self, tokeniser, sample_flat_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_flat_transaction)
        assert parsed is not None
        assert parsed['first6'] == '530000'
        assert parsed['last4'] == '5678'
 
    def test_parse_json_string(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(json.dumps(sample_raw_transaction))
        assert parsed is not None
        assert parsed['first6'] == '520000'
 
    def test_parse_extracts_amount(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['amount'] == 5000.00
 
    def test_parse_extracts_currency(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['currency'] == 'KES'
 
    def test_parse_extracts_customer_email(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['customer_email'] == 'john@example.com'
 
    def test_parse_extracts_customer_phone(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['customer_phone'] == '254700000001'
 
    def test_parse_extracts_status(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['status'] == 'successful'
 
    def test_parse_extracts_flw_ref(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['flw_ref'] == 'FLW123456789'
 
    def test_parse_invalid_json_returns_none(self, tokeniser):
        parsed = tokeniser.parse_flutterwave_message("not valid json {{{")
        assert parsed is None
 
    def test_parse_empty_dict_returns_parsed(self, tokeniser):
        """Empty dict should return a parsed object with defaults."""
        parsed = tokeniser.parse_flutterwave_message({})
        assert parsed is not None
 
    def test_parse_assigns_transaction_id(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        assert parsed['transaction_id'] is not None
        assert len(parsed['transaction_id']) > 0
 
 
# ── Tokenised message building tests ─────────────────────────────────────────
 
class TestTokenisedMessageBuilding:
 
    def test_build_output_has_card_token(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        assert output['card_token'] == card_token
 
    def test_build_output_has_no_raw_pan(self, tokeniser, sample_raw_transaction):
        """Output must not contain the raw PAN (first6+last4 combined)."""
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        output_str = json.dumps(output)
        # Raw combined PAN should not appear
        assert "520000" + "1234" not in output_str
 
    def test_build_output_has_no_raw_email(self, tokeniser, sample_raw_transaction):
        """Raw customer email must not appear in tokenised output."""
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        assert 'john@example.com' not in json.dumps(output)
 
    def test_build_output_preserves_bin(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        assert output['bin'] == '520000'
        assert output['last4'] == '1234'
 
    def test_build_output_has_tokenisation_method(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        assert output['tokenisation_method'] == 'HMAC_SHA256_PSEUDONYMISATION'
 
    def test_build_output_has_customer_email_hash_not_raw(self, tokeniser, sample_raw_transaction):
        parsed = tokeniser.parse_flutterwave_message(sample_raw_transaction)
        card_token, _ = tokeniser.tokenise_card(parsed['first6'], parsed['last4'])
        customer_token, _ = tokeniser.tokenise_customer(
            parsed['customer_id'], parsed['customer_email'], parsed['customer_phone']
        )
        ref_token, _ = tokeniser.tokenise_reference(parsed['tx_ref'])
        output = tokeniser._build_tokenised_message(parsed, card_token, customer_token, ref_token, False)
        assert 'customer_email_hash' in output
        assert output['customer_email_hash'] != 'john@example.com'
 
 
# ── process_message tests ─────────────────────────────────────────────────────
 
class TestProcessMessage:
 
    def _make_mock_message(self, value: dict):
        """Create a mock Kafka message."""
        msg = MagicMock()
        msg.value.return_value = json.dumps(value).encode('utf-8')
        msg.error.return_value = None
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        return msg
 
    def test_process_valid_message_returns_true(self, tokeniser, sample_raw_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_raw_transaction)
        result = tokeniser.process_message(msg)
        assert result is True
 
    def test_process_valid_message_increments_stats(self, tokeniser, sample_raw_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_raw_transaction)
        tokeniser.process_message(msg)
        assert tokeniser.stats['total_consumed'] == 1
        assert tokeniser.stats['total_tokenised'] == 1
 
    def test_process_produces_to_output_topic(self, tokeniser, sample_raw_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_raw_transaction)
        tokeniser.process_message(msg)
        tokeniser.producer.produce.assert_called_once()
        call_kwargs = tokeniser.producer.produce.call_args
        assert call_kwargs[1]['topic'] == 'flutter_tokenised'
 
    def test_process_message_key_is_card_token(self, tokeniser, sample_raw_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_raw_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        key = call_kwargs['key'].decode('utf-8')
        assert key.startswith('FLW_')
 
    def test_process_invalid_json_sends_to_dlq(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b"not valid json {{{"
        msg.error.return_value = None
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        result = tokeniser.process_message(msg)
        assert result is False
        assert tokeniser.stats['total_dlq'] == 1
 
    def test_process_empty_message_returns_false(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = None
        msg.error.return_value = None
        result = tokeniser.process_message(msg)
        assert result is False
 
    def test_process_output_value_is_valid_json(self, tokeniser, sample_raw_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_raw_transaction)
        tokeniser.process_message(msg)
        call_kwargs = tokeniser.producer.produce.call_args[1]
        output = json.loads(call_kwargs['value'].decode('utf-8'))
        assert isinstance(output, dict)
        assert 'card_token' in output
        assert 'customer_token' in output
        assert 'ref_token' in output
 
    def test_process_flat_transaction(self, tokeniser, sample_flat_transaction):
        tokeniser.producer = MagicMock()
        msg = self._make_mock_message(sample_flat_transaction)
        result = tokeniser.process_message(msg)
        assert result is True
 
 
# ── DLQ tests ─────────────────────────────────────────────────────────────────
 
class TestDLQ:
 
    def test_dlq_message_has_error_reason(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b'{"test": "data"}'
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        tokeniser._send_to_dlq(msg, "Test error reason")
        call_kwargs = tokeniser.producer.produce.call_args[1]
        payload = json.loads(call_kwargs['value'].decode('utf-8'))
        assert payload['error_reason'] == 'Test error reason'
 
    def test_dlq_message_has_service_name(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b'{"test": "data"}'
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        tokeniser._send_to_dlq(msg, "error")
        call_kwargs = tokeniser.producer.produce.call_args[1]
        payload = json.loads(call_kwargs['value'].decode('utf-8'))
        assert payload['service'] == 'flutterwave-tokeniser'
 
    def test_dlq_increments_stat(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b'{"test": "data"}'
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        tokeniser._send_to_dlq(msg, "error")
        assert tokeniser.stats['total_dlq'] == 1
 
    def test_dlq_topic_is_correct(self, tokeniser):
        tokeniser.producer = MagicMock()
        msg = MagicMock()
        msg.value.return_value = b'{"test": "data"}'
        msg.topic.return_value = 'flutter_raw'
        msg.partition.return_value = 0
        msg.offset.return_value = 0
        tokeniser._send_to_dlq(msg, "error")
        call_kwargs = tokeniser.producer.produce.call_args[1]
        assert call_kwargs['topic'] == 'flutter_dlq'
 
 
# ── Cache eviction tests ──────────────────────────────────────────────────────
 
class TestCacheEviction:
 
    def test_cache_evicts_when_full(self):
        config = FlutterwaveTokeniserConfig(
            hmac_secret="test_secret",
            max_cache_size=10,
        )
        tokeniser = FlutterwaveTokeniser(config)
 
        # Fill cache beyond limit
        for i in range(15):
            tokeniser.tokenise_card(f"52000{i%10}", f"{1000+i}")
 
        assert len(tokeniser.card_token_cache) <= 10
 
    def test_cache_continues_working_after_eviction(self):
        config = FlutterwaveTokeniserConfig(
            hmac_secret="test_secret",
            max_cache_size=5,
        )
        tokeniser = FlutterwaveTokeniser(config)
        for i in range(10):
            token, _ = tokeniser.tokenise_card(f"52000{i%10}", f"{1000+i}")
            assert token is not None
 
 
# ── Stats tests ───────────────────────────────────────────────────────────────
 
class TestStats:
 
    def test_stats_initialised_to_zero(self, tokeniser):
        for key, value in tokeniser.stats.items():
            assert value == 0 or value == 0.0, f"Stat {key} should be 0, got {value}"
 
    def test_card_token_stat_increments(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")
        assert tokeniser.stats['card_tokens'] == 1
 
    def test_customer_token_stat_increments(self, tokeniser):
        tokeniser.tokenise_customer("1001", "a@b.com", "254700000001")
        assert tokeniser.stats['customer_tokens'] == 1
 
    def test_tokenisation_time_is_recorded(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")
        assert tokeniser.stats['tokenisation_total_ms'] > 0
 
    def test_cache_miss_stat_increments(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")
        assert tokeniser.stats['cache_misses'] == 1
 
    def test_cache_hit_stat_increments(self, tokeniser):
        tokeniser.tokenise_card("520000", "1234")
        tokeniser.tokenise_card("520000", "1234")
        assert tokeniser.stats['cache_hits'] >= 1
 
 
# ── Integration test (requires Kafka) ────────────────────────────────────────
 
class TestFlutterwaveTokeniserIntegration:
    """
    Full round-trip integration test.
    Requires Kafka running on 127.0.0.1:9092.
    Skipped automatically if Kafka is unreachable.
    """
 
    @pytest.fixture(autouse=True)
    def check_kafka(self):
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
            admin.list_topics(timeout=5)
        except Exception:
            pytest.skip("Kafka not reachable — skipping integration tests")
 
    def test_full_round_trip(self, config, sample_raw_transaction):
        from confluent_kafka import Consumer, Producer
        from confluent_kafka.admin import AdminClient, NewTopic
        import time
 
        # Ensure topics exist
        admin = AdminClient({"bootstrap.servers": "127.0.0.1:9092"})
        for topic in ["flutter_raw", "flutter_tokenised", "flutter_dlq"]:
            try:
                admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
            except Exception:
                pass
 
        # Subscribe consumer before producing
        consumer = Consumer({
            "bootstrap.servers": "127.0.0.1:9092",
            "group.id": f"test-flutter-tokenised-{int(time.time())}",
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["flutter_tokenised"])
        consumer.poll(timeout=3.0)  # join group
 
        # Produce raw message
        producer = Producer({"bootstrap.servers": "127.0.0.1:9092", "acks": "all"})
        producer.produce(
            topic="flutter_raw",
            value=json.dumps(sample_raw_transaction).encode("utf-8"),
        )
        producer.flush(timeout=10)
 
        # Run tokeniser for one message
        tokeniser = FlutterwaveTokeniser(config)
        tokeniser.setup()
 
        raw_consumer = tokeniser.consumer
        deadline = time.time() + 15
        processed = False
        while time.time() < deadline and not processed:
            msg = raw_consumer.poll(timeout=1.0)
            if msg and not msg.error():
                processed = tokeniser.process_message(msg)
        tokeniser.producer.flush(timeout=10)
 
        # Consume tokenised message
        received = None
        deadline = time.time() + 15
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg and not msg.error():
                received = json.loads(msg.value().decode("utf-8"))
                break
 
        consumer.close()
        tokeniser.shutdown()
 
        assert received is not None, "No tokenised message received"
        assert "card_token" in received
        assert received["card_token"].startswith("FLW_")
        assert "customer_token" in received
        assert "john@example.com" not in json.dumps(received)

