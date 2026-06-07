import base64
import json

import pytest
import hmac
import hashlib
from datetime import datetime
from unittest.mock import Mock,patch,ANY,MagicMock
import random
import time
from data_generator.data_generator import TransactionDataGenerator

class TestVisaTransactionData:

    def test_transaction_structure(self):
        transaction = TransactionDataGenerator().generate_visa_transaction()

        assert isinstance(transaction, dict)

        expected_keys = {
            'source',
            'source_type',
            'bitmap',
            'card_data',
            'timestamp',
            'event_time_ms'
        }

        assert expected_keys.issubset(transaction.keys())

    def test_source_is_visa(self):
        transaction = TransactionDataGenerator().generate_visa_transaction()

        assert transaction['source'] == 'VISA'
        assert transaction['source_type']=="ISO_8583"

    def test_generated_pan_is_valid(self):
        transaction = TransactionDataGenerator().generate_visa_transaction()

        pan = transaction['bitmap']["DE002"]

        assert len(pan) == 16
        assert TransactionDataGenerator._is_luhn_valid(pan)

    def test_pan_uses_visa_bin(self):
        transaction = TransactionDataGenerator().generate_visa_transaction()

        pan = transaction['bitmap']["DE002"]
        assert pan.startswith(
            (
                "412345", "498765", "432187",
                "445678", "453201", "471610"
            )
        )

    def test_card_data_matches_pan(self):
        transaction = TransactionDataGenerator().generate_visa_transaction()
        pan = transaction['bitmap']["DE002"]
        assert transaction["card_data"]['bin'] == pan[:6]
        assert transaction["card_data"]["last4"] == pan[-4:]

class TestMastercardData:
    def test_transaction_structure(self):
        transaction = TransactionDataGenerator().generate_mastercard_transaction()

        assert isinstance(transaction, dict)

        expected_keys = {
            'source',
            'source_type',
            'bitmap',
            'card_data',
            'timestamp',
            'event_time_ms'
        }

        assert expected_keys.issubset(transaction.keys())

    def test_source_is_visa(self):
        transaction = TransactionDataGenerator().generate_mastercard_transaction()

        assert transaction['source'] == 'MASTERCARD'
        assert transaction['source_type']=="ISO_8583"

    def test_generated_pan_is_valid(self):
        transaction = TransactionDataGenerator().generate_mastercard_transaction()

        pan = transaction['bitmap']["DE002"]

        assert len(pan) == 16
        assert TransactionDataGenerator._is_luhn_valid(pan)

    def test_pan_uses_visa_bin(self):
        transaction = TransactionDataGenerator().generate_mastercard_transaction()

        pan = transaction['bitmap']["DE002"]
        assert pan.startswith(
            (
                '512345', '534567', '545678', '556789', '522234', '531234'
            )
        )

    def test_card_data_matches_pan(self):
        transaction = TransactionDataGenerator().generate_mastercard_transaction()
        pan = transaction['bitmap']["DE002"]
        assert transaction["card_data"]['bin'] == pan[:6]
        assert transaction["card_data"]["last4"] == pan[-4:]

class TestMpesaData:
    @pytest.fixture
    def generator(self):

        generator = TransactionDataGenerator()
        return generator

    def test_transaction_structure(self):
        transaction = TransactionDataGenerator().generate_mpesa_transaction()
        assert isinstance(transaction, dict)
        expected_keys = {
            'source',
            'source_type',
            'webhook_payload',
            'signature',

            'timestamp',
            'event_time_ms'
        }

    def test_webhook_payload_has_all_fields(self, generator):

        transaction = generator.generate_mpesa_transaction()
        payload = transaction['webhook_payload']

        expected_fields = [
            'TransactionType', 'TransID', 'TransTime', 'TransAmount',
            'BusinessShortCode', 'BillRefNumber', 'InvoiceNumber',
            'OrgAccountBalance', 'ThirdPartyTransID', 'MSISDN',
            'FirstName', 'MiddleName', 'LastName', 'TransactionReceipt'
        ]

        for field in expected_fields:
            assert field in payload

    #Data validation tests
    def test_transaction_id_format(self,generator):

        transaction = generator.generate_mpesa_transaction()
        trans_id = transaction['webhook_payload']['TransID']

        assert trans_id.startswith('MP')
        assert len(trans_id) == 11
        assert trans_id[2:].isdigit()

    def test_amount_range_and_type(self,generator):
        transaction = generator.generate_mpesa_transaction()
        amount=transaction['webhook_payload']['TransAmount']

        assert isinstance(amount, float)
        assert 1<=amount<=400000
        assert len(str(amount).split('.')[1]) <= 2

    def test_msisdn_format(self,generator):

        transaction = generator.generate_mpesa_transaction()
        msisdn = transaction['webhook_payload']['MSISDN']

        assert msisdn.startswith('254')
        assert len(msisdn) == 12
        assert msisdn.isdigit()

    def test_transaction_time_format(self,generator):

        transaction = generator.generate_mpesa_transaction()
        trans_time = transaction['webhook_payload']['TransTime']

        assert len(trans_time) == 14
        assert trans_time.isdigit()

        #validate if it's a proper datetime
        try:
            datetime.strptime(trans_time, '%Y%m%d%H%M%S')
        except ValueError:
            pytest.fail(f'Invalid time format: {trans_time}')


    def test_signature_changes_with_transaction_data(self, generator):
        """Test that different transactions produce different signatures."""
        generator=TransactionDataGenerator()
        transaction1 = generator.generate_mpesa_transaction()
        transaction2 = generator.generate_mpesa_transaction()

        # Small chance of collision, but practically impossible with random data
        assert transaction1['signature'] != transaction2['signature']



    def test_timestamp_is_iso_format(self, generator):
        """Test that timestamp follows ISO format."""
        generator= TransactionDataGenerator()
        transaction = generator.generate_mpesa_transaction()
        timestamp = transaction['timestamp']

        try:
            datetime.fromisoformat(timestamp)
        except ValueError:
            pytest.fail(f"Invalid ISO timestamp: {timestamp}")

    def test_event_time_ms_is_milliseconds(self, generator):
        """Test that event_time_ms is a millisecond timestamp."""

        transaction = generator.generate_mpesa_transaction()
        event_time_ms = transaction['event_time_ms']

        assert isinstance(event_time_ms, int)
        # Should be roughly current time
        current_time_ms = int(time.time() * 1000)
        assert current_time_ms - event_time_ms < 1000  # Within 1 second

    # === FIELD VALIDATION TESTS ===

    def test_source_and_source_type_are_correct(self, generator):
        """Test that source fields have expected static values."""
        generator=TransactionDataGenerator()
        transaction = generator.generate_mpesa_transaction()

        assert transaction['source'] == 'MPESA'
        assert transaction['source_type'] == 'DARAJA_WEBHOOK'

    @pytest.mark.parametrize("transaction_type", [
        'CustomerPayBillOnline',
        'CustomerBuyGoodsOnline',
        'STKPush'
    ])
    def test_transaction_type_is_valid(self, generator, transaction_type):
        """Test that TransactionType is one of the valid values."""
        generator=TransactionDataGenerator()
        with patch('random.choice', return_value=transaction_type):
            transaction = generator.generate_mpesa_transaction()
            actual_type = transaction['webhook_payload']['TransactionType']
            assert actual_type == transaction_type

    def test_business_shortcode_is_in_range(self, generator):
        """Test that BusinessShortCode is a 6-digit number."""
        generator=TransactionDataGenerator()
        transaction = generator.generate_mpesa_transaction()
        shortcode = transaction['webhook_payload']['BusinessShortCode']

        assert 100000 <= shortcode <= 999999
        assert isinstance(shortcode, int)

    def test_reference_numbers_format(self, generator):
        """Test that reference numbers follow expected patterns."""
        generator=TransactionDataGenerator()
        transaction = generator.generate_mpesa_transaction()
        payload = transaction['webhook_payload']

        assert payload['BillRefNumber'].startswith('INV')
        assert payload['InvoiceNumber'].startswith('INV')
        assert payload['ThirdPartyTransID'].startswith('TP')
        assert payload['TransactionReceipt'].startswith('RCT')

    # EDGE CASE TESTS

    def test_amount_boundaries(self, generator):
        """Test minimum and maximum possible amounts."""
        # Test minimum amount
        generator=TransactionDataGenerator()
        with patch('random.uniform', return_value=10.00):
            transaction = generator.generate_mpesa_transaction()
            assert transaction['webhook_payload']['TransAmount'] == 10.00

        # Test maximum amount
        with patch('random.uniform', return_value=50000.00):
            transaction = generator.generate_mpesa_transaction()
            assert transaction['webhook_payload']['TransAmount'] == 50000.00



    def test_org_account_balance_is_valid(self, generator):
        """Test that organization account balance is within range."""
        generator=TransactionDataGenerator()
        transaction = generator.generate_mpesa_transaction()
        balance = transaction['webhook_payload']['OrgAccountBalance']

        assert isinstance(balance, float)
        assert 10000 <= balance <= 1000000


    def test_batch_generation_consistency(self, generator):
        """Test that generating multiple transactions doesn't cause issues."""
        transactions = [generator.generate_mpesa_transaction() for _ in range(100)]

        # All transactions should have unique IDs
        trans_ids = [t['webhook_payload']['TransID'] for t in transactions]
        assert len(set(trans_ids)) == len(trans_ids)

        # All should have valid structure
        for transaction in transactions:
            assert transaction['signature']
            assert transaction['timestamp']

    # === ERROR HANDLING TESTS ===

    def test_hmac_secret_is_required(self, generator):
        """Test that HMAC secret must be provided."""
        generator=TransactionDataGenerator()
        generator.hmac_secret = None
        with pytest.raises(Exception):  # TypeError or AttributeError
            generator.generate_mpesa_transaction()


class TestFlutterwaveTransaction:
    """Tests for Flutterwave webhook transaction generation."""

    @pytest.fixture
    def generator(self):
        """Create a TransactionDataGenerator instance with test secret."""
        generator = TransactionDataGenerator()
        generator.hmac_secret = b'test_secret_key_123'
        return generator

    # === STRUCTURE TESTS ===

    def test_transaction_has_required_keys(self, generator):
        """Test that transaction contains all required top-level keys."""
        transaction = generator.generate_flutterwave_transaction()

        assert 'source' in transaction
        assert 'source_type' in transaction
        assert 'webhook_payload' in transaction
        assert 'verification_hash' in transaction
        assert 'timestamp' in transaction
        assert 'event_time_ms' in transaction

    def test_webhook_payload_structure(self, generator):
        """Test that webhook payload has correct structure."""
        transaction = generator.generate_flutterwave_transaction()
        payload = transaction['webhook_payload']

        assert 'event' in payload
        assert 'data' in payload
        assert isinstance(payload['data'], dict)

    def test_data_payload_has_all_fields(self, generator):
        """Test that data payload contains all expected fields."""
        transaction = generator.generate_flutterwave_transaction()
        data = transaction['webhook_payload']['data']

        expected_fields = [
            'id', 'tx_ref', 'flw_ref', 'device_fingerprint', 'amount',
            'currency', 'charged_amount', 'app_fee', 'merchant_fee',
            'processor_response', 'auth_model', 'payment_type', 'status',
            'customer', 'card'
        ]

        for field in expected_fields:
            assert field in data, f"Missing field: {field}"

    def test_customer_object_structure(self, generator):
        """Test that customer object has all required fields."""
        transaction = generator.generate_flutterwave_transaction()
        customer = transaction['webhook_payload']['data']['customer']

        assert 'id' in customer
        assert 'name' in customer
        assert 'phone_number' in customer
        assert 'email' in customer
        assert isinstance(customer['id'], int)
        assert isinstance(customer['name'], str)

    def test_card_object_structure(self, generator):
        """Test that card object has all required fields."""
        transaction = generator.generate_flutterwave_transaction()
        card = transaction['webhook_payload']['data']['card']

        assert 'first_6digits' in card
        assert 'last_4digits' in card
        assert 'issuer' in card
        assert 'country' in card
        assert 'type' in card
        assert len(card['last_4digits']) == 4
        assert card['last_4digits'].isdigit()

    # === DATA VALIDATION TESTS ===

    def test_tx_ref_format(self, generator):
        """Test that transaction reference follows TX timestamp format."""
        with patch('time.time', return_value=1234567890):
            transaction = generator.generate_flutterwave_transaction()
            tx_ref = transaction['webhook_payload']['data']['tx_ref']

            assert tx_ref.startswith('TX')
            assert '1234567890' in tx_ref
            # Should have random 3-digit suffix
            suffix = tx_ref.split('1234567890')[1]
            assert len(suffix) == 3
            assert suffix.isdigit()

    def test_amount_range_and_type(self, generator):
        """Test that amount is a float between 10 and 5000."""
        transaction = generator.generate_flutterwave_transaction()
        amount = transaction['webhook_payload']['data']['amount']

        assert isinstance(amount, float)
        assert 1 <= amount <= 400000
        assert len(str(amount).split('.')[1]) <= 2

    def test_flw_ref_format(self, generator):
        """Test that Flutterwave reference follows FLW format."""
        transaction = generator.generate_flutterwave_transaction()
        flw_ref = transaction['webhook_payload']['data']['flw_ref']

        assert flw_ref.startswith('FLW')
        assert len(flw_ref) == 12  # FLW + 9 digits
        assert flw_ref[3:].isdigit()

    def test_device_fingerprint_is_md5(self, generator):
        """Test that device fingerprint is a valid MD5 hash."""
        transaction = generator.generate_flutterwave_transaction()
        fingerprint = transaction['webhook_payload']['data']['device_fingerprint']

        assert len(fingerprint) == 32  # MD5 produces 32 hex characters
        assert all(c in '0123456789abcdef' for c in fingerprint)

    def test_fees_are_calculated_correctly(self, generator):
        """Test that app fee and merchant fee are calculated correctly."""
        with patch('random.uniform', return_value=100.00):
            transaction = generator.generate_flutterwave_transaction()
            data = transaction['webhook_payload']['data']

            expected_app_fee = round(100.00 * 0.025, 2)  # 2.5%
            expected_merchant_fee = round(100.00 * 0.01, 2)  # 1%

            assert data['app_fee'] == expected_app_fee
            assert data['merchant_fee'] == expected_merchant_fee

    def test_charged_amount_equals_amount(self, generator):
        """Test that charged amount equals the original amount."""
        transaction = generator.generate_flutterwave_transaction()
        data = transaction['webhook_payload']['data']

        assert data['charged_amount'] == data['amount']

    def test_currency_is_valid(self, generator):
        """Test that currency is one of the supported values."""
        valid_currencies = ['KES', 'USD', 'GBP', 'EUR']
        transaction = generator.generate_flutterwave_transaction()
        currency = transaction['webhook_payload']['data']['currency']

        assert currency in valid_currencies

    @pytest.mark.parametrize("event_type", [
        'charge.completed', 'transfer.completed', 'payment.verified'
    ])
    def test_event_type_is_valid(self, generator, event_type):
        """Test that event type can be each valid value."""
        with patch('random.choice', return_value=event_type):
            transaction = generator.generate_flutterwave_transaction()
            actual_event = transaction['webhook_payload']['event']
            assert actual_event == event_type

    @pytest.mark.parametrize("status", ['successful', 'pending', 'failed'])
    def test_payment_status_is_valid(self, generator, status):
        """Test that payment status can be each valid value."""
        with patch('random.choice', return_value=status):
            transaction = generator.generate_flutterwave_transaction()
            actual_status = transaction['webhook_payload']['data']['status']
            assert actual_status == status

    def test_phone_number_format(self, generator):
        """Test that phone number follows Kenyan format."""
        transaction = generator.generate_flutterwave_transaction()
        phone = transaction['webhook_payload']['data']['customer']['phone_number']

        assert phone.startswith('254')
        assert len(phone) == 12
        assert phone.isdigit()

    def test_email_format(self, generator):
        """Test that email is valid format."""
        transaction = generator.generate_flutterwave_transaction()
        email = transaction['webhook_payload']['data']['customer']['email']

        assert '@' in email
        assert '.' in email.split('@')[1]

    def test_card_first_6digits_valid(self, generator):
        """Test that card's first 6 digits are valid BINs."""
        valid_bins = ['520000', '530000', '540000']
        transaction = generator.generate_flutterwave_transaction()
        first_6 = transaction['webhook_payload']['data']['card']['first_6digits']

        assert first_6 in valid_bins
        assert len(first_6) == 6
        assert first_6.isdigit()



    def test_verification_hash_changes_with_payload(self, generator):
        """Test that different payloads produce different hashes."""
        transaction1 = generator.generate_flutterwave_transaction()
        transaction2 = generator.generate_flutterwave_transaction()

        assert transaction1['verification_hash'] != transaction2['verification_hash']

    def test_verification_hash_verifiable(self, generator):
        """Test that verification hash can be verified."""
        transaction = generator.generate_flutterwave_transaction()
        data = transaction['webhook_payload']['data']

        # Recreate verification hash
        payload = {
            'id': data['id'],
            'tx_ref': data['tx_ref'],
            'amount': data['amount']
        }
        expected_hash = hmac.new(
            generator.hmac_secret,
            json.dumps(payload, sort_keys=True).encode(),
            hashlib.sha512
        ).hexdigest()

        assert hmac.compare_digest(transaction['verification_hash'], expected_hash)

    # === TIMESTAMP TESTS ===

    def test_timestamp_is_iso_format(self, generator):
        """Test that timestamp follows ISO format."""
        transaction = generator.generate_flutterwave_transaction()
        timestamp = transaction['timestamp']

        try:
            datetime.fromisoformat(timestamp)
        except ValueError:
            pytest.fail(f"Invalid ISO timestamp: {timestamp}")

    def test_event_time_ms_is_milliseconds(self, generator):
        """Test that event_time_ms is a millisecond timestamp."""
        transaction = generator.generate_flutterwave_transaction()
        event_time_ms = transaction['event_time_ms']

        assert isinstance(event_time_ms, int)
        current_time_ms = int(time.time() * 1000)
        assert abs(current_time_ms - event_time_ms) < 5000

    def test_source_fields_are_correct(self, generator):
        """Test that source fields have expected values."""
        transaction = generator.generate_flutterwave_transaction()

        assert transaction['source'] == 'FLUTTERWAVE'
        assert transaction['source_type'] == 'WEBHOOK'

    # === EDGE CASE TESTS ===

    def test_amount_boundaries(self, generator):
        """Test minimum and maximum amounts."""
        with patch('random.uniform', return_value=10.00):
            transaction = generator.generate_flutterwave_transaction()
            assert transaction['webhook_payload']['data']['amount'] == 10.00

        with patch('random.uniform', return_value=5000.00):
            transaction = generator.generate_flutterwave_transaction()
            assert transaction['webhook_payload']['data']['amount'] == 5000.00



    def test_fees_never_exceed_amount(self, generator):
        """Test that fees never exceed the original amount."""
        transaction = generator.generate_flutterwave_transaction()
        data = transaction['webhook_payload']['data']

        assert data['app_fee'] <= data['amount']
        assert data['merchant_fee'] <= data['amount']


class TestPesapalTransaction:
    """Tests for Pesapal IPN callback transaction generation."""

    @pytest.fixture
    def generator(self):
        """Create a TransactionDataGenerator instance with test secret."""
        generator = TransactionDataGenerator()
        generator.hmac_secret = b'test_secret_key_123'
        return generator

    # === STRUCTURE TESTS ===

    def test_transaction_has_required_keys(self, generator):
        """Test that transaction contains all required top-level keys."""
        transaction = generator.generate_pesapal_transaction()

        assert 'source' in transaction
        assert 'source_type' in transaction
        assert 'ipn_payload' in transaction
        assert 'ipn_signature' in transaction
        assert 'timestamp' in transaction
        assert 'event_time_ms' in transaction

    def test_ipn_payload_has_all_fields(self, generator):
        """Test that IPN payload contains all expected fields."""
        transaction = generator.generate_pesapal_transaction()
        payload = transaction['ipn_payload']

        expected_fields = [
            'pesapal_merchant_reference', 'pesapal_transaction_tracking_id',
            'payment_status', 'payment_method', 'amount', 'currency',
            'created_date', 'confirmation_code', 'payment_account',
            'customer_email', 'customer_phone', 'customer_first_name',
            'customer_last_name', 'description'
        ]

        for field in expected_fields:
            assert field in payload, f"Missing field: {field}"

    # === DATA VALIDATION TESTS ===

    def test_merchant_reference_format(self, generator):
        """Test that merchant reference follows REF timestamp format."""
        with patch('time.time', return_value=1234567890):
            transaction = generator.generate_pesapal_transaction()
            merchant_ref = transaction['ipn_payload']['pesapal_merchant_reference']

            assert merchant_ref.startswith('REF')
            assert '1234567890' in merchant_ref
            suffix = merchant_ref.split('1234567890')[1]
            assert len(suffix) == 3
            assert suffix.isdigit()

    def test_tracking_id_format(self, generator):
        """Test that tracking ID follows TRACK format."""
        transaction = generator.generate_pesapal_transaction()
        tracking_id = transaction['ipn_payload']['pesapal_transaction_tracking_id']

        assert tracking_id.startswith('TRACK')
        assert len(tracking_id) == 14  # TRACK + 9 digits
        assert tracking_id[5:].isdigit()

    def test_amount_range_and_type(self, generator):
        """Test that amount is a float between 10 and 50000."""
        transaction = generator.generate_pesapal_transaction()
        amount = transaction['ipn_payload']['amount']

        assert isinstance(amount, float)
        assert 10 <= amount <= 50000

    @pytest.mark.parametrize("status", ['COMPLETED', 'PENDING', 'FAILED', 'INVALID'])
    def test_payment_status_is_valid(self, generator, status):
        """Test that payment status can be each valid value."""
        with patch('random.choice', return_value=status):
            transaction = generator.generate_pesapal_transaction()
            actual_status = transaction['ipn_payload']['payment_status']
            assert actual_status == status

    @pytest.mark.parametrize("method", ['VISA', 'MASTERCARD', 'MPESA', 'AIRTEL_MONEY'])
    def test_payment_method_is_valid(self, generator, method):
        """Test that payment method can be each valid value."""
        with patch('random.choice', return_value=method):
            transaction = generator.generate_pesapal_transaction()
            actual_method = transaction['ipn_payload']['payment_method']
            assert actual_method == method

    def test_currency_is_valid(self, generator):
        """Test that currency is one of the supported values."""
        valid_currencies = ['KES', 'USD', 'UGX', 'TZS', 'GBP']
        transaction = generator.generate_pesapal_transaction()
        currency = transaction['ipn_payload']['currency']

        assert currency in valid_currencies

    def test_created_date_format(self, generator):
        """Test that created date follows YYYY-MM-DD HH:MM:SS format."""
        transaction = generator.generate_pesapal_transaction()
        created_date = transaction['ipn_payload']['created_date']

        try:
            datetime.strptime(created_date, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            pytest.fail(f"Invalid date format: {created_date}")

    def test_confirmation_code_format(self, generator):
        """Test that confirmation code follows CODE format."""
        transaction = generator.generate_pesapal_transaction()
        confirmation_code = transaction['ipn_payload']['confirmation_code']

        assert confirmation_code.startswith('CODE')
        assert len(confirmation_code) == 9  # CODE + 5 digits
        assert confirmation_code[4:].isdigit()

    def test_payment_account_format(self, generator):
        """Test that payment account follows phone number format."""
        transaction = generator.generate_pesapal_transaction()
        payment_account = transaction['ipn_payload']['payment_account']

        assert payment_account.startswith('254')
        assert len(payment_account) == 12
        assert payment_account.isdigit()

    def test_customer_phone_format(self, generator):
        """Test that customer phone follows phone number format."""
        transaction = generator.generate_pesapal_transaction()
        customer_phone = transaction['ipn_payload']['customer_phone']

        assert customer_phone.startswith('254')
        assert len(customer_phone) == 12
        assert customer_phone.isdigit()

    def test_customer_email_format(self, generator):
        """Test that customer email is valid format."""
        transaction = generator.generate_pesapal_transaction()
        email = transaction['ipn_payload']['customer_email']

        assert '@' in email
        assert '.' in email.split('@')[1]

    def test_customer_names_are_strings(self, generator):
        """Test that customer names are non-empty strings."""
        transaction = generator.generate_pesapal_transaction()
        payload = transaction['ipn_payload']

        assert isinstance(payload['customer_first_name'], str)
        assert isinstance(payload['customer_last_name'], str)
        assert len(payload['customer_first_name']) > 0
        assert len(payload['customer_last_name']) > 0

    def test_description_format(self, generator):
        """Test that description contains order reference."""
        transaction = generator.generate_pesapal_transaction()
        description = transaction['ipn_payload']['description']

        assert description.startswith('Payment for order ')
        order_number = description.split('order ')[1]
        assert order_number.isdigit()
        assert 1000 <= int(order_number) <= 9999

    # === SIGNATURE TESTS ===


    def test_ipn_signature_changes_with_data(self, generator):
        """Test that different transactions produce different signatures."""
        transaction1 = generator.generate_pesapal_transaction()
        transaction2 = generator.generate_pesapal_transaction()

        assert transaction1['ipn_signature'] != transaction2['ipn_signature']



    def test_signature_encoding_is_base64(self, generator):
        """Test that IPN signature is base64 encoded."""
        transaction = generator.generate_pesapal_transaction()
        signature = transaction['ipn_signature']

        # Base64 strings should only contain valid characters
        import base64
        try:
            base64.b64decode(signature)
        except Exception:
            pytest.fail("Signature is not valid base64")

    # === TIMESTAMP TESTS ===

    def test_timestamp_is_iso_format(self, generator):
        """Test that timestamp follows ISO format."""
        transaction = generator.generate_pesapal_transaction()
        timestamp = transaction['timestamp']

        try:
            datetime.fromisoformat(timestamp)
        except ValueError:
            pytest.fail(f"Invalid ISO timestamp: {timestamp}")

    def test_event_time_ms_is_milliseconds(self, generator):
        """Test that event_time_ms is a millisecond timestamp."""
        transaction = generator.generate_pesapal_transaction()
        event_time_ms = transaction['event_time_ms']

        assert isinstance(event_time_ms, int)
        current_time_ms = int(time.time() * 1000)
        assert abs(current_time_ms - event_time_ms) < 5000

    def test_source_fields_are_correct(self, generator):
        """Test that source fields have expected values."""
        transaction = generator.generate_pesapal_transaction()

        assert transaction['source'] == 'PESAPAL'
        assert transaction['source_type'] == 'IPN_CALLBACK'

    # === EDGE CASE TESTS ===

    def test_amount_boundaries(self, generator):
        """Test minimum and maximum amounts."""
        with patch('random.uniform', return_value=10.00):
            transaction = generator.generate_pesapal_transaction()
            assert transaction['ipn_payload']['amount'] == 10.00

        with patch('random.uniform', return_value=50000.00):
            transaction = generator.generate_pesapal_transaction()
            assert transaction['ipn_payload']['amount'] == 50000.00



    def test_phone_number_consistency(self, generator):
        """Test that payment account and customer phone follow same format."""
        transaction = generator.generate_pesapal_transaction()
        payload = transaction['ipn_payload']

        assert len(payload['payment_account']) == len(payload['customer_phone'])
        assert payload['payment_account'].startswith('254')
        assert payload['customer_phone'].startswith('254')


# Optional: Combined tests for all payment processors
class TestAllPaymentProcessors:
    """Tests that span across all payment processors."""

    @pytest.fixture
    def generator(self):
        generator = TransactionDataGenerator()
        generator.hmac_secret = b'test_secret_key_123'
        return generator

    def test_all_transactions_have_timestamp(self, generator):
        """Test that all transaction types have timestamp."""
        mpesa_tx = generator.generate_mpesa_transaction()
        flutterwave_tx = generator.generate_flutterwave_transaction()
        pesapal_tx = generator.generate_pesapal_transaction()

        assert 'timestamp' in mpesa_tx
        assert 'timestamp' in flutterwave_tx
        assert 'timestamp' in pesapal_tx

    def test_all_transactions_have_event_time_ms(self, generator):
        """Test that all transaction types have event_time_ms."""
        mpesa_tx = generator.generate_mpesa_transaction()
        flutterwave_tx = generator.generate_flutterwave_transaction()
        pesapal_tx = generator.generate_pesapal_transaction()

        assert 'event_time_ms' in mpesa_tx
        assert 'event_time_ms' in flutterwave_tx
        assert 'event_time_ms' in pesapal_tx

    def test_all_transactions_have_correct_source(self, generator):
        """Test that all transactions have correct source identification."""
        mpesa_tx = generator.generate_mpesa_transaction()
        flutterwave_tx = generator.generate_flutterwave_transaction()
        pesapal_tx = generator.generate_pesapal_transaction()

        assert mpesa_tx['source'] == 'MPESA'
        assert flutterwave_tx['source'] == 'FLUTTERWAVE'
        assert pesapal_tx['source'] == 'PESAPAL'