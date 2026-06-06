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