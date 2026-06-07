import hmac
import json
import random
import uuid
import time
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import numpy as np
from faker import Faker

#set seeds for reproducibility
random.seed(42)
np.random.seed(42)
fake=Faker()
Faker.seed(42)

import os
import warnings

class TransactionDataGenerator:
    def __init__(self):
        secret = os.environ.get("WEBHOOK_HMAC_SECRET")
        if not secret:
            warnings.warn(
                "WEBHOOK_HMAC_SECRET not set — using default dev secret. "
                "Never use this in production.",
                stacklevel=2,
            )
            secret = "shared_secret_key_for_webhook_verification_2024"
        self.hmac_secret = secret.encode("utf-8")

    #Luhn helpers (used to check if the digits of a card are valid)
    @staticmethod
    def _apply_luhn_check_digit(pan_without_check: str) -> str:
        """
                Given a PAN missing its final check digit, calculate and append it.
                Input must be exactly 15 digits for a 16-digit card (or N-1 for any length).
        """
        total = 0
        for i, digit in enumerate(reversed(pan_without_check)):
            n = int(digit)
            if i % 2 == 0:
                n *= 2
                if n > 9:
                    n -= 9
            total += n

        check_digit=(10-(total % 10)) % 10
        return pan_without_check + str(check_digit)

    @staticmethod
    def _is_luhn_valid(pan:str) -> bool:
        """
        Verigy a full APN passes the luhn check
        :param pan:
        :return:
        """

        total = 0
        for i, digit in enumerate(reversed(pan)):
            n = int(digit)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    @staticmethod
    def _generate_pan(bin_prefix: str, total_length: int=16) -> str:
        """

        :param bin_prefix:
        :param total_length: 16
        :return: luhn-valid pan
        """
        middle_length = total_length - len(bin_prefix) -1
        middle = ''.join([str(random.randint(0, 9)) for _ in range(middle_length)])
        return TransactionDataGenerator._apply_luhn_check_digit(bin_prefix + middle)

    def generate_visa_transaction(self) -> Dict:
        """Generate a Visa ISO 8583 transaction with a Luhn-valid PAN."""
        visa_bins=['412345', '498765', '432187', '445678', '453201', '471610']
        bin_prefix = random.choice(visa_bins)
        pan = self._generate_pan(bin_prefix, total_length=16)

        assert self._is_luhn_valid(pan)
        transaction ={
            'source': 'VISA',
            'source_type': 'ISO_8583',
            'message_type': random.choice(['0100', '0110', '0200', '0210']),
            'bitmap': {
                'DE002': pan,
                'DE003': random.choice(['PURCHASE', 'WITHDRAWAL', 'BALANCE_INQUIRY']),
                'DE004': round(random.uniform(10, 5000), 2),
                'DE005': round(random.uniform(10, 5000), 2),
                'DE007': datetime.now().strftime('%m%d%H%M%S'),
                'DE011': str(random.randint(100000, 999999)),
                'DE012': datetime.now().strftime('%H%M%S'),
                'DE013': datetime.now().strftime('%m%d'),
                'DE018': random.choice(['5311', '5812', '5411', '7011']),
                'DE022': random.choice(['01', '02', '03', '05']),
                'DE025': random.choice(['01', '02']),
                'DE032': str(random.randint(10000, 99999)),
                'DE033': str(random.randint(10000, 99999)),
                'DE035': f"{pan}={random.randint(1000, 9999)}",
                'DE041': f"TERM{random.randint(1000, 9999)}",
                'DE042': f"MERC{random.randint(10000, 99999)}",
                'DE043': fake.company(),
                'DE049': random.choice(['840', '826', '404', '710']),
                'DE053': str(random.randint(100, 999)),
            },
            'card_data': {
                'bin': bin_prefix,
                'last4': pan[-4:],
                'card_type': 'CREDIT' if random.random() > 0.6 else 'DEBIT',
                'issuer_country': random.choice(['US', 'GB', 'KE', 'ZA']),
            },
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_mastercard_transaction(self) -> Dict:
        mc_bins=['512345', '534567', '545678', '556789', '522234', '531234']
        bin_prefix = random.choice(mc_bins)
        pan = self._generate_pan(bin_prefix, total_length=16)

        assert self._is_luhn_valid(pan)
        transaction ={
            'source': 'MASTERCARD',
            'source_type': 'ISO_8583',
            'message_type': random.choice(['0100', '0110', '0200', '0210', '0400']),
            'bitmap': {
                'DE002': pan,
                'DE003': random.choice(['PURCHASE', 'WITHDRAWAL', 'TRANSFER']),
                'DE004': round(random.uniform(10, 10000), 2),
                'DE005': round(random.uniform(10, 10000), 2),
                'DE007': datetime.now().strftime('%m%d%H%M%S'),
                'DE011': str(random.randint(100000, 999999)),
                'DE012': datetime.now().strftime('%H%M%S'),
                'DE013': datetime.now().strftime('%m%d'),
                'DE018': random.choice(['5311', '5812', '5411', '7011', '5732']),
                'DE022': random.choice(['01', '02', '03', '05', '07']),
                'DE025': random.choice(['01', '02', '03']),
                'DE032': str(random.randint(10000, 99999)),
                'DE033': str(random.randint(10000, 99999)),
                'DE035': f"{pan}={random.randint(1000, 9999)}",
                'DE041': f"TERM{random.randint(1000, 9999)}",
                'DE042': f"MERC{random.randint(10000, 99999)}",
                'DE043': fake.company(),
                'DE049': random.choice(['840', '826', '404', '710', '978']),
                'DE053': str(random.randint(100, 999)),
            },
            'card_data': {
                'bin': bin_prefix,
                'last4': pan[-4:],
                'card_type': 'WORLD_ELITE' if random.random() > 0.8 else 'STANDARD',
                'issuer_country': random.choice(['US', 'GB', 'KE', 'NG', 'ZA']),
            },
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_mpesa_transaction(self) -> Dict:
        """
        Generate an MPESA transaction
        :return: transaction as Dict
        """

        transaction_id = f"MP{random.randint(100000000, 999999999)}"
        amount = round(random.uniform(1, 400000), 2)
        msisdn = f"254{random.randint(700000000, 799999999)}"

        payload_string=f"{transaction_id}{amount}{msisdn}"
        signature = hmac.new(
            self.hmac_secret,
            msg=payload_string.encode(),
            digestmod=hashlib.sha256
        ).hexdigest()

        transaction = {
            'source': 'MPESA',
            'source_type': 'DARAJA_WEBHOOK',
            'webhook_payload': {
                'TransactionType': random.choice([
                    'CustomerPayBillOnline', 'CustomerBuyGoodsOnline', 'STKPush'
                ]),
                'TransID': transaction_id,
                'TransTime': datetime.now().strftime('%Y%m%d%H%M%S'),
                'TransAmount': amount,
                'BusinessShortCode': random.randint(100000, 999999),
                'BillRefNumber': f'INV{random.randint(10000, 99999)}',
                'InvoiceNumber': f'INV{random.randint(10000, 99999)}',
                'OrgAccountBalance': round(random.uniform(10000, 1000000), 2),
                'ThirdPartyTransID': f'TP{random.randint(100000, 999999)}',
                'MSISDN': msisdn,
                'FirstName': fake.first_name(),
                'MiddleName': fake.last_name(),
                'LastName': fake.last_name(),
                'TransactionReceipt': f'RCT{random.randint(100000, 999999)}',
            },
            'signature': signature,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_flutterwave_transaction(self) -> Dict:
        transaction_ref=f"TX{int(time.time())}{random.randint(100, 999)}"
        amount=round(random.uniform(1, 400000), 2)
        transaction_id = random.randint(1000000, 9999999)

        payload = {
            'id': transaction_id,
            'tx_ref': transaction_ref,
            'amount': amount,
        }
        verification_hash = hmac.new(
            self.hmac_secret,
            json.dumps(payload, sort_keys=True).encode(),
            hashlib.sha512
        ).hexdigest()

        transaction = {
            'source': 'FLUTTERWAVE',
            'source_type': 'WEBHOOK',
            'webhook_payload': {
                'event': random.choice([
                    'charge.completed', 'transfer.completed', 'payment.verified'
                ]),
                'data': {
                    'id': transaction_id,
                    'tx_ref': transaction_ref,
                    'flw_ref': f'FLW{random.randint(100000000, 999999999)}',
                    'device_fingerprint': hashlib.md5(
                        f"device_{random.randint(1, 10000)}".encode()
                    ).hexdigest(),
                    'amount': amount,
                    'currency': random.choice(['KES', 'USD', 'GBP', 'EUR']),
                    'charged_amount': amount,
                    'app_fee': round(amount * 0.025, 2),
                    'merchant_fee': round(amount * 0.01, 2),
                    'processor_response': random.choice(['Approved', 'Declined']),
                    'auth_model': random.choice(['PIN', 'NOAUTH', 'VBV', 'OTP']),
                    'payment_type': random.choice(['card', 'mpesa', 'banktransfer']),
                    'status': random.choice(['successful', 'pending', 'failed']),
                    'customer': {
                        'id': random.randint(1000, 9999),
                        'name': fake.name(),
                        'phone_number': f'254{random.randint(700000000, 799999999)}',
                        'email': fake.email(),
                    },
                    'card': {
                        'first_6digits': random.choice(['520000', '530000', '540000']),
                        'last_4digits': str(random.randint(1000, 9999)),
                        'issuer': random.choice(['VISA', 'MASTERCARD']),
                        'country': 'KE',
                        'type': random.choice(['DEBIT', 'CREDIT']),
                    },
                },
            },
            'verification_hash': verification_hash,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction

    def generate_pesapal_transaction(self) -> Dict:
        """Generate a Pesapal IPN callback transaction."""

        merchant_ref = f'REF{int(time.time())}{random.randint(100, 999)}'
        tracking_id = f'TRACK{random.randint(100000000, 999999999)}'
        amount = round(random.uniform(10, 50000), 2)

        signature_string = f"{merchant_ref}{tracking_id}COMPLETED"
        ipn_signature = base64.b64encode(
            hmac.new(self.hmac_secret, signature_string.encode(), hashlib.sha1).digest()
        ).decode()

        transaction = {
            'source': 'PESAPAL',
            'source_type': 'IPN_CALLBACK',
            'ipn_payload': {
                'pesapal_merchant_reference': merchant_ref,
                'pesapal_transaction_tracking_id': tracking_id,
                'payment_status': random.choice(['COMPLETED', 'PENDING', 'FAILED', 'INVALID']),
                'payment_method': random.choice(['VISA', 'MASTERCARD', 'MPESA', 'AIRTEL_MONEY']),
                'amount': amount,
                'currency': random.choice(['KES', 'USD', 'UGX', 'TZS', 'GBP']),
                'created_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'confirmation_code': f'CODE{random.randint(10000, 99999)}',
                'payment_account': f'254{random.randint(700000000, 799999999)}',
                'customer_email': fake.email(),
                'customer_phone': f'254{random.randint(700000000, 799999999)}',
                'customer_first_name': fake.first_name(),
                'customer_last_name': fake.last_name(),
                'description': f'Payment for order {random.randint(1000, 9999)}',
            },
            'ipn_signature': ipn_signature,
            'timestamp': datetime.now().isoformat(),
            'event_time_ms': int(time.time() * 1000),
        }

        return transaction