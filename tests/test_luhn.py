import pytest
from data_generator.data_generator import TransactionDataGenerator


# Apply luhn check digits


class TestApplyLuhnCheckDigit:
    def test_known_visa_test_card(self):
        result = TransactionDataGenerator._apply_luhn_check_digit("411111111111111")
        assert result == "4111111111111111"

    def test_known_mastercard_test_card2(self):
        result = TransactionDataGenerator._apply_luhn_check_digit("550000555555555")
        assert result == "5500005555555559"

    def test_output_is_one_digit_longer(self):
        pan_15 = "412345678901234"
        result = TransactionDataGenerator._apply_luhn_check_digit(pan_15)
        assert result.startswith(pan_15)

    def test_output_passes_luhn(self):
        result = TransactionDataGenerator._apply_luhn_check_digit("412345678901234")
        assert TransactionDataGenerator._is_luhn_valid(result)

    def test_check_digit_is_numeric(self):
        result = TransactionDataGenerator._apply_luhn_check_digit("412345678901234")
        check_digit = result[-1]
        assert check_digit.isdigit()

    def test_check_digit_zero_edge_case(self):
        pan_15 = "400000000000000"
        result = TransactionDataGenerator._apply_luhn_check_digit(pan_15)
        assert TransactionDataGenerator._is_luhn_valid(result)
        assert len(result) == 16

#is_luhn_valid
class TestLuhnValid:
    def test_valid_visa_test_card(self):
        assert TransactionDataGenerator._is_luhn_valid("4111111111111111") is True

    def test_valid_mastercard_test_card(self):
        assert TransactionDataGenerator._is_luhn_valid("5500005555555559") is True

    def test_invalid_one_digit_changed(self):
        """Changing the last digit must invalidate the card."""
        assert TransactionDataGenerator._is_luhn_valid("4111111111111112") is False

    def test_invalid_random_number(self):
        """A purely random number is very unlikely to pass Luhn."""
        assert TransactionDataGenerator._is_luhn_valid("1234567890123456") is False

    def test_invalid_all_zeros(self):
        # All zeros are actually Luhn-valid (sum=0, divisible by 10)
        # so we test a known invalid number instead
        assert TransactionDataGenerator._is_luhn_valid("0000000000000001") is False

    def test_valid_amex_15_digit(self):
        """Amex cards are 15 digits — Luhn works on any length."""
        assert TransactionDataGenerator._is_luhn_valid("378282246310005") is True

    def test_consistent_results(self):
        """Same input must always return the same result."""
        pan = "4111111111111111"
        results = [TransactionDataGenerator._is_luhn_valid(pan) for _ in range(10)]
        assert all(results)

#_generate_pan

class TestGeneratePan:
    def test_correct_length(self):
        pan=TransactionDataGenerator._generate_pan("412345",total_length=16)
        assert len(pan) == 16

    def test_preserve_bin(self):
        bin_prefix="412345"
        pan =TransactionDataGenerator._generate_pan(bin_prefix,total_length=16)
        assert pan.startswith(bin_prefix)

    def test_passes_luhn(self):
        pan = TransactionDataGenerator._generate_pan("412345",total_length=16)
        assert TransactionDataGenerator._is_luhn_valid(pan)


    def test_all_digits(self):
        pan = TransactionDataGenerator._generate_pan("412345",total_length=16)
        assert pan.isdigit()

    def test_visa_bin(self):
        pan = TransactionDataGenerator._generate_pan("412345",total_length=16)
        assert pan.startswith("412345")
        assert len(pan) == 16
        assert TransactionDataGenerator._is_luhn_valid(pan)

    def test_mastercard_binary(self):
        pan = TransactionDataGenerator._generate_pan("512345",total_length=16)
        assert pan.startswith("512345")
        assert TransactionDataGenerator._is_luhn_valid(pan)

    def test_100_consecutive_pans_all_valid(self):
        invalid = []
        for i in range(100):
            pan = TransactionDataGenerator._generate_pan("412345",total_length=16)
            if not TransactionDataGenerator._is_luhn_valid(pan):
                invalid.append(pan)
        assert invalid==[]

    def test_generate_different_pans(self):
        pans={
            TransactionDataGenerator._generate_pan("412345",total_length=16)
            for _ in range(20)

        }
        assert len(pans) >1




