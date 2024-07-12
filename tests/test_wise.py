from decimal import Decimal
from pprint import pprint

from couch._banker import AccountType
from couch._banker import Bank
from couch._banker import Currency
from couch._banker import ProfileType
from couch._banker import create_wise_bank_accounts
from tests.dummy_data import WISE_ACCOUNTS
from tests.dummy_data import WISE_BALANCES
from tests.dummy_data import WISE_PROFILES


class DummyBank(Bank):
    def fetch_accounts(self): ...


def test_create_wise_bank_accounts_smoke():
    bank = DummyBank("api_key")

    create_wise_bank_accounts(bank, WISE_PROFILES, WISE_BALANCES, WISE_ACCOUNTS)


def test_create_wise_bank_accounts_minimal():
    bank = DummyBank("api_key")

    profiles = [
        {
            "type": "PERSONAL",
            "id": 1,
        }
    ]

    balances = {
        1: [
            {
                "id": 2,
                "currency": "USD",
                "amount": {"value": 1.23, "currency": "USD"},
                "type": "STANDARD",
                "name": "Account 2",
            },
        ]
    }

    accounts = {
        1: [
            {
                "id": 3,
                "profileId": 1,
                "recipientId": 4,
                "balances": [
                    {
                        "id": 2,
                        "balanceType": "AVAILABLE",
                        "currency": "USD",
                        "amount": {"value": 1.23, "currency": "USD"},
                        "bankDetails": {
                            "id": 5,
                            "currency": "USD",
                            "accountNumber": "6",
                            "bankName": "Wise US Inc",
                            "accountHolderName": "Tyler Durden",
                        },
                    }
                ],
            }
        ]
    }

    bank_accounts = create_wise_bank_accounts(bank, profiles, balances, accounts)

    assert len(bank_accounts) == 1
    bank_account = bank_accounts[0]
    pprint(bank_account)

    assert bank_account.id == 2
    assert bank_account.bank == bank
    assert bank_account.account_number == "6"
    assert bank_account.balance == Decimal("1.23")
    assert bank_account.currency == Currency.USD
    assert bank_account.account_type == AccountType.CHECKING
    assert bank_account.profile_type == ProfileType.PERSONAL
    assert bank_account.name == "Account 2"
