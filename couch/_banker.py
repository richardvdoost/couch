import uuid
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from enum import StrEnum
from enum import auto

import httpx

from couch._logging import get_logger

logger = get_logger(__name__)


class Currency(StrEnum):
    EUR = auto()
    USD = auto()


class ProfileType(StrEnum):
    PERSONAL = auto()
    BUSINESS = auto()


class AccountType(StrEnum):
    CHECKING = auto()
    SAVING = auto()


class Bank(ABC):
    api_url: str

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.accounts = []
        self.recipients = []

        self.fetch_accounts()
        self.fetch_recipients()

    @abstractmethod
    def fetch_accounts(self): ...

    def fetch_recipients(self): ...

    def __repr__(self) -> str:
        return type(self).__name__

    def find_account(self, **kwargs) -> "BankAccount":
        for account in self.accounts:
            if all(getattr(account, key) == val for key, val in kwargs.items()):
                return account

        raise Exception(f"Account not found - Properties: {kwargs}")

    def find_recipient(self, **kwargs) -> "Recipient":
        for recipient in self.recipients:
            if all(getattr(recipient, key) == val for key, val in kwargs.items()):
                return recipient

        raise Exception(f"Recipient not found - Properties: {kwargs}")


@dataclass
class BankAccount:
    id: str
    bank: Bank
    account_number: str | None
    balance: Decimal
    currency: Currency
    account_type: AccountType
    profile_type: ProfileType
    name: str | None = None
    context: dict = field(default_factory=dict)


@dataclass
class Recipient:
    id: str
    name: str
    bank_name: str
    account_number: str
    context: dict


class Mercury(Bank):
    api_url = "https://api.mercury.com/api/v1"

    def fetch_accounts(self):
        url = f"{self.api_url}/accounts"
        response = ensure_json(httpx.get(url, auth=(self.api_key, "")))

        self.accounts = [
            BankAccount(
                id=a.pop("id"),
                bank=self,
                account_number=a.pop("accountNumber"),
                balance=Decimal(f"{a.pop('availableBalance'):.02f}"),
                currency=Currency.USD,
                account_type=AccountType(a.pop("kind")),
                profile_type=ProfileType.BUSINESS,
                name=a.pop("nickname").partition("(")[0].strip(),
                context=a,
            )
            for a in response.get("accounts", [])
            if a["status"] != "archived"
        ]

    def fetch_recipients(self):
        url = f"{self.api_url}/recipients"
        response = ensure_json(httpx.get(url, auth=(self.api_key, "")))

        self.recipients = [
            Recipient(
                id=r.pop("id"),
                name=" - ".join(
                    [str(n) for n in [r.pop("name"), r.pop("nickname", None)] if n]
                ),
                account_number=r["electronicRoutingInfo"].pop("accountNumber", None),
                bank_name=r["electronicRoutingInfo"].pop("bankName"),
                context=r,
            )
            for r in response.get("recipients", [])
            if r.pop("status") == "active" and "electronicRoutingInfo" in r
        ]


class Wise(Bank):
    api_url = "https://api.transferwise.com"

    @property
    def headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def fetch_accounts(self):
        url = f"{self.api_url}/v2/profiles"
        profiles = ensure_json(httpx.get(url, headers=self.headers))

        balances = {
            p["id"]: ensure_json(
                httpx.get(
                    f"{self.api_url}/v4/profiles/{p['id']}/balances"
                    + "?types=STANDARD,SAVINGS",
                    headers=self.headers,
                )
            )
            for p in profiles
        }

        accounts = {
            p["id"]: ensure_json(
                httpx.get(
                    f"{self.api_url}/v1/borderless-accounts?profileId={p['id']}",
                    headers=self.headers,
                )
            )
            for p in profiles
        }

        self.accounts = create_wise_bank_accounts(self, profiles, balances, accounts)

    def create_quote(
        self,
        profile_id: str,
        amount: Decimal,
        source_currency: Currency,
        target_currency: Currency | None = None,
    ) -> str:
        url = f"{self.api_url}/v3/profiles/{profile_id}/quotes"
        payload = {
            "sourceAmount": str(amount),
            "sourceCurrency": source_currency.upper(),
            "targetCurrency": (target_currency or source_currency).upper(),
            "payOut": "BALANCE",
            "preferredPayIn": "BALANCE",
            "paymentMetadata": {"transferNature": "MOVING_MONEY_BETWEEN_OWN_ACCOUNTS"},
        }
        quote = ensure_json(httpx.post(url, headers=self.headers, json=payload))

        payment_options = quote["paymentOptions"]
        enabled_payment_options = [
            po for po in payment_options if po["disabled"] == False
        ]
        free_payment_options = [
            po for po in enabled_payment_options if po["fee"]["total"] < 0.005
        ]
        fee_rates = [po["feePercentage"] for po in enabled_payment_options]
        lowest_fee_rate = min(fee_rates)

        logger.debug(f"Quote has {len(payment_options)} total payment options")
        logger.debug(f"{len(enabled_payment_options)} are enabled")
        logger.debug(f"{len(free_payment_options)} are free")
        if not free_payment_options:
            logger.debug(f"Lowest fee rate: {lowest_fee_rate:.02%}")

        if target_currency is None:
            assert free_payment_options, "No free payment options available"

        return quote["id"]

    def create_transfer(
        self,
        source_recipient_id: str,
        target_recipient_id: str,
        quote_id: str,
        note: str | None = None,
    ) -> str:
        if note is None:
            transaction_id = str(uuid.uuid4())
        else:
            transaction_id = text_to_uuid(note)

        url = f"{self.api_url}/v1/transfers"
        transfer_payload = {
            "sourceAccount": source_recipient_id,
            "targetAccount": target_recipient_id,
            "quoteUuid": quote_id,
            "customerTransactionId": transaction_id,
            "details": {
                "reference": note,
                "transferPurpose": "verification.transfers.purpose.pay.bills",
                "transferPurposeSubTransferPurpose": (
                    "verification.sub.transfers.purpose.pay.interpretation.service"
                ),
                "sourceOfFunds": "verification.source.of.funds.other",
            },
        }
        transfer = ensure_json(
            httpx.post(url, headers=self.headers, json=transfer_payload)
        )

        return transfer["id"]

    def fund_transfer(self, profile_id: str, transfer_id: str):
        url = (
            f"{self.api_url}/v3/profiles/{profile_id}/transfers/{transfer_id}/payments"
        )
        payload = {"type": "BALANCE"}
        return ensure_json(httpx.post(url, headers=self.headers, json=payload))

    def move_balance(
        self,
        profile_id: str,
        quote_id: str,
        note: str | None = None,
    ):
        if note is None:
            idempotence_uuid = str(uuid.uuid4())
        else:
            idempotence_uuid = text_to_uuid(note)

        url = f"{self.api_url}/v2/profiles/{profile_id}/balance-movements"
        payload = {"quoteId": quote_id}
        headers = self.headers | {"X-idempotence-uuid": idempotence_uuid}
        return ensure_json(httpx.post(url, json=payload, headers=headers))


def create_wise_bank_accounts(
    bank: Bank,
    profiles: list[dict],
    balances: dict[int, list],
    accounts: dict[int, list],
) -> list[BankAccount]:
    map_balance_type_to_bank_account_type = {
        "STANDARD": AccountType.CHECKING,
        "SAVINGS": AccountType.SAVING,
    }

    bank_accounts = []

    for profile in profiles:
        profile_type = ProfileType(profile.pop("type").lower())

        for account in accounts[profile["id"]]:
            account_balances = account.pop("balances")
            balances_by_id = {b.pop("id"): b for b in account_balances}

            for balance in balances[profile["id"]]:
                balance_id = balance.pop("id")
                balance |= balances_by_id.get(balance_id, {})
                bank_details = balance.pop("bankDetails", {})
                bank_accounts.append(
                    BankAccount(
                        id=balance_id,
                        bank=bank,
                        account_number=bank_details.pop("accountNumber").replace(
                            " ", ""
                        )
                        if "accountNumber" in bank_details
                        else None,
                        balance=Decimal(f"{balance.pop('amount')['value']:.02f}"),
                        currency=Currency(balance.pop("currency").lower()),
                        account_type=map_balance_type_to_bank_account_type[
                            balance.pop("type")
                        ],
                        profile_type=profile_type,
                        name=balance.pop("name"),
                        context=dict(
                            profile=profile,
                            account=account,
                            balance=balance,
                            bank_details=bank_details,
                        ),
                    )
                )

    return bank_accounts


class TransferStrategy(ABC):
    @abstractmethod
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ): ...


class MercuryExternalTransfer(TransferStrategy):
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ):
        assert type(source.bank) is Mercury
        assert type(target.bank) is not Mercury

        logger.info("Executing Mercury external transfer")

        recipient = source.bank.find_recipient(account_number=target.account_number)

        if note is None:
            transaction_id = str(uuid.uuid4())
        else:
            transaction_id = text_to_uuid(note)
        logger.debug(f"Transaction ID: {transaction_id}")

        payload = {
            "recipientId": recipient.id,
            "amount": str(amount),
            "paymentMethod": "ach",
            "idempotencyKey": transaction_id,
            "note": note,
        }

        response = ensure_json(
            httpx.post(
                f"{source.bank.api_url}/account/{source.id}/transactions",
                json=payload,
                auth=(source.bank.api_key, ""),
            )
        )

        logger.debug(response)


class WiseInternalTransfer(TransferStrategy):
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ):
        assert type(source.bank) is Wise
        assert type(target.bank) is Wise

        logger.info("Executing Wise internal transfer")

        source_profile_id = source.context.get("profile", {}).get("id")
        if source_profile_id is None:
            raise Exception(
                f"Wise profile ID not found in source bank account: {source.context}"
            )

        target_profile_id = target.context.get("profile", {}).get("id")
        if target_profile_id is None:
            raise Exception(
                f"Wise profile ID not found in target bank account: {source.context}"
            )

        same_profiles = source_profile_id == target_profile_id
        same_currencies = source.currency == target.currency

        assert (
            same_profiles or same_currencies
        ), "Cannot convert currencies between different Wise profiles"

        source_recipient_id = source.context.get("account", {}).get("recipientId")
        if source_recipient_id is None:
            raise Exception(
                f"Wise recipient ID not found in source bank account: {source.context}"
            )
        target_recipient_id = target.context.get("account", {}).get("recipientId")

        if target_recipient_id is None:
            raise Exception(
                f"Wise recipient ID not found in target bank account: {source.context}"
            )

        quote_id = source.bank.create_quote(
            profile_id=source_profile_id,
            amount=amount,
            source_currency=source.currency,
            target_currency=target.currency,
        )

        logger.debug(f"Quote: {quote_id}")

        if same_profiles:
            logger.info("Moving balance within one profile")

            response = source.bank.move_balance(
                profile_id=source_profile_id,
                quote_id=quote_id,
                note=note,
            )
            logger.debug(f"Move balance response: {response}")

        if same_currencies and not same_profiles:
            logger.info("Creating transfer between profiles")

            transfer_id = source.bank.create_transfer(
                source_recipient_id=source_recipient_id,
                target_recipient_id=target_recipient_id,
                quote_id=quote_id,
                note=note,
            )

            response = source.bank.fund_transfer(
                profile_id=source_profile_id, transfer_id=transfer_id
            )

            logger.debug(f"Fund response: {response}")


class Banker:
    def __init__(self):
        self.full_strategies = {
            (Wise, Wise): WiseInternalTransfer(),
        }
        self.from_strategies = {
            Mercury: MercuryExternalTransfer(),
        }

    def transfer(
        self,
        source: BankAccount,
        target: BankAccount,
        amount: Decimal,
        note: str | None = None,
    ):
        strategy = self.full_strategies.get(
            (type(source.bank), type(target.bank)),  # type: ignore
        ) or self.from_strategies.get(type(source.bank))  # type: ignore

        if not strategy:
            raise NotImplementedError(
                "No transfer strategy for the given bank account combination"
            )

        strategy.handle(source, target, amount, note)


def text_to_uuid(text: str) -> str:
    namespace_uuid = uuid.NAMESPACE_DNS
    name_uuid = uuid.uuid5(namespace_uuid, text)

    uuid_str = str(name_uuid)

    return uuid_str[:14] + "4" + uuid_str[15:]


def ensure_json(response: httpx.Response):
    if response.status_code not in {200, 201}:
        raise Exception(f"HTTPX Error: {response.status_code} {response.json()}")

    return response.json()
