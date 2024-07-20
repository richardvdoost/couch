import uuid
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from enum import StrEnum
from enum import auto
from pprint import pformat

import httpx

from couch._logging import get_logger

logger = get_logger(__name__)


class Currency(StrEnum):
    EUR = auto()
    USD = auto()


def get_currency(iso_code: str) -> Currency | None:
    try:
        return Currency(iso_code.lower())
    except ValueError:
        return None


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
        self.rates = {}

        self.fetch_accounts()
        self.fetch_recipients()

    @abstractmethod
    def fetch_accounts(self): ...

    def fetch_recipients(self): ...

    def __repr__(self) -> str:
        return type(self).__name__

    def find_account(self, **kwargs) -> "BankAccount":
        account_matches = [
            account
            for account in self.accounts
            if all(getattr(account, key) == val for key, val in kwargs.items())
        ]

        if len(account_matches) == 0:
            raise Exception(f"Account not found - Properties: {kwargs}")

        if len(account_matches) > 1:
            raise Exception(f"Multiple accounts found - Properties: {kwargs}")

        return account_matches[0]

    def find_recipient(self, **kwargs) -> "Recipient":
        for recipient in self.recipients:
            if all(getattr(recipient, key) == val for key, val in kwargs.items()):
                return recipient

        raise Exception(f"Recipient not found - Properties: {kwargs}")

    def get_conversion_rate(self, source: Currency, target: Currency) -> Decimal:
        assert type(source) is type(target) is Currency
        raise NotImplementedError(f"Conversion rate not available for {type(self)}")


@dataclass
class BankAccount:
    id: str
    bank: Bank | str
    account_number: str | None
    currency: Currency
    account_type: AccountType
    profile_type: ProfileType
    balance: Decimal | None = None
    name: str | None = None
    context: dict = field(default_factory=dict)

    def balance_in_currency(self, currency: Currency) -> Decimal:
        if self.balance is None:
            raise Exception("Balance not available")

        if not isinstance(self.bank, Bank):
            raise NotImplementedError(f"Conversion rate not available for {self.bank}")

        return self.balance * self.bank.get_conversion_rate(self.currency, currency)


@dataclass
class Recipient:
    id: str
    name: str
    bank_name: str
    account_number: str
    context: dict


@dataclass
class Transaction:
    id: str
    source_amount: Decimal
    source_currency: Currency
    target_amount: Decimal
    target_currency: Currency
    fee_amount: Decimal
    fee_currency: Currency


class Mercury(Bank):
    api_url = "https://api.mercury.com/api/v1"

    def fetch_accounts(self):
        url = f"{self.api_url}/accounts"
        response = ensure_success(httpx.get(url, auth=(self.api_key, ""))).json()

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
        response = ensure_success(httpx.get(url, auth=(self.api_key, ""))).json()

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
        profiles = ensure_success(httpx.get(url, headers=self.headers)).json()

        balances = {
            p["id"]: ensure_success(
                httpx.get(
                    f"{self.api_url}/v4/profiles/{p['id']}/balances"
                    + "?types=STANDARD,SAVINGS",
                    headers=self.headers,
                )
            ).json()
            for p in profiles
        }
        assert type(balances) is dict

        accounts = {
            p["id"]: ensure_success(
                httpx.get(
                    f"{self.api_url}/v1/borderless-accounts?profileId={p['id']}",
                    headers=self.headers,
                )
            ).json()
            for p in profiles
        }
        assert type(accounts) is dict

        self.accounts = create_wise_bank_accounts(self, profiles, balances, accounts)

    def fetch_recipients(self):
        url = f"{self.api_url}/v2/profiles"
        profiles = ensure_success(httpx.get(url, headers=self.headers)).json()

        accounts = [
            a
            for p in profiles
            for a in ensure_success(
                httpx.get(
                    f"{self.api_url}/v2/accounts?profileId={p['id']}",
                    headers=self.headers,
                )
            ).json()["content"]
        ]

        self.recipients = [
            Recipient(
                id=a.pop("id"),
                name=a.pop("name", {}).get("fullName", ""),
                bank_name=a.get("details", {}).get(
                    a.get("commonFieldMap", {}).get("bankCodeField"), ""
                ),
                account_number=a.pop("details", {}).get(
                    a.pop("commonFieldMap", {}).get("accountNumberField"), ""
                ),
                context=a,
            )
            for a in accounts
        ]

    def create_balance_quote(
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
        quote = ensure_success(
            httpx.post(url, headers=self.headers, json=payload)
        ).json()

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

        logger.debug(f"Quote: {quote['id']}")

        return quote["id"]

    def create_bank_quote(
        self,
        profile_id: str,
        target_recipient_id: str,
        amount: Decimal,
        source_currency: Currency,
        target_currency: Currency | None = None,
    ) -> str:
        url = f"{self.api_url}/v3/profiles/{profile_id}/quotes"
        payload = {
            "sourceAmount": str(amount),
            "sourceCurrency": source_currency.upper(),
            "targetCurrency": (target_currency or source_currency).upper(),
            "targetAccount": target_recipient_id,
            "paymentMetadata": {"transferNature": "MOVING_MONEY_BETWEEN_OWN_ACCOUNTS"},
        }
        quote = ensure_success(
            httpx.post(url, headers=self.headers, json=payload)
        ).json()

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

        logger.debug(f"Quote: {quote['id']}")

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
        transfer = ensure_success(
            httpx.post(url, headers=self.headers, json=transfer_payload)
        ).json()

        return transfer["id"]

    def fund_transfer(self, profile_id: str, transfer_id: str) -> Transaction | None:
        url = (
            f"{self.api_url}/v3/profiles/{profile_id}/transfers/{transfer_id}/payments"
        )
        payload = {"type": "BALANCE"}

        response = ensure_success(
            httpx.post(url, headers=self.headers, json=payload),
            allowed_status_codes={200, 201, 409},
        ).json()

        return wise_response_to_transaction(response)

    def move_balance(
        self,
        profile_id: str,
        source_balance_id: str,
        target_balance_id: str,
        amount: Decimal,
        currency: Currency,
        note: str | None = None,
    ) -> Transaction | None:
        idempotence_uuid = str(uuid.uuid4()) if note is None else text_to_uuid(note)

        url = f"{self.api_url}/v2/profiles/{profile_id}/balance-movements"
        payload = dict(
            amount=dict(value=str(amount), currency=currency.upper()),
            sourceBalanceId=source_balance_id,
            targetBalanceId=target_balance_id,
        )
        headers = self.headers | {"X-idempotence-uuid": idempotence_uuid}

        response = ensure_success(httpx.post(url, json=payload, headers=headers)).json()

        return wise_response_to_transaction(response)

    def convert_balance(
        self,
        profile_id: str,
        quote_id: str,
        note: str | None = None,
    ):
        idempotence_uuid = str(uuid.uuid4()) if note is None else text_to_uuid(note)

        url = f"{self.api_url}/v2/profiles/{profile_id}/balance-movements"
        payload = {"quoteId": quote_id}
        headers = self.headers | {"X-idempotence-uuid": idempotence_uuid}

        response = ensure_success(httpx.post(url, json=payload, headers=headers)).json()

        return wise_response_to_transaction(response)

    def get_conversion_rate(self, source: Currency, target: Currency) -> Decimal:
        if source == target:
            return Decimal("1")

        if (source, target) in self.rates:
            logger.debug(f"Using cached rate for {source.upper()}/{target.upper()}")
            return self.rates[(source, target)]

        logger.debug(f"Fetching rate for {source.upper()}/{target.upper()}")
        url = f"{self.api_url}/v1/rates?source={source.upper()}&target={target.upper()}"
        rates = ensure_success(httpx.get(url, headers=self.headers)).json()

        if not rates:
            raise Exception(f"No rates found for {source.upper()}/{target.upper()}")

        rate = Decimal(str(rates[0]["rate"]))

        self.rates[(source, target)] = rate

        return rate


def wise_response_to_transaction(response: dict) -> Transaction | None:
    if response["status"] == "REJECTED":
        return None

    logger.debug(f"Response: {pformat(response)}")

    source = response["steps"]["sourceAmount"]
    target = response["steps"]["targetAmount"]

    source_amount = Decimal(source["value"])
    source_currency = get_currency(source["currency"])

    target_amount = Decimal(target["value"])
    target_currency = target["currency"]

    fee_amount = Decimal(response["fee"]["value"])
    fee_currency = get_currency(response["fee"]["currency"])

    assert (
        source_currency is not None
        and target_currency is not None
        and fee_currency is not None
    )

    return Transaction(
        id=response["id"],
        source_amount=source_amount,
        source_currency=source_currency,
        target_amount=target_amount,
        target_currency=target_currency,
        fee_amount=fee_amount,
        fee_currency=fee_currency,
    )


def create_wise_bank_accounts(
    bank: Bank, profiles: list, balances: dict, accounts: dict
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
                currency = get_currency(balance.pop("currency"))
                if currency is None:
                    continue

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
                        currency=currency,
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
    ) -> Transaction | None: ...


class MercuryExternalTransfer(TransferStrategy):
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ) -> Transaction:
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

        response = ensure_success(
            httpx.post(
                f"{source.bank.api_url}/account/{source.id}/transactions",
                json=payload,
                auth=(source.bank.api_key, ""),
            ),
            allowed_status_codes={200, 201, 409},
        ).json()

        logger.debug(response)

        return Transaction(
            id=response["id"],
            source_amount=Decimal(response["amount"]),
            source_currency=source.currency,
            target_amount=Decimal(response["amount"]),
            target_currency=target.currency,
            fee_amount=Decimal("0"),
            fee_currency=source.currency,
        )


class WiseInternalTransfer(TransferStrategy):
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ) -> Transaction | None:
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

        need_quote = not same_profiles or not same_currencies
        quote_id = (
            source.bank.create_balance_quote(
                profile_id=source_profile_id,
                amount=amount,
                source_currency=source.currency,
                target_currency=target.currency,
            )
            if need_quote
            else None
        )

        if same_profiles:
            if same_currencies:
                logger.info("Moving balance within one profile")
                transaction = source.bank.move_balance(
                    profile_id=source_profile_id,
                    source_balance_id=source.id,
                    target_balance_id=target.id,
                    amount=amount,
                    currency=target.currency,
                    note=note,
                )
                logger.debug(f"Move balance transaction: {transaction}")

                return transaction

            assert quote_id is not None

            logger.info("Converting balance within one profile")
            transaction = source.bank.convert_balance(
                profile_id=source_profile_id,
                quote_id=quote_id,
                note=note,
            )
            logger.debug(f"Convert balance transaction: {transaction}")

            return transaction

        if same_currencies and not same_profiles:
            assert quote_id is not None

            logger.info("Creating transfer between profiles")

            transfer_id = source.bank.create_transfer(
                source_recipient_id=source_recipient_id,
                target_recipient_id=target_recipient_id,
                quote_id=quote_id,
                note=note,
            )

            transaction = source.bank.fund_transfer(
                profile_id=source_profile_id, transfer_id=transfer_id
            )

            logger.debug(f"Fund transaction: {transaction}")

            return transaction


class WiseExternalTransfer(TransferStrategy):
    def handle(
        self,
        source: "BankAccount",
        target: "BankAccount",
        amount: Decimal,
        note: str | None = None,
    ) -> Transaction | None:
        # Probably not possible due to some SCA restrictions

        assert type(source.bank) is Wise
        assert type(target.bank) is not Wise

        logger.info("Executing Wise external transfer")

        target_recipient = source.bank.find_recipient(
            account_number=target.account_number
        )

        logger.debug(f"Recipient: {target_recipient}")

        if note is None:
            transaction_id = str(uuid.uuid4())
        else:
            transaction_id = text_to_uuid(note)
        logger.debug(f"Transaction ID: {transaction_id}")

        source_profile_id = source.context.get("profile", {}).get("id")
        quote_id = source.bank.create_bank_quote(
            profile_id=source_profile_id,
            target_recipient_id=target_recipient.id,
            amount=amount,
            source_currency=source.currency,
        )

        logger.debug(f"Quote ID: {quote_id}")

        source_recipient_id = source.context.get("account", {}).get("recipientId")
        transfer_id = source.bank.create_transfer(
            source_recipient_id=source_recipient_id,
            target_recipient_id=target_recipient.id,
            quote_id=quote_id,
            note=note,
        )

        logger.debug(f"Transfer ID: {transfer_id}")

        transaction = source.bank.fund_transfer(
            profile_id=source_profile_id, transfer_id=transfer_id
        )

        logger.debug(f"Fund transaction: {transaction}")

        return transaction


class Banker:
    def __init__(self):
        self.full_strategies = {
            (Wise, Wise): WiseInternalTransfer(),
        }
        self.from_strategies = {
            Mercury: MercuryExternalTransfer(),
            Wise: WiseExternalTransfer(),
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

        amount_quantized = amount.quantize(Decimal("0.01"))

        strategy.handle(source, target, amount_quantized, note)


def text_to_uuid(text: str) -> str:
    namespace_uuid = uuid.NAMESPACE_DNS
    name_uuid = uuid.uuid5(namespace_uuid, text)

    uuid_str = str(name_uuid)

    return uuid_str[:14] + "4" + uuid_str[15:]


def ensure_success(
    response: httpx.Response, allowed_status_codes: set[int] = {200, 201}
) -> httpx.Response:
    if response.status_code not in allowed_status_codes:
        raise Exception(f"HTTPX Error: {response.status_code} {response.json()}")

    return response
