"""
Migration: update HZ_CUST_ACCOUNTS and HZ_PARTIES catalog descriptions
to include explicit join hints so the LLM knows how to link AR tables to customer names.

Run on VM:
  cd /path/to/server
  python -m migrations.update_hz_catalog_descriptions
"""
from __future__ import annotations

import asyncio
from sqlalchemy import text
from app.core.database import async_session


UPDATES = [
    # ── Oracle HZ (Trading Community Architecture) ───────────────────────────
    {
        "pattern": "HZ_CUST_ACCOUNTS",
        "ai_description": (
            "Oracle customer account records. Bridge table linking AR transaction tables to customer names. "
            "Join path: AR_PAYMENT_SCHEDULES_ALL.CUSTOMER_ID = HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID, "
            "then HZ_CUST_ACCOUNTS.PARTY_ID = HZ_PARTIES.PARTY_ID to get PARTY_NAME. "
            "Contains credit limits, account status, and account classification."
        ),
        "good_for": ["customer name lookup", "AR to customer join", "credit limit analysis", "account status analysis"],
    },
    {
        "pattern": "HZ_PARTIES",
        "ai_description": (
            "Oracle party master — the source of customer names (PARTY_NAME) and addresses. "
            "Always accessed via HZ_CUST_ACCOUNTS bridge: "
            "AR tables → HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID → HZ_CUST_ACCOUNTS.PARTY_ID → HZ_PARTIES.PARTY_ID → PARTY_NAME."
        ),
        "good_for": ["customer name resolution", "party name lookup", "customer address", "customer demographics"],
    },
    {
        "pattern": "HZ_LOCATIONS",
        "ai_description": (
            "Oracle location/address records. "
            "Join path: HZ_CUST_SITE_USES_ALL → HZ_CUST_ACCT_SITES_ALL → HZ_PARTY_SITES → HZ_LOCATIONS. "
            "Contains ADDRESS1, CITY, STATE, POSTAL_CODE, COUNTRY."
        ),
        "good_for": ["customer address lookup", "billing address", "shipping address", "location analysis"],
    },
    {
        "pattern": "HZ_CUST_SITE_USES_ALL",
        "ai_description": (
            "Oracle customer site uses — defines whether a site is BILL_TO or SHIP_TO. "
            "Key column: SITE_USE_CODE ('BILL_TO', 'SHIP_TO'). "
            "Join: CUSTOMER_SITE_USE_ID in AR tables = HZ_CUST_SITE_USES_ALL.SITE_USE_ID."
        ),
        "good_for": ["billing site", "shipping site", "site use analysis", "customer location"],
    },
    # ── Oracle AR (Accounts Receivable) ──────────────────────────────────────
    {
        "pattern": "AR_PAYMENT_SCHEDULES_ALL",
        "ai_description": (
            "Oracle AR payment schedules — the primary table for invoice ageing and outstanding balances. "
            "One row per invoice instalment. KEY COLUMNS: CUSTOMER_ID (FK to HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID), "
            "TRX_NUMBER (invoice number), DUE_DATE, AMOUNT_DUE_ORIGINAL, AMOUNT_DUE_REMAINING, STATUS ('OP'=open, 'CL'=closed). "
            "Filter STATUS='OP' for outstanding invoices. Use DUE_DATE for ageing buckets."
        ),
        "good_for": ["invoice ageing", "outstanding invoices", "overdue analysis", "payment tracking", "AR balance"],
    },
    {
        "pattern": "AR_CASH_RECEIPTS_ALL",
        "ai_description": (
            "Oracle AR cash receipts — records of payments received from customers. "
            "KEY COLUMNS: PAY_FROM_CUSTOMER (FK to HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID), "
            "RECEIPT_DATE, AMOUNT, CURRENCY_CODE, STATUS ('APP'=applied, 'UNAPP'=unapplied, 'REV'=reversed). "
            "Join to HZ_CUST_ACCOUNTS via PAY_FROM_CUSTOMER = CUST_ACCOUNT_ID."
        ),
        "good_for": ["cash receipts", "payment received", "cash flow", "payment history", "receipt analysis"],
    },
    {
        "pattern": "AR_RECEIVABLE_APPLICATIONS_ALL",
        "ai_description": (
            "Oracle AR receivable applications — records how receipts are applied against invoices. "
            "Links AR_CASH_RECEIPTS_ALL to AR_PAYMENT_SCHEDULES_ALL. "
            "KEY COLUMNS: CASH_RECEIPT_ID (FK to AR_CASH_RECEIPTS_ALL), "
            "PAYMENT_SCHEDULE_ID (FK to AR_PAYMENT_SCHEDULES_ALL), AMOUNT_APPLIED, STATUS, APPLICATION_TYPE."
        ),
        "good_for": ["receipt application", "payment matching", "applied vs unapplied", "AR reconciliation"],
    },
    {
        "pattern": "RA_CUSTOMER_TRX_ALL",
        "ai_description": (
            "Oracle AR customer transactions — invoice header records. "
            "Key columns: CUSTOMER_TRX_ID (PK, FK from AR_PAYMENT_SCHEDULES_ALL), "
            "CUSTOMER_ID (FK to HZ_CUST_ACCOUNTS.CUST_ACCOUNT_ID), "
            "TRX_DATE, TRX_NUMBER, INVOICE_CURRENCY_CODE, BILL_TO_CUSTOMER_ID."
        ),
        "good_for": ["invoice header", "transaction analysis", "customer billing", "invoice list"],
    },
    {
        "pattern": "RA_CUSTOMER_TRX_LINES_ALL",
        "ai_description": (
            "Oracle AR transaction line items — one row per invoice line. "
            "FK: CUSTOMER_TRX_ID = RA_CUSTOMER_TRX_ALL.CUSTOMER_TRX_ID. "
            "Key columns: LINE_TYPE, EXTENDED_AMOUNT, QUANTITY_INVOICED, UNIT_SELLING_PRICE, DESCRIPTION."
        ),
        "good_for": ["invoice line items", "line detail", "revenue by line", "quantity analysis"],
    },
    {
        "pattern": "RA_CUST_TRX_LINE_GL_DIST_ALL",
        "ai_description": (
            "Oracle AR GL distributions for transaction lines — accounting entries. "
            "FK: CUSTOMER_TRX_LINE_ID = RA_CUSTOMER_TRX_LINES_ALL.CUSTOMER_TRX_LINE_ID. "
            "Key columns: AMOUNT, ACCOUNT_CLASS ('REC'=receivable, 'REV'=revenue, 'TAX'), CODE_COMBINATION_ID."
        ),
        "good_for": ["GL accounting", "revenue recognition", "AR accounting entries", "journal lines"],
    },
]


async def run() -> None:
    async with async_session() as session:
        for upd in UPDATES:
            result = await session.execute(
                text(
                    "UPDATE file_metadata "
                    "SET ai_description = :desc, "
                    "    good_for = :good_for, "
                    "    updated_at = NOW() "
                    "WHERE blob_path ILIKE :pattern "
                    "RETURNING blob_path"
                ),
                {
                    "desc": upd["ai_description"],
                    "good_for": upd["good_for"],
                    "pattern": f"%{upd['pattern']}%",
                },
            )
            rows = result.fetchall()
            if rows:
                for row in rows:
                    print(f"  Updated: {row[0]}")
            else:
                print(f"  No match for pattern: {upd['pattern']}")

        await session.commit()
        print("Done.")


if __name__ == "__main__":
    asyncio.run(run())
