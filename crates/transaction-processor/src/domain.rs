//! Domain model.
//!
//! The entities the system reasons about — transactions arriving from a
//! source, the per-client account state they mutate, and the soft-failure
//! vocabulary that classifies non-fatal outcomes. Independent of any port
//! or processor wiring.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Soft failures the processor can encounter and continue from. These are
/// *expected* business outcomes — malformed input, disputes on non-deposit
/// transactions, etc.
/// TODO: Naming for ProcessorSoftFailures can probably just be SoftFailures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorSoftFailures {
    /// Monetary tx_id has already been ingested in this run.
    AlreadyProcessed,
    /// Deposit/Withdrawal arrived with a negative amount.
    NegativeAmount,
    /// Withdrawal amount exceeded available funds.
    InsufficientFunds,
    /// Withdrawal targeted a locked account.
    AccountLocked,
    /// Lifecycle event (Dispute/Resolve/Chargeback) referenced a tx_id that
    /// is not currently in the active or disputed map.
    DisputedTransactionNotFound,
    /// Dispute referenced a non-deposit transaction.
    DisputeOnNonDeposit,
    /// Lifecycle event referenced a tx_id whose owner is a different client.
    ClientIdMismatch,
}

impl ProcessorSoftFailures {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AlreadyProcessed => "already_processed",
            Self::NegativeAmount => "negative_amount",
            Self::InsufficientFunds => "insufficient_funds",
            Self::AccountLocked => "account_locked",
            Self::DisputedTransactionNotFound => "disputed_transaction_not_found",
            Self::DisputeOnNonDeposit => "dispute_on_non_deposit",
            Self::ClientIdMismatch => "client_id_mismatch",
        }
    }
}

/// Type of tx coming in from the source.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Internal transaction representation used for routing and processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub tx_type: TxType,
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: Decimal,
}

/// Client account state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    pub client_id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

impl Account {
    /// Apply a deposit. Soft-fails on a negative amount. Locked accounts may
    /// still receive deposits (they're how a locked account regains funds for
    /// downstream chargeback resolution).
    pub fn apply_deposit(self, amount: Decimal) -> Result<Self, ProcessorSoftFailures> {
        if amount < Decimal::ZERO {
            return Err(ProcessorSoftFailures::NegativeAmount);
        }
        Ok(Self {
            available: self.available + amount,
            total: self.total + amount,
            ..self
        })
    }

    /// Apply a withdrawal. Soft-fails on negative amount, locked account, or
    /// insufficient available funds.
    pub fn apply_withdrawal(self, amount: Decimal) -> Result<Self, ProcessorSoftFailures> {
        if amount < Decimal::ZERO {
            return Err(ProcessorSoftFailures::NegativeAmount);
        }
        if self.locked {
            return Err(ProcessorSoftFailures::AccountLocked);
        }
        if self.available < amount {
            return Err(ProcessorSoftFailures::InsufficientFunds);
        }
        Ok(Self {
            available: self.available - amount,
            total: self.total - amount,
            ..self
        })
    }

    /// Move the disputed amount from available → held; total unchanged. The
    /// caller is responsible for verifying the dispute is well-formed (the
    /// disputed tx exists, belongs to this client, and is a deposit) — those
    /// checks live in the orchestration layer because they require storage
    /// lookups, not pure account arithmetic.
    pub fn apply_dispute(self, disputed_amount: Decimal) -> Self {
        Self {
            available: self.available - disputed_amount,
            held: self.held + disputed_amount,
            ..self
        }
    }

    /// Release held funds back to available; total unchanged.
    pub fn apply_resolve(self, disputed_amount: Decimal) -> Self {
        Self {
            available: self.available + disputed_amount,
            held: self.held - disputed_amount,
            ..self
        }
    }

    /// Remove held funds from total and lock the account. Idempotent on the
    /// `locked` field — applying chargeback to an already-locked account
    /// keeps it locked.
    pub fn apply_chargeback(self, disputed_amount: Decimal) -> Self {
        Self {
            held: self.held - disputed_amount,
            total: self.total - disputed_amount,
            locked: true,
            ..self
        }
    }
}

#[cfg(test)]
mod account_tests {
    use super::*;

    fn d(v: f64) -> Decimal {
        Decimal::try_from(v).unwrap()
    }

    fn account(available: f64, held: f64, total: f64, locked: bool) -> Account {
        Account {
            client_id: 1,
            available: d(available),
            held: d(held),
            total: d(total),
            locked,
        }
    }

    // ---- apply_deposit ----

    #[test]
    fn apply_deposit_increases_available_and_total() {
        let updated = account(10.0, 0.0, 10.0, false)
            .apply_deposit(d(5.0))
            .unwrap();
        assert_eq!(updated.available, d(15.0));
        assert_eq!(updated.total, d(15.0));
        assert_eq!(updated.held, Decimal::ZERO);
        assert!(!updated.locked);
    }

    #[test]
    fn apply_deposit_rejects_negative_amount() {
        let err = account(10.0, 0.0, 10.0, false)
            .apply_deposit(d(-5.0))
            .unwrap_err();
        assert_eq!(err, ProcessorSoftFailures::NegativeAmount);
    }

    #[test]
    fn apply_deposit_works_on_locked_account() {
        let updated = account(10.0, 0.0, 10.0, true)
            .apply_deposit(d(5.0))
            .unwrap();
        assert_eq!(updated.available, d(15.0));
        assert_eq!(updated.total, d(15.0));
        assert!(updated.locked, "deposit must not unlock the account");
    }

    // ---- apply_withdrawal ----

    #[test]
    fn apply_withdrawal_decreases_available_and_total() {
        let updated = account(10.0, 0.0, 10.0, false)
            .apply_withdrawal(d(4.0))
            .unwrap();
        assert_eq!(updated.available, d(6.0));
        assert_eq!(updated.total, d(6.0));
        assert_eq!(updated.held, Decimal::ZERO);
    }

    #[test]
    fn apply_withdrawal_rejects_negative_amount() {
        let err = account(10.0, 0.0, 10.0, false)
            .apply_withdrawal(d(-5.0))
            .unwrap_err();
        assert_eq!(err, ProcessorSoftFailures::NegativeAmount);
    }

    #[test]
    fn apply_withdrawal_rejects_when_locked() {
        let err = account(100.0, 0.0, 100.0, true)
            .apply_withdrawal(d(10.0))
            .unwrap_err();
        assert_eq!(err, ProcessorSoftFailures::AccountLocked);
    }

    #[test]
    fn apply_withdrawal_rejects_when_insufficient_funds() {
        let err = account(5.0, 0.0, 5.0, false)
            .apply_withdrawal(d(10.0))
            .unwrap_err();
        assert_eq!(err, ProcessorSoftFailures::InsufficientFunds);
    }

    // ---- apply_dispute / resolve / chargeback ----

    #[test]
    fn apply_dispute_moves_funds_from_available_to_held() {
        let updated = account(100.0, 0.0, 100.0, false).apply_dispute(d(40.0));
        assert_eq!(updated.available, d(60.0));
        assert_eq!(updated.held, d(40.0));
        assert_eq!(updated.total, d(100.0));
        assert!(!updated.locked);
    }

    #[test]
    fn apply_resolve_releases_held_back_to_available() {
        let updated = account(60.0, 40.0, 100.0, false).apply_resolve(d(40.0));
        assert_eq!(updated.available, d(100.0));
        assert_eq!(updated.held, Decimal::ZERO);
        assert_eq!(updated.total, d(100.0));
    }

    #[test]
    fn apply_chargeback_removes_held_and_locks_account() {
        let updated = account(60.0, 40.0, 100.0, false).apply_chargeback(d(40.0));
        assert_eq!(updated.available, d(60.0));
        assert_eq!(updated.held, Decimal::ZERO);
        assert_eq!(updated.total, d(60.0));
        assert!(updated.locked);
    }

    #[test]
    fn apply_chargeback_keeps_already_locked_account_locked() {
        let updated = account(60.0, 40.0, 100.0, true).apply_chargeback(d(40.0));
        assert!(updated.locked);
    }
}
