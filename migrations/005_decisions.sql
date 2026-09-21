-- Blocking risk checks.
--
-- Until now every rule was advisory: the risk worker ran AFTER the money
-- moved and could only flag. That is the right default -- declining someone's
-- groceries because a counter looked odd is a worse outcome than a flag a
-- human reviews -- but it is not the whole truth about payments. Real systems
-- decline, and the interesting engineering question is not whether to block,
-- it is WHERE the check runs:
--
--   advisory  -> asynchronous, after commit, cannot affect the posting
--   blocking  -> synchronous, before commit, and the posting never happens
--
-- A declined attempt still has to be recorded. It is evidence, it is what a
-- customer will call about, and it is the only trace that anything occurred --
-- the ledger correctly has no entry for money that never moved.

ALTER TABLE fraud_signals
    ADD COLUMN action TEXT NOT NULL DEFAULT 'flag'
        CHECK (action IN ('flag', 'block')),
    -- the attempted amount, which for a blocked attempt exists nowhere else
    ADD COLUMN attempted_amount_minor BIGINT,
    ADD COLUMN reason TEXT;

CREATE INDEX fraud_signals_blocked_idx ON fraud_signals(tenant_id, evaluated_at DESC)
    WHERE action = 'block';

-- transaction_id is already nullable, which is what a decline needs: there is
-- no transaction to point at, and inventing one would put a posting in the
-- ledger for money that did not move.
COMMENT ON COLUMN fraud_signals.transaction_id IS
    'NULL for a blocked attempt: no posting was created, by design.';
