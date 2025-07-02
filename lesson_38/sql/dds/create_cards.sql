CREATE TABLE IF NOT EXISTS dds.cards (
    card_number TEXT PRIMARY KEY,
    user_id TEXT REFERENCES dds.users(user_id)
);