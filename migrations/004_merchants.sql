-- Dictionary gaps found by running real-shaped descriptors through the
-- normalizer and reading what it refused to resolve.
--
-- What is deliberately NOT added: a landlord, a person-to-person transfer, a
-- farmers market, a local pizza place. Those are the genuine long tail -- no
-- merchant dictionary contains them, and a demo where everything resolves
-- misrepresents the problem the normalizer exists to solve.

INSERT INTO merchants (id, display_name, category) VALUES
    ('mch_doordash',  'DoorDash',   'Food and Drink'),
    ('mch_instacart', 'Instacart',  'Groceries'),
    ('mch_costco',    'Costco',     'Groceries'),
    ('mch_lowes',     'Lowe''s',    'Shopping'),
    ('mch_bestbuy',   'Best Buy',   'Shopping')
ON CONFLICT (id) DO NOTHING;

INSERT INTO merchant_aliases (merchant_id, pattern, source, weight) VALUES
    -- 'STEAMGAMES.COM ...' cleans to 'steamgames', which scored only ~0.5
    -- against the existing 'steam' alias and fell below the threshold
    ('mch_steam',     'steamgames',      'learned', 1.0),
    ('mch_doordash',  'doordash',        'seed',    1.0),
    ('mch_doordash',  'dd doordash',     'seed',    1.0),
    ('mch_instacart', 'instacart',       'seed',    1.0),
    ('mch_costco',    'costco',          'seed',    1.0),
    ('mch_costco',    'costco whse',     'seed',    1.0),
    ('mch_lowes',     'lowes',           'seed',    1.0),
    ('mch_bestbuy',   'best buy',        'seed',    1.0),
    ('mch_bestbuy',   'bestbuy',         'seed',    1.0)
ON CONFLICT DO NOTHING;
