"""Finish initial internal-service setup; does not change end-user permissions."""
from bootstrap import sql

sql("INSERT INTO api.export_delegates(token_id) SELECT t.id FROM api.tokens t "
    "JOIN api.users u ON u.id=t.user_id WHERE u.email='demo-service@nts.mgimo.ru' "
    "AND t.revoked_at IS NULL ON CONFLICT DO NOTHING;", 'tradeapi')
print('Internal demo service export delegation enabled.')
