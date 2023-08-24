import toml

betfair_configuration = toml.load('betfair_config.toml')

smarkets_configuration = toml.load('smarkets_config.toml')

sport_event_mapping = toml.load('sport_identifier_mapping.toml')

sports_tables_mapping = toml.load('sports_tables_mapping.toml')

strategy_cash_out_limits = toml.load('strategy_cash_out_limits.toml')
