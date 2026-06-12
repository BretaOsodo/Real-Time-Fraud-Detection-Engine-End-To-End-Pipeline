from flask_appbuilder.security.manager import AUTH_DB

SECRET_KEY = '3067771c76a86285b58c68cd67f393dec95a2daf4971310b218687abc71d502a'
AUTH_TYPE = AUTH_DB

# Superset metadata database - use your druid postgres credentials

# Druid configurations
DRUID_IS_ACTIVE = True
DRUID_HOST = 'broker'
DRUID_PORT = 8082
DRUID_ENDPOINT = '/druid/v2/sql'
DRUID_USER = ''
DRUID_PASSWORD = ''

FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DRUID_JOINS': True,
}



