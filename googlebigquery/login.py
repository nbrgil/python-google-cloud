import gcs_oauth2_boto_plugin

# URI scheme for Cloud Storage.
GOOGLE_STORAGE = 'gs'
# URI scheme for accessing local files.
LOCAL_FILE = 'file'

# Fallback logic. In https://console.cloud.google.com/
# under Credentials, create a new client ID for an installed application.
# Required only if you have not configured client ID/secret in
# the .boto file or as environment variables.
CLIENT_ID = '64051886005-t8bej8ohetmlfbopee3tqib8q7sjg95i.apps.googleusercontent.com'
CLIENT_SECRET = 'G2n75Egc6u_fKiJwyXGZaw8Q'
gcs_oauth2_boto_plugin.SetFallbackClientIdAndSecret(CLIENT_ID, CLIENT_SECRET)