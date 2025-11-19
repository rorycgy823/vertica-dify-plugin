from dify_plugin import DifyPluginEnv

from provider.vertica import VerticaProvider

# Initialize the plugin
plugin_env = DifyPluginEnv()

# Register the provider
plugin_env.register_provider(VerticaProvider)
