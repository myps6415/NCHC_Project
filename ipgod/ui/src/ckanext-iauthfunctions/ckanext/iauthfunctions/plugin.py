import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

def most_popular_groups():
    groups = toolkit.get_action('group_list')(
    data_dict={'sort':'package_count desc','all_fields':True})
    groups = groups[:10]
    return groups

class IauthfunctionsPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)
    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'example_theme')
            
    def get_helpers(self):
        return {'example_theme_most_popular_groups':most_popular_groups}