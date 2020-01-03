import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import psycopg2

# Required so that GET requests workr
@toolkit.side_effect_free
def get_news(context,data_dict=None):
  # The actual custom API method
  return {"hello":"world"}

@toolkit.side_effect_free
def get_selection(context,data_dict=None):
  # The actual custom API method
  conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % ('ipgod', 'thomas', '127.0.0.1', 'nchcnchc'))
  cur = conn.cursor()
  pkgs = []
  cur.execute("SELECT package_name, resourceid from extractor where status=0 and skip=FALSE")
  rows = cur.fetchall()
  for row in rows:
      pkg = row[0].rstrip()
      rid = row[1].rstrip()
      pkg={"package":pkg, "resource":rid}
      pkgs.append(pkg)
  #return {"user":context['user'], "package":pkgs}
  return {"list":pkgs}


class IpgodPlugin(plugins.SingletonPlugin):
  plugins.implements(plugins.interfaces.IActions)

  def get_actions(self):
    # Registers the custom API method defined above
    return {'get_news': get_news, 'get_selection':get_selection}
