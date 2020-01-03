import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import hashlib
import psycopg2
import ckan.lib.base as base




def caeser_try(arg1,arg2):
    s = arg1 + arg2
    m = hashlib.md5()
    m.update(s)
    ans = m.hexdigest()[-5:]
    temp_a = "username=" + arg1
    temp_b = "url=" + arg2
    temp_c = "check=" + ans
    ret = "http://140.110.141.163:8080/IPGOD?"+temp_a+"&"+temp_b+"&"+temp_c
    return ret

def caeser_ffff(arg1,arg_package,arg_id,arg_name):
   # temp_s = "/dataset"
   # temp_index = arg1.index(temp_s)
   # temp_index = temp_index + 9
   # final_s = arg1[temp_index:]
    return base.render_snippet('snippets/add_button.html',package_name = arg1,pack_id = arg_id,pack_r_id = arg_package,user_name = arg_name)


def caeser_ano_try(arg_id,arg_name):
    conn = psycopg2.connect(database = "ipgod",user="ckan_default",password="ckan_passwd",host="127.0.0.1",port="5432")
    #conn = psycopg2.connect(database = "testingdb",user="psqluser",password="postgres",host="127.0.0.1",port="5432")
    cur = conn.cursor()

    sql = "SELECT * FROM  extractor WHERE resourceid = '%s' and ckanuser = '%s' "%(arg_id,arg_name)
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    if not rows:
        return True
    else :
        return False



def caeser_PostGresSql(arg_name):
    #conn = psycopg2.connect(database = "testingdb",user="psqluser",password="postgres",host="127.0.0.1",port="5432")
    conn = psycopg2.connect(database = "ipgod",user="ckan_default",password="ckan_passwd",host="127.0.0.1",port="5432")
    cur = conn.cursor()

    sql = "SELECT * FROM  extractor WHERE ckanuser = '%s'"%(arg_name)
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()

    return rows

def caeser_check_if_add():
    return base.render_snippet('snippets/add_button.html')


class CaeserMethodPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.ITemplateHelpers)

    def update_config(self, config):
        toolkit.add_template_directory(config,'templates')

    def get_helpers(self):
        return {'caeser_try_func':caeser_try,'caeser_postsql':caeser_PostGresSql,'caeser_check_data':caeser_check_if_add,'caeser_ano_try':caeser_ano_try,'caeser_ff':caeser_ffff}
