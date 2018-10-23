from squaad import db
from squaad import file

"""
For the config file and format please look the repository for an example
{"pgsql":{"host":"","user":"","passwd":"","db":""} }
Support only for postgresql. Database must have the tables and fields of
SQUAAD Database

@config_file path to config file that contains the connection information
@cache_folder path to the cache folder where to save the results
"""
myConnection=db("config.json","cache")


print("Connection Status: %s"%myConnection.testConnection())



"""
    Gets the quality metrics of a developer at the application level
     email,
     app,
     loc,
     cpx,
     sml,
     vul,
     fbg,
     locs_inc,
     cplxs_inc,
     smls_inc,
     vuls_inc,
     fbgs_inc,
     locs_dec,
     cplxs_dec,
     smls_dec,
     vuls_dec,
     fbgs_dec,
     total

"""
myConnection.getQuality()


"""
Gets the compliation results for a developer at the application level
 email,
 app,
 domain,
 organization,
 c_impactful,
 c_analyzed

"""
myConnection.getCompilation()


"""
    Gets the quality metrics AND compilation of a developer at the application level
     email,
     app,
     loc,
     cpx,
     sml,
     vul,
     fbg,
     locs_inc,
     cplxs_inc,
     smls_inc,
     vuls_ingetQualityCompilationc,
     fbgs_inc,
     locs_dec,
     cplxs_dec,
     smls_dec,
     vuls_dec,
     fbgs_dec,
     total,
     impactful,
     analyzed
"""
myConnection.getQualityCompilation()


"""
        locs.email         AS email,
        locs.organization as organization,
        locs.domain       as domain,
        locs.app           as app,
        locs.cwhen as cwhen,
        locs.message as message,
        --email_app_group_by_impactful.domain as domain,
        --email_app_group_by_impactful.organization as organization,
        --email_app_group_by_impactful.c_analyzed as c_analyzed,
        --email_app_group_by_impactful.c_impactful as c_impactful,
        locs.cnt           AS loc,
        cplxs.cnt          AS cpx,
        smls.cnt           AS sml,
        vuls.cnt           AS vul,
        fbgs.cnt           AS fbg,
        locs_inc.cnt       AS locs_inc,
        cplxs_inc.cnt      AS cplxs_inc,
        smls_inc.cnt       AS smls_inc,
        vuls_inc.cnt       AS vuls_inc,
        fbgs_inc.cnt       AS fbgs_inc,
        locs_dec.cnt       AS locs_dec,
        cplxs_dec.cnt      AS cplxs_dec,
        smls_dec.cnt       AS smls_dec,
        vuls_dec.cnt       AS vuls_dec,
        fbgs_dec.cnt       AS fbgs_dec
"""
myConnection.getQualityCompilationCommitLevel()



"""
    Get Compilation at the organization level and affiliation status
                        organization,
                        'unaffiliated'                     as status,
                        sum(c_analyzed) / sum(c_impactful) as comp_rate,

                        sum(cnt_email)                     as developers,
                        sum(c_analyzed)                    as c_analyzed,
                        sum(c_impactful)                   as c_impactful
"""
myConnection.getAffiliationCompilation()


"""
    Get Quality Metrics at the organization level and affiliation status
    organization,
    'unaffiliated' as status,
    sum(loc)     as locs,
    sum(sml)     as smls,
    sum(cpx)     as cpxs,
    sum(vul)     as vuls,
    sum(fbg)     as fbgs,
     sum(locs_inc)     as locs_inc,
    sum(smls_inc)     as smls_inc,
    sum(cplxs_inc)     as cpxs_inc,
    sum(vuls_inc)     as vuls_inc,
    sum(fbgs_inc)     as fbgs_inc,
    sum(locs_dec)     as locs_dec,
    sum(smls_dec)     as smls_dec,
    sum(cplxs_dec)     as cpxs_dec,
    sum(vuls_dec)     as vuls_dec,
    sum(fbgs_dec)     as fbgs_dec,
    sum(total)   as total,
"""
myConnection.getAffiliationQuality()


"""
    Developer Time Map based on 30 days of activity.
    Developers that have contributed within 30 days of each other
    at the same project and made impactful commits.

    a_email,
    a_app,
    a_cwhen,
    a_csha,
    b_email,
    b_app,
    b_cwhen,
    b_csha
"""
myConnection.getDeveloperTimeMap()
