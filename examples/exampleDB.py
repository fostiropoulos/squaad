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
    Returns:
    email, app, loc, cpx, sml, vul, fbg, locs_inc, cplxs_inc, smls_inc, vuls_inc, fbgs_inc, locs_dec, cplxs_dec, smls_dec, vuls_dec, fbgs_dec, total

"""
myConnection.getQuality()


"""
    Gets the compliation results for a developer at the application level
    Returns:
    email, app, domain, organization, c_impactful, c_analyzed
"""
myConnection.getCompilation()


"""
    Gets the quality metrics AND compilation of a developer at the application level
    Returns:
    email, app, loc, cpx, sml, vul, fbg, locs_inc, cplxs_inc, smls_inc, vuls_ingetQualityCompilationc, fbgs_inc, locs_dec, cplxs_dec, smls_dec, vuls_dec, fbgs_dec, total, impactful, analyzed

"""
myConnection.getQualityCompilation()


"""
    Compilation rates at commit level
    Returns:
    email, organization, domain, app, cwhen, message, loc, cpx, sml, vul, fbg, locs_inc, cplxs_inc, smls_inc, vuls_inc, fbgs_inc, locs_dec, cplxs_dec, smls_dec, vuls_dec, fbgs_dec
"""
myConnection.getQualityCompilationCommitLevel()



"""
    Get Compilation at the organization level and affiliation status
    organization, status, comp_rate, developers, c_analyzed, c_impactful
"""
myConnection.getAffiliationCompilation()


"""
    Get Quality Metrics at the organization level and affiliation status organization
    Returns:
    status, locs, smls, cpxs, vuls, fbgs, locs_inc, smls_inc, cpxs_inc, vuls_inc, fbgs_inc, locs_dec, smls_dec, cpxs_dec, vuls_dec, fbgs_dec, total,
"""
myConnection.getAffiliationQuality()


"""
    Developer Time Map based on 30 days of activity. Developers that have contributed within 30 days of each other at the same project and made impactful commits.
    Returns:
    a_email, a_app, a_cwhen, a_csha, b_email, b_app, b_cwhen, b_csha
"""
myConnection.getDeveloperTimeMap()
